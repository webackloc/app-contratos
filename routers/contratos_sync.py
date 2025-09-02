# =============================================================================
# routers/contratos_sync.py
# Versão: v2.8.0 (2025-09-02)
#
# O QUE MUDA NESTA VERSÃO (em relação à v2.7.0):
# - [CRÍTICO] PROCESSO EM 2 FASES:
#   Fase 1) Atualiza/Preenche 'periodo_contratual' (ou alias) em TODOS os itens.
#           Se algum item continuar sem período (0/None), a rotina PARA e retorna
#           a lista de pendentes para ajuste manual (sem rodar fórmulas).
#   Fase 2) Somente se a Fase 1 não tiver pendências, recalcula os 3 campos:
#           - meses_restantes (aliases suportados)
#           - valor_global_contrato (aliases)
#           - valor_presente_contrato (aliases)
#
# - [MANTIDO] Auto-force por virada de mês, rotas, exportação, diagnósticos,
#   paginação por ID, e compatibilidade com SQLite/Postgres.
#
# Observação: Usamos 'periodo_contratual' como nome "canônico" no item, mas
#             aceitamos aliases tanto no item quanto no cabeçalho.
# =============================================================================
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from fastapi.responses import JSONResponse
from starlette.responses import RedirectResponse, StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, Numeric, String, desc, asc, or_, text
from fastapi.templating import Jinja2Templates
from jinja2 import TemplateNotFound

from database import get_db, SessionLocal
from models import Contrato, ContratoCabecalho
try:
    from models import ContratoLog  # opcional
except Exception:
    ContratoLog = None  # type: ignore

from utils.recalculo_contratos import (
    calc_meses_restantes, calc_valor_global, calc_valor_presente
)
from io import StringIO, BytesIO
from datetime import datetime, date
import json, os, logging, re, time

VERSION = "v2.8.0"
log = logging.getLogger("uvicorn.error")
log.info("[contratos_sync] carregado %s", VERSION)

templates = Jinja2Templates(directory="templates")
router = APIRouter(tags=["Contratos"])

# ---------------- helpers ---------------

_INT_RE = re.compile(r"[-+]?\d+")

def _pick_attr(model, *names):
    for n in names:
        attr = getattr(model, n, None)
        if attr is not None:
            return n, attr
    return None, None

def _first_existing_name(model, names):
    for n in names:
        if hasattr(model, n):
            return n
    return None

def _first_existing_name_instance(obj, names):
    for n in names:
        if hasattr(obj, n):
            return n
    return None

def _ilike_ci(column, term: str | None):
    if not term:
        return True
    term = f"%{term.strip().lower()}%"
    return func.lower(column).like(term)

def _to_float(val):
    try:
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip().replace(" ", "").replace(".", "").replace(",", ".")
        return float(s) if s != "" else None
    except Exception:
        return None

def _to_int(val, default=0):
    try:
        if val is None:
            return default
        if isinstance(val, bool):
            return int(val)
        if isinstance(val, int):
            return val
        if isinstance(val, float):
            return int(round(val))
        s = str(val)
        m = _INT_RE.search(s)
        if not m:
            return default
        return int(m.group(0))
    except Exception:
        return default

def _parse_date_any(d):
    if d is None or d == "":
        return None
    if isinstance(d, (date, datetime)):
        return d
    s = str(d).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _safe_valor_presente(valor_mensal, meses_restantes, indice_anual):
    vm = _to_float(valor_mensal) or 0.0
    mr = int(_to_int(meses_restantes, 0))
    if mr < 0:
        mr = 0
    taxa = _to_float(indice_anual)
    if not taxa or taxa <= 0:
        return round(vm * mr, 2)
    try:
        vp = calc_valor_presente(vm, mr, taxa)
        return round(float(vp or (vm * mr)), 2)
    except Exception:
        return round(vm * mr, 2)

def _is_retornado(item) -> bool:
    if hasattr(item, "status"):
        st = getattr(item, "status", None)
        if isinstance(st, str) and st.strip().upper() == "RETORNADO":
            return True
    if hasattr(item, "data_retorno"):
        dr = getattr(item, "data_retorno", None)
        if dr is not None and str(dr).strip() != "":
            return True
    return False

# --------- Estado do batch (virada de mês) ---------

RUNTIME_DIR = "runtime"
STATE_FILE = os.path.join(RUNTIME_DIR, "contratos_sync_state.json")

def _load_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_state(d: dict):
    try:
        os.makedirs(RUNTIME_DIR, exist_ok=True)
        tmp = dict(d or {})
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(tmp, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _month_key(dt: datetime | None = None) -> str:
    dt = dt or datetime.utcnow()
    return dt.strftime("%Y-%m")

# --------- Fallback de template ---------

def _template_response(context: dict):
    for name in ["contratos_lista.html", "contratos.html", "contratos_list.html"]:
        try:
            templates.env.get_template(name)
            return templates.TemplateResponse(name, context)
        except TemplateNotFound:
            continue
    raise HTTPException(500, detail="Templates não encontrados: contratos_lista.html/contratos.html/contratos_list.html")

# ------------------ LISTAGEM ------------------

@router.get("")
def contratos_view(
    request: Request,
    db: Session = Depends(get_db),
    cliente: str | None = Query(default=None),
    contrato: str | None = Query(default=None),
    ativo: str | None = Query(default=None),
    incluir_retornados: bool = Query(default=False),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=25, ge=5, le=200),
    order_by: str = Query(default="data_envio"),
    order_dir: str = Query(default="desc"),
):
    q_base = db.query(Contrato)
    if cliente and hasattr(Contrato, "nome_cli"):
        q_base = q_base.filter(_ilike_ci(Contrato.nome_cli, cliente))
    if contrato:
        _, col = _pick_attr(Contrato, "contrato_n", "contrato_num", "numero", "numero_contrato")
        if col is None:
            col = cast(getattr(Contrato, "id"), String())
        q_base = q_base.filter(_ilike_ci(col, contrato))
    if ativo and hasattr(Contrato, "ativo"):
        q_base = q_base.filter(_ilike_ci(Contrato.ativo, ativo))

    if not incluir_retornados:
        if hasattr(Contrato, "status"):
            q_base = q_base.filter(or_(Contrato.status.is_(None), func.upper(Contrato.status) != "RETORNADO"))
        if hasattr(Contrato, "data_retorno"):
            q_base = q_base.filter(or_(Contrato.data_retorno.is_(None), cast(Contrato.data_retorno, String) == ""))

    total = q_base.count()

    col = getattr(Contrato, order_by, None) or getattr(Contrato, "data_envio", None) or getattr(Contrato, "id")
    q_rows = q_base.order_by(desc(col) if str(order_dir).lower() == "desc" else asc(col))
    offset = (page - 1) * per_page
    rows = q_rows.offset(offset).limit(per_page).all()

    # KPIs
    mr_colname = _first_existing_name(Contrato, ["meses_restantes","meses_rest","meses_restante","mes_rest"]) or "meses_restantes"
    sq = q_base.with_entities(
        cast(Contrato.valor_mensal, Numeric).label("vm"),
        cast(getattr(Contrato, mr_colname), Numeric).label("mr"),
    ).subquery()
    valor_mensal_sum = float(db.query(func.coalesce(func.sum(sq.c.vm), 0)).scalar() or 0)
    backlog_sum = float(db.query(func.coalesce(func.sum(sq.c.vm * sq.c.mr), 0)).scalar() or 0)

    ctx = {
        "request": request,
        "rows": rows,
        "contratos": rows,
        "itens": rows,
        "total": total,
        "page": page,
        "per_page": per_page,
        "order_by": order_by,
        "order_dir": order_dir,
        "cliente": cliente or "",
        "contrato": contrato or "",
        "ativo": ativo or "",
        "incluir_retornados": incluir_retornados,
        "valor_mensal_sum": valor_mensal_sum,
        "backlog_sum": backlog_sum,
    }
    return _template_response(ctx)

# ------------------ DIAGNÓSTICO ------------------

@router.get("/_diag")
def diagnostico(db: Session = Depends(get_db)):
    mr_name = _first_existing_name(Contrato, ["meses_restantes", "meses_rest", "meses_restante", "mes_rest"]) or None
    vg_name = _first_existing_name(Contrato, ["valor_global_contrato", "valor_global", "valor_global_total", "valor_total"]) or None
    vp_name = _first_existing_name(Contrato, ["valor_presente_contrato", "valor_presente", "valor_presente_total", "valor_presente_backlog", "backlog", "backlog_total"]) or None
    total = db.query(func.count(getattr(Contrato, 'id'))).scalar() if hasattr(Contrato, 'id') else 0
    st = _load_state()
    return JSONResponse({
        "version": VERSION,
        "dest": {"mr": mr_name, "vg": vg_name, "vp": vp_name},
        "total_contratos": int(total or 0),
        "estado": {"last_calc_month": st.get("last_calc_month"), "now_month": _month_key()},
    })

# ------------------ EXPORTAÇÃO ------------------

@router.get("/export")
def exportar_contratos(
    db: Session = Depends(get_db),
    fmt: str = Query("csv", description="csv|xlsx"),
    cliente: str | None = None,
    contrato: str | None = None,
    ativo: str | None = None,
    incluir_retornados: bool = False,
    order_by: str = "data_envio",
    order_dir: str = "desc",
    limit: int = Query(50000, ge=1, le=200000),
):
    q = db.query(Contrato)
    if cliente and hasattr(Contrato, "nome_cli"):
        q = q.filter(_ilike_ci(Contrato.nome_cli, cliente))
    if contrato:
        _, col = _pick_attr(Contrato, "contrato_n", "contrato_num", "numero", "numero_contrato")
        if col is None:
            col = cast(getattr(Contrato, "id"), String())
        q = q.filter(_ilike_ci(col, contrato))
    if ativo and hasattr(Contrato, "ativo"):
        q = q.filter(_ilike_ci(Contrato.ativo, ativo))
    if not incluir_retornados:
        if hasattr(Contrato, "status"):
            q = q.filter(or_(Contrato.status.is_(None), func.upper(Contrato.status) != "RETORNADO"))
        if hasattr(Contrato, "data_retorno"):
            q = q.filter(or_(Contrato.data_retorno.is_(None), cast(Contrato.data_retorno, String) == ""))

    col = getattr(Contrato, order_by, None) or getattr(Contrato, "data_envio", None) or getattr(Contrato, "id")
    q = q.order_by(desc(col) if str(order_dir).lower() == "desc" else asc(col)).limit(limit)

    def _row(c: Contrato):
        contrato_num = getattr(c, "contrato_num", getattr(c, "contrato_n", None))
        data_envio = getattr(c, "data_envio", None)
        vm = float(getattr(c, "valor_mensal", 0) or 0)
        mr_name = _first_existing_name(Contrato, ["meses_restantes","meses_rest","meses_restante","mes_rest"]) or "meses_restantes"
        mr = int(getattr(c, mr_name, 0) or 0)
        return {
            "Ativo": getattr(c, "ativo", None),
            "Cliente": getattr(c, "nome_cli", None),
            "CodCliente": getattr(c, "cod_cli", None),
            "Contrato": contrato_num,
            "DataEnvio": str(data_envio) if data_envio else "",
            "ValorMensal": vm,
            "MesesRestantes": mr,
            "Backlog": round(vm * mr, 2),
        }

    rows = [_row(c) for c in q.all()]
    headers = list(rows[0].keys()) if rows else ["Ativo","Cliente","CodCliente","Contrato","DataEnvio","ValorMensal","MesesRestantes","Backlog"]

    if fmt.lower() == "xlsx":
        try:
            from openpyxl import Workbook
            wb = Workbook(); ws = wb.active; ws.title = "Contratos"
            ws.append(headers)
            for r in rows: ws.append([r.get(h) for h in headers])
            bio = BytesIO(); wb.save(bio); bio.seek(0)
            return StreamingResponse(
                bio,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": 'attachment; filename="contratos.xlsx"'}
            )
        except Exception:
            pass

    sio = StringIO(); sio.write(";".join(headers) + "\n")
    for r in rows:
        vals = [str(r.get(h, "")) for h in headers]
        sio.write(";".join(vals) + "\n")
    sio.seek(0)
    return StreamingResponse(
        sio,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="contratos.csv"'}
    )

# ------------------ Núcleo do batch (duas fases) ------------------

def _run_batch(
    *,
    force: bool,
    dry: bool,
    debug: bool,
    start_id: int,
    max_seconds: int,
    max_batches: int,
    processar_retornados: bool = True,
):
    """
    Agora o processo tem DUAS FASES:
      Fase 1) Preencher periodo_contratual em TODOS os itens (via cabeçalho).
              Se sobrar qualquer item sem período => PARA e informa pendências.
      Fase 2) Se Fase 1 OK, recalcula os 3 campos derivados em TODOS os itens.
    """
    t0 = time.monotonic()

    st_before = _load_state()
    last_calc_month = st_before.get("last_calc_month")
    now_month = _month_key()
    auto_force_by_month = (last_calc_month != now_month)

    effective_force = True if (force or auto_force_by_month or True) else False

    db_read = SessionLocal()
    try:
        # --- Cabeçalho mapeado por número ---
        cab_num_name, cab_num_col = _pick_attr(
            ContratoCabecalho, "contrato_n", "contrato_num", "numero", "numero_contrato"
        )
        if cab_num_col is None:
            raise HTTPException(500, detail="ContratoCabecalho sem coluna de número.")

        prazo_name = _first_existing_name(ContratoCabecalho, ["prazo_contratual", "meses_contrato", "tempo_contrato", "prazo"])
        indice_name = _first_existing_name(ContratoCabecalho, ["indice_reajuste", "indice"])
        codcli_header_exists = hasattr(ContratoCabecalho, "cod_cli")

        header_by_num: dict[str, tuple[int | None, float | None, str | None, int | None]] = {}
        for cab in db_read.query(ContratoCabecalho).all():
            num = getattr(cab, cab_num_name)
            key = str(num).strip() if num is not None else None
            if not key:
                continue
            raw_prazo = getattr(cab, prazo_name, None) if prazo_name else None
            prazo = _to_int(raw_prazo, 0) if raw_prazo is not None else None
            indice = getattr(cab, indice_name, None) if indice_name else None
            cab_cod_cli = getattr(cab, "cod_cli", None) if codcli_header_exists else None
            header_by_num[key] = (prazo, indice, cab_cod_cli, getattr(cab, "id", None))

        # ALIASES
        mr_aliases = ["meses_restantes", "meses_rest", "meses_restante", "mes_rest"]
        vg_aliases = ["valor_global_contrato", "valor_global", "valor_global_total", "valor_total"]
        vp_aliases = ["valor_presente_contrato", "valor_presente", "valor_presente_total", "valor_presente_backlog", "backlog", "backlog_total"]
        periodo_aliases = ["periodo_contratual", "prazo_contratual", "meses_contrato", "tempo_contrato", "prazo", "periodo"]

        mr_name = _first_existing_name(Contrato, mr_aliases)
        vg_name = _first_existing_name(Contrato, vg_aliases)
        vp_name = _first_existing_name(Contrato, vp_aliases)
        periodo_name = _first_existing_name(Contrato, periodo_aliases)

        item_num_name = _first_existing_name(Contrato, ["contrato_n", "contrato_num", "numero", "numero_contrato"])
        item_cab_id_name = _first_existing_name(Contrato, ["cabecalho_id", "contrato_cabecalho_id"])

        batch_size = 500
        # métricas
        atualizados = 0
        inalterados = 0
        pulados_retornado = 0
        copiados_cod_cli = 0
        preencheu_periodo = 0
        skip_campos = 0
        erros = 0
        err_types: dict[str, int] = {}
        err_samples = []
        pendencias_periodo = 0
        pendencias_lista = []  # amostras

        last_id = int(max(0, start_id or 0))
        batches_done = 0
        partial = False
        cursor_stall = False

        def log_error(kind: str, msg: str, item_id: int | None):
            nonlocal erros
            erros += 1
            err_types[kind] = err_types.get(kind, 0) + 1
            if len(err_samples) < 20:
                err_samples.append({"id": item_id, "kind": kind, "msg": msg[:300]})

        def _get_prazo_from_header(db_session, item):
            # 1) via cabecalho_id
            try:
                if item_cab_id_name and hasattr(item, item_cab_id_name):
                    cid = getattr(item, item_cab_id_name, None)
                    if cid:
                        cab = db_session.get(ContratoCabecalho, cid)
                        if cab:
                            raw = getattr(cab, prazo_name, None) if prazo_name else None
                            if raw is not None:
                                return _to_int(raw, 0)
            except Exception:
                pass
            # 2) via número
            try:
                if item_num_name and hasattr(item, item_num_name):
                    numv = getattr(item, item_num_name, None)
                    key = str(numv).strip() if numv not in (None, "") else None
                    if key and key in header_by_num:
                        prazo, _, _, _ = header_by_num[key]
                        if prazo is not None:
                            return _to_int(prazo, 0)
            except Exception:
                pass
            return None

        # ---------------- FASE 1: preencher periodo em TODOS os itens ----------------
        def preencher_periodos(ids: list[int]):
            nonlocal preencheu_periodo, pendencias_periodo, copiados_cod_cli
            if not ids:
                return
            dbw = SessionLocal()
            try:
                try:
                    if 'sqlite' in str(dbw.bind.engine.url):
                        dbw.execute(text("PRAGMA journal_mode=WAL"))
                except Exception:
                    pass
                itens = dbw.query(Contrato).filter(Contrato.id.in_(ids)).all()
                for it in itens:
                    if not processar_retornados and _is_retornado(it):
                        # Mesmo na fase 1, se o usuário optar por não processar retornados, pulamos.
                        continue

                    num_val = getattr(it, item_num_name, None) if item_num_name else None
                    dados = header_by_num.get(str(num_val).strip()) if (num_val not in (None, "") and header_by_num) else None
                    cab_prazo = dados[0] if dados else None
                    cab_cod_cli = dados[2] if dados else None

                    periodo_attr_name = periodo_name or _first_existing_name_instance(it, periodo_aliases)
                    periodo_val_raw = getattr(it, periodo_attr_name, None) if periodo_attr_name else None
                    periodo_item = _to_int(periodo_val_raw, 0) if periodo_attr_name else 0

                    # se vazio/0, tenta puxar do cabeçalho
                    if (periodo_item is None) or (periodo_item == 0):
                        prazo_header = _get_prazo_from_header(dbw, it)
                        if prazo_header is None:
                            prazo_header = cab_prazo
                        if prazo_header is not None and periodo_attr_name:
                            try:
                                setattr(it, periodo_attr_name, int(prazo_header))
                                preencheu_periodo += 1
                                periodo_item = int(prazo_header)
                            except Exception:
                                pass

                    # se ainda ficou inválido => registrar pendência
                    if (periodo_item is None) or (periodo_item == 0):
                        pendencias_periodo += 1
                        if len(pendencias_lista) < 100:
                            pendencias_lista.append({
                                "item_id": getattr(it, "id", None),
                                "contrato_num": str(num_val) if num_val not in (None, "") else None,
                                "motivo": "Periodo não encontrado em cabeçalho (cabecalho_id/numero).",
                            })

                    # manter cod_cli alinhado ao cabeçalho (opcional, ajuda nos relatórios)
                    if hasattr(it, "cod_cli") and cab_cod_cli and getattr(it, "cod_cli", None) != cab_cod_cli:
                        try:
                            setattr(it, "cod_cli", cab_cod_cli)
                            copiados_cod_cli += 1
                        except Exception:
                            pass

                if not dry:
                    dbw.commit()
            except Exception as e:
                log_error(e.__class__.__name__, str(e), None)
                try:
                    dbw.rollback()
                except Exception:
                    pass
            finally:
                dbw.close()

        # ---------------- FASE 2: calcular campos derivados para TODOS ----------------
        def calcular_derivados(ids: list[int]):
            nonlocal atualizados, inalterados, skip_campos
            if not ids:
                return
            dbw = SessionLocal()
            try:
                try:
                    if 'sqlite' in str(dbw.bind.engine.url):
                        dbw.execute(text("PRAGMA journal_mode=WAL"))
                except Exception:
                    pass
                itens = dbw.query(Contrato).filter(Contrato.id.in_(ids)).all()
                for it in itens:
                    if not processar_retornados and _is_retornado(it):
                        continue

                    # período agora DEVE existir (fase 1 garantiu)
                    periodo_attr_name = periodo_name or _first_existing_name_instance(it, periodo_aliases)
                    periodo_val_raw = getattr(it, periodo_attr_name, None) if periodo_attr_name else None
                    periodo_item = _to_int(periodo_val_raw, 0) if periodo_attr_name else 0

                    # metadados do cabeçalho (índice usado no valor-presente)
                    num_val = getattr(it, item_num_name, None) if item_num_name else None
                    dados = header_by_num.get(str(num_val).strip()) if (num_val not in (None, "") and header_by_num) else None
                    indice_anual = dados[1] if dados else None

                    valor_mensal = _to_float(getattr(it, "valor_mensal", 0.0)) or 0.0
                    data_inicio = _parse_date_any(getattr(it, "data_envio", None) or getattr(it, "data_inicio", None) or getattr(it, "data", None))

                    try:
                        mr = calc_meses_restantes(data_inicio, _to_int(periodo_item, 0)) if data_inicio else 0
                    except Exception:
                        mr = 0

                    changed = False
                    if not any([mr_name, vg_name, vp_name]):
                        skip_campos += 1
                    else:
                        if mr_name:
                            try:
                                setattr(it, mr_name, int(mr or 0)); changed = True
                            except Exception: pass
                        if vg_name:
                            try:
                                vg = calc_valor_global(valor_mensal, _to_int(periodo_item, 0))
                                setattr(it, vg_name, vg); changed = True
                            except Exception: pass
                        if vp_name:
                            try:
                                vp = _safe_valor_presente(valor_mensal, mr, indice_anual)
                                setattr(it, vp_name, vp); changed = True
                            except Exception: pass

                    if not dry:
                        dbw.flush()
                        atualizados += 1
                    else:
                        if changed:
                            atualizados += 1
                        else:
                            inalterados += 1

                if not dry:
                    dbw.commit()
            except Exception as e:
                log_error(e.__class__.__name__, str(e), None)
                try:
                    dbw.rollback()
                except Exception:
                    pass
            finally:
                dbw.close()

        # ---------------- LOOP PAGINADO: FASE 1 ----------------
        last_id = int(max(0, start_id or 0))
        while True:
            if max_seconds > 0 and (time.monotonic() - t0) >= max_seconds:
                partial = True; break
            if max_batches > 0 and batches_done >= max_batches:
                partial = True; break

            id_rows = (
                db_read.query(Contrato.id)
                .filter(Contrato.id > last_id)
                .order_by(Contrato.id)
                .limit(batch_size)
                .all()
            )
            ids = [int(r[0] if isinstance(r, (list, tuple)) else getattr(r, "id", r)) for r in id_rows]
            if not ids:
                break
            prev_last_id = last_id
            preencher_periodos(ids)
            last_id = ids[-1]
            batches_done += 1
            if last_id == prev_last_id:
                cursor_stall = True; partial = True; break

        # Se houve pendências na Fase 1, PARA e informa
        if pendencias_periodo > 0:
            elapsed = round(time.monotonic() - t0, 3)
            return {
                "version": VERSION,
                "ok": 0,
                "fase": "periodo",
                "stop_reason": "periodo_invalido",
                "faltando_periodo": pendencias_periodo,
                "faltando_amostras": pendencias_lista,
                "preencheu_periodo": preencheu_periodo,
                "copiados_cod_cli": copiados_cod_cli,
                "errors": erros,
                "err_types": err_types if debug and err_types else None,
                "error_samples": err_samples if debug and err_samples else None,
                "partial": partial,
                "cursor_stall": cursor_stall,
                "batches_done": batches_done,
                "elapsed_s": elapsed,
                "limits": {"max_seconds": max_seconds, "max_batches": max_batches, "batch_size": batch_size},
            }

        # ---------------- LOOP PAGINADO: FASE 2 ----------------
        # reinicia varredura desde o início
        last_id = int(max(0, start_id or 0))
        # zera contadores que são da fase 2
        atualizados = 0
        inalterados = 0
        skip_campos = 0
        batches_done = 0
        partial = False
        cursor_stall = False

        while True:
            if max_seconds > 0 and (time.monotonic() - t0) >= max_seconds:
                partial = True; break
            if max_batches > 0 and batches_done >= max_batches:
                partial = True; break

            id_rows = (
                db_read.query(Contrato.id)
                .filter(Contrato.id > last_id)
                .order_by(Contrato.id)
                .limit(batch_size)
                .all()
            )
            ids = [int(r[0] if isinstance(r, (list, tuple)) else getattr(r, "id", r)) for r in id_rows]
            if not ids:
                break
            prev_last_id = last_id
            calcular_derivados(ids)
            last_id = ids[-1]
            batches_done += 1
            if last_id == prev_last_id:
                cursor_stall = True; partial = True; break

        # fim OK -> atualiza estado de mês
        if not partial:
            _save_state({
                "last_calc_month": now_month,
                "last_run_utc": datetime.utcnow().isoformat(),
                "version": VERSION,
            })

    finally:
        db_read.close()

    elapsed = round(time.monotonic() - t0, 3)
    payload = {
        "version": VERSION,
        "ok": 1,
        "fase": "calculos",
        "updated": atualizados,
        "unchanged": inalterados,
        "skip_campos": skip_campos,
        "copiados_cod_cli": copiados_cod_cli,
        "preencheu_periodo": preencheu_periodo,  # do ciclo completo
        "errors": erros,
        "err_types": err_types if debug and err_types else None,
        "error_samples": err_samples if debug and err_samples else None,
        "partial": partial,
        "cursor_stall": cursor_stall,
        "next_start_id": last_id,
        "batches_done": batches_done,
        "elapsed_s": elapsed,
        "limits": {"max_seconds": max_seconds, "max_batches": max_batches, "batch_size": batch_size},
        "force_requested": bool(force),
        "auto_force_by_month": bool(auto_force_by_month),
        "last_calc_month_before": last_calc_month,
        "now_month": now_month,
    }
    return payload

# ------------------ Endpoints ------------------

@router.post("/sincronizar")
def sincronizar_todos(request: Request):
    debug = request.query_params.get("debug") in {"1", "true", "True"}
    as_json = request.query_params.get("json") in {"1", "true", "True"} or request.query_params.get("format") == "json"
    force = request.query_params.get("force", "true") in {"1","true","True"}
    start_id = int(request.query_params.get("start_id", 0) or 0)
    max_seconds = int(request.query_params.get("max_seconds", 0) or 0)
    max_batches = int(request.query_params.get("max_batches", 0) or 0)
    processar_ret = request.query_params.get("processar_retornados", "true") in {"1","true","True"}

    payload = _run_batch(
        force=force, dry=False, debug=debug,
        start_id=start_id, max_seconds=max_seconds, max_batches=max_batches,
        processar_retornados=processar_ret,
    )
    if as_json:
        return JSONResponse(payload)

    # Query params resumidos para UI
    if payload.get("fase") == "periodo" and payload.get("faltando_periodo", 0) > 0:
        # inclui indicador de pendências
        qp = f"?ok=0&faltando={payload['faltando_periodo']}&preencheu={payload.get('preencheu_periodo',0)}"
    else:
        qp = (
            f"?ok=1&n={payload.get('updated',0)}&inalterados={payload.get('unchanged',0)}"
            f"&preencheu_periodo={payload.get('preencheu_periodo',0)}"
            f"&skip_campos={payload.get('skip_campos',0)}&err={payload.get('errors',0)}"
            f"&partial={'1' if payload.get('partial') else '0'}"
        )

    url = request.headers.get("referer") or "/contratos"
    return RedirectResponse(url + qp, status_code=303)

@router.get("/sincronizar_debug")
def sincronizar_debug(
    force: bool = Query(default=True),
    start_id: int = Query(default=0, ge=0),
    max_seconds: int = Query(default=0, ge=0),
    max_batches: int = Query(default=0, ge=0),
    processar_retornados: bool = Query(default=True),
):
    payload = _run_batch(
        force=bool(force), dry=False, debug=True,
        start_id=int(start_id or 0), max_seconds=int(max_seconds or 0), max_batches=int(max_batches or 0),
        processar_retornados=bool(processar_retornados),
    )
    return JSONResponse(payload)

@router.get("/sincronizar_dry")
def sincronizar_dry(
    force: bool = Query(default=True),
    start_id: int = Query(default=0, ge=0),
    max_seconds: int = Query(default=0, ge=0),
    max_batches: int = Query(default=0, ge=0),
    processar_retornados: bool = Query(default=True),
):
    payload = _run_batch(
        force=bool(force), dry=True, debug=True,
        start_id=int(start_id or 0), max_seconds=int(max_seconds or 0), max_batches=int(max_batches or 0),
        processar_retornados=bool(processar_retornados),
    )
    return JSONResponse(payload)
