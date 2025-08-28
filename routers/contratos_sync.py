# =============================================================================
# routers/contratos_sync.py
# Versão: v2.4.7 (2025-08-28)
#
# Objetivo desta versão
# - Diagnóstico claro: endpoint /contratos/_diag e opção JSON em /sincronizar.
# - Log de versão ao carregar para confirmar que este arquivo está ativo.
# - Aliases de campos ampliados (inclui 'meses_restante').
# - SAVEPOINT por item, paginação por PK, logs de erros em runtime/sync_errors.jsonl.
# - Params úteis: debug=1 (adiciona err_types), json=1 (retorna JSON), force=1 (marca changed=True para todos os itens válidos).
# =============================================================================

from fastapi import APIRouter, Depends, HTTPException, Request, Query
from fastapi.responses import JSONResponse
from starlette.responses import RedirectResponse, StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, Numeric, String, desc, asc, and_, or_, text
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
import json, os, logging

# --------- versão/log ---------
VERSION = "v2.4.7"
log = logging.getLogger("uvicorn.error")
log.info("[contratos_sync] carregado %s", VERSION)


templates = Jinja2Templates(directory="templates")
router = APIRouter(tags=["Contratos"])

# --------------------- helpers ---------------------

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
        s = str(val).strip()
        if isinstance(val, str):
            s = s.replace(" ", "").replace(".", "").replace(",", ".")
        return float(s) if s != "" else None
    except Exception:
        return None

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
    mr = int(meses_restantes or 0)
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


def _apply_filtros(q, cliente: str | None, contrato: str | None, ativo: str | None):
    if cliente and hasattr(Contrato, "nome_cli"):
        q = q.filter(_ilike_ci(Contrato.nome_cli, cliente))
    if contrato:
        _, col = _pick_attr(Contrato, "contrato_n", "contrato_num", "numero", "numero_contrato")
        if col is None:
            fallback = getattr(Contrato, "cabecalho_id", getattr(Contrato, "id"))
            col = cast(fallback, String())
        q = q.filter(_ilike_ci(col, contrato))
    if ativo and hasattr(Contrato, "ativo"):
        q = q.filter(_ilike_ci(Contrato.ativo, ativo))
    return q


def _tune_sqlite(db: Session):
    try:
        db.execute(text("PRAGMA journal_mode=WAL"))
    except Exception:
        pass
    try:
        db.execute(text("PRAGMA busy_timeout=60000"))
    except Exception:
        pass

# ------------------ LISTAGEM HTML ------------------

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
    q_base = _apply_filtros(q_base, cliente, contrato, ativo)
    if not incluir_retornados:
        # Usando somente itens em carteira quando possível
        if hasattr(Contrato, "status"):
            q_base = q_base.filter(or_(Contrato.status.is_(None), func.upper(Contrato.status) != "RETORNADO"))
        if hasattr(Contrato, "data_retorno"):
            q_base = q_base.filter(or_(Contrato.data_retorno.is_(None), cast(Contrato.data_retorno, String) == ""))

    total = q_base.count()

    col = getattr(Contrato, order_by, None)
    if col is None:
        for name in ("data_envio", "id", "nome_cli", "ativo"):
            col = getattr(Contrato, name, None)
            if col is not None:
                break
    q_rows = q_base.order_by(desc(col) if str(order_dir).lower() == "desc" else asc(col))
    offset = (page - 1) * per_page
    rows = q_rows.offset(offset).limit(per_page).all()

    # KPIs da lista
    sq = q_base.with_entities(
        cast(Contrato.valor_mensal, Numeric).label("vm"),
        cast(getattr(Contrato, _first_existing_name(Contrato, ["meses_restantes","meses_rest"]) or "meses_restantes"), Numeric).label("mr"),
    ).subquery()

    valor_mensal_sum = db.query(func.coalesce(func.sum(sq.c.vm), 0)).scalar() or 0
    backlog_sum = db.query(func.coalesce(func.sum(sq.c.vm * sq.c.mr), 0)).scalar() or 0

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
        "valor_mensal_sum": float(valor_mensal_sum),
        "backlog_sum": float(backlog_sum),
        "valor_mensal_total": float(valor_mensal_sum),
        "backlog_total": float(backlog_sum),
    }
    return templates.TemplateResponse("contratos_lista.html", ctx)

# ------------------ DIAGNÓSTICO ------------------

@router.get("/_diag")
def diagnostico(db: Session = Depends(get_db)):
    mr_name = _first_existing_name(Contrato, ["meses_restantes", "meses_rest", "meses_restante"]) or None
    vg_name = _first_existing_name(Contrato, ["valor_global_contrato", "valor_global", "valor_global_total", "valor_total"]) or None
    vp_name = _first_existing_name(Contrato, ["valor_presente_contrato", "valor_presente", "valor_presente_total", "valor_presente_backlog", "backlog", "backlog_total"]) or None
    total = db.query(func.count(getattr(Contrato, 'id'))).scalar() if hasattr(Contrato, 'id') else None
    return JSONResponse({
        "version": VERSION,
        "dest": {"mr": mr_name, "vg": vg_name, "vp": vp_name},
        "total_contratos": int(total or 0),
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
    q = _apply_filtros(q, cliente, contrato, ativo)
    if not incluir_retornados:
        if hasattr(Contrato, "status"):
            q = q.filter(or_(Contrato.status.is_(None), func.upper(Contrato.status) != "RETORNADO"))
        if hasattr(Contrato, "data_retorno"):
            q = q.filter(or_(Contrato.data_retorno.is_(None), cast(Contrato.data_retorno, String) == ""))

    col = getattr(Contrato, order_by, None)
    if col is None:
        col = getattr(Contrato, "data_envio", None) or getattr(Contrato, "id")
    q = q.order_by(desc(col) if str(order_dir).lower() == "desc" else asc(col)).limit(limit)

    def _row(c: Contrato):
        contrato_num = getattr(c, "contrato_num", getattr(c, "contrato_n", None))
        data_envio = getattr(c, "data_envio", None)
        vm = float(getattr(c, "valor_mensal", 0) or 0)
        mr_name = _first_existing_name(Contrato, ["meses_restantes", "meses_rest", "meses_restante"]) or "meses_restantes"
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
            wb = Workbook()
            ws = wb.active
            ws.title = "Contratos"
            ws.append(headers)
            for r in rows:
                ws.append([r.get(h) for h in headers])
            bio = BytesIO()
            wb.save(bio)
            bio.seek(0)
            return StreamingResponse(
                bio,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": 'attachment; filename="contratos.xlsx"'}
            )
        except Exception:
            pass

    sio = StringIO()
    sio.write(";".join(headers) + "\n")
    for r in rows:
        vals = [str(r.get(h, "")) for h in headers]
        sio.write(";".join(vals) + "\n")
    sio.seek(0)
    return StreamingResponse(
        sio,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="contratos.csv"'}
    )

# ------------------ SINCRONIZAÇÕES ------------------

@router.post("/sincronizar/{contrato_num}")
def sincronizar_contrato(contrato_num: str, request: Request, db: Session = Depends(get_db)):
    contrato_num = (contrato_num or "").strip()

    cab_num_name, cab_num_col = _pick_attr(
        ContratoCabecalho, "contrato_n", "contrato_num", "numero", "numero_contrato"
    )
    if cab_num_col is None:
        raise HTTPException(500, detail="ContratoCabecalho sem coluna de número.")

    cab = db.query(ContratoCabecalho).filter(cab_num_col == contrato_num).first()
    if not cab:
        raise HTTPException(404, detail=f"Cabeçalho do contrato {contrato_num} não encontrado.")

    prazo = getattr(cab, "prazo_contratual", None) or getattr(cab, "periodo_contratual", None)
    indice_anual = getattr(cab, "indice_reajuste", None)
    cab_cod_cli = getattr(cab, "cod_cli", None)

    item_num_name, item_num_col = _pick_attr(
        Contrato, "contrato_n", "contrato_num", "numero", "numero_contrato"
    )
    if item_num_col is None:
        raise HTTPException(500, detail="Contrato (itens) sem coluna de número.")

    mr_aliases = ["meses_restantes", "meses_rest", "meses_restante"]
    vg_aliases = ["valor_global_contrato", "valor_global", "valor_global_total", "valor_total"]
    vp_aliases = ["valor_presente_contrato", "valor_presente", "valor_presente_total", "valor_presente_backlog", "backlog", "backlog_total"]

    itens = db.query(Contrato).filter(item_num_col == contrato_num).all()
    itens = [it for it in itens if not _is_retornado(it)]

    if not itens:
        url = request.headers.get("referer") or "/contratos"
        return RedirectResponse(url=f"{url}?ok=0&msg=sem_itens", status_code=303)

    atualizados = 0
    inalterados = 0

    force = request.query_params.get("force") in {"1","true","True"}

    for it in itens:
        if hasattr(it, "cod_cli") and cab_cod_cli and getattr(it, "cod_cli", None) != cab_cod_cli:
            it.cod_cli = cab_cod_cli

        if hasattr(it, "periodo_contratual") and prazo is not None:
            it.periodo_contratual = prazo

        periodo = getattr(it, "periodo_contratual", None) or prazo or 0
        data_inicio = _parse_date_any(
            getattr(it, "data_envio", None) or getattr(it, "data_inicio", None) or getattr(it, "data", None)
        )
        valor_mensal = _to_float(getattr(it, "valor_mensal", 0.0)) or 0.0

        try:
            mr = calc_meses_restantes(data_inicio, int(periodo or 0)) if data_inicio else 0
        except Exception:
            mr = 0

        # definir nos primeiros nomes existentes
        mr_name = _first_existing_name(Contrato, mr_aliases)
        vg_name = _first_existing_name(Contrato, vg_aliases)
        vp_name = _first_existing_name(Contrato, vp_aliases)

        changed = False
        if mr_name:
            prev = getattr(it, mr_name, None)
            newv = int(mr or 0)
            if force or prev != newv:
                setattr(it, mr_name, newv)
                changed = True
        if vg_name:
            prev = getattr(it, vg_name, None)
            newv = calc_valor_global(valor_mensal, int(periodo or 0))
            if force or prev != newv:
                setattr(it, vg_name, newv)
                changed = True
        if vp_name:
            prev = getattr(it, vp_name, None)
            newv = _safe_valor_presente(valor_mensal, int(mr or 0), indice_anual)
            if force or prev != newv:
                setattr(it, vp_name, newv)
                changed = True

        if changed:
            atualizados += 1
        else:
            inalterados += 1

    db.commit()

    # resposta
    if request.query_params.get("json") in {"1","true","True"} or request.query_params.get("format") == "json":
        return JSONResponse({
            "version": VERSION,
            "dest": {"mr": mr_name, "vg": vg_name, "vp": vp_name},
            "ok": 1,
            "updated": atualizados,
            "unchanged": inalterados,
            "scope": "single",
        })

    url = request.headers.get("referer") or f"/contratos/{contrato_num}"
    return RedirectResponse(url=f"{url}?ok=1&n={atualizados}&inalterados={inalterados}", status_code=303)


@router.post("/sincronizar")
def sincronizar_todos(request: Request, db: Session = Depends(get_db)):
    """Batch robusto compatível com variações de esquema.
    Params: debug=1 | json=1 | force=1
    """
    debug = request.query_params.get("debug") in {"1", "true", "True"}
    as_json = request.query_params.get("json") in {"1", "true", "True"} or request.query_params.get("format") == "json"
    force = request.query_params.get("force") in {"1","true","True"}

    # --------- Sessão de LEITURA ---------
    db_read = SessionLocal()
    try:
        cab_num_name, cab_num_col = _pick_attr(
            ContratoCabecalho, "contrato_n", "contrato_num", "numero", "numero_contrato"
        )
        if cab_num_col is None:
            raise HTTPException(500, detail="ContratoCabecalho sem coluna de número.")

        prazo_name = "prazo_contratual" if hasattr(ContratoCabecalho, "prazo_contratual") else (
            "periodo_contratual" if hasattr(ContratoCabecalho, "periodo_contratual") else None
        )
        indice_name = "indice_reajuste" if hasattr(ContratoCabecalho, "indice_reajuste") else None
        codcli_header_exists = hasattr(ContratoCabecalho, "cod_cli")
        codcli_item_exists = hasattr(Contrato, "cod_cli")

        header_map: dict[str, tuple[int | None, float | None, str | None]] = {}
        for cab in db_read.query(ContratoCabecalho).all():
            num = getattr(cab, cab_num_name)
            key = str(num).strip() if num is not None else None
            if not key:
                continue
            prazo = getattr(cab, prazo_name, None) if prazo_name else None
            indice = getattr(cab, indice_name, None) if indice_name else None
            codcli = getattr(cab, "cod_cli", None) if codcli_header_exists else None
            header_map[key] = (prazo, indice, codcli)

        mr_aliases = ["meses_restantes", "meses_rest", "meses_restante"]
        vg_aliases = ["valor_global_contrato", "valor_global", "valor_global_total", "valor_total"]
        vp_aliases = ["valor_presente_contrato", "valor_presente", "valor_presente_total", "valor_presente_backlog", "backlog", "backlog_total"]
        mr_name = _first_existing_name(Contrato, mr_aliases)
        vg_name = _first_existing_name(Contrato, vg_aliases)
        vp_name = _first_existing_name(Contrato, vp_aliases)

        batch_size = 500
        atualizados = 0
        inalterados = 0
        pulados_sem_cab = 0
        pulados_retornado = 0
        copiados_cod_cli = 0
        skip_campos = 0
        erros = 0
        err_types: dict[str, int] = {}
        err_samples = []

        last_id = 0

        def log_error(kind: str, msg: str, item_id: int | None):
            nonlocal erros
            erros += 1
            err_types[kind] = err_types.get(kind, 0) + 1
            if len(err_samples) < 50:
                err_samples.append({"id": item_id, "kind": kind, "msg": msg[:300]})

        def processar_lote(ids: list[int]):
            nonlocal atualizados, inalterados, pulados_sem_cab, pulados_retornado, copiados_cod_cli, skip_campos
            if not ids:
                return
            db_write = SessionLocal()
            try:
                _tune_sqlite(db_write)
                itens = db_write.query(Contrato).filter(Contrato.id.in_(ids)).all()
                for it in itens:
                    if _is_retornado(it):
                        pulados_retornado += 1
                        continue

                    item_num_name, _ = _pick_attr(Contrato, "contrato_n", "contrato_num", "numero", "numero_contrato")
                    if not item_num_name:
                        pulados_sem_cab += 1
                        continue

                    num_val = getattr(it, item_num_name)
                    dados = header_map.get(str(num_val).strip() if num_val is not None else "")
                    if not dados:
                        pulados_sem_cab += 1
                        continue

                    prazo, indice_anual, cab_cod_cli = dados

                    try:
                        with db_write.begin_nested():
                            if hasattr(it, "periodo_contratual") and prazo is not None:
                                it.periodo_contratual = prazo

                            if codcli_item_exists and cab_cod_cli and getattr(it, "cod_cli", None) != cab_cod_cli:
                                it.cod_cli = cab_cod_cli
                                copiados_cod_cli += 1

                            periodo = getattr(it, "periodo_contratual", None) or prazo or 0
                            data_inicio = _parse_date_any(
                                getattr(it, "data_envio", None) or getattr(it, "data_inicio", None) or getattr(it, "data", None)
                            )
                            valor_mensal = _to_float(getattr(it, "valor_mensal", 0.0)) or 0.0

                            try:
                                mr = calc_meses_restantes(data_inicio, int(periodo or 0)) if data_inicio else 0
                            except Exception:
                                mr = 0

                            if not any([mr_name, vg_name, vp_name]):
                                skip_campos += 1
                            else:
                                changed = False
                                if mr_name:
                                    prev = getattr(it, mr_name, None)
                                    newv = int(mr or 0)
                                    if force or prev != newv:
                                        setattr(it, mr_name, newv)
                                        changed = True
                                if vg_name:
                                    prev = getattr(it, vg_name, None)
                                    newv = calc_valor_global(valor_mensal, int(periodo or 0))
                                    if force or prev != newv:
                                        setattr(it, vg_name, newv)
                                        changed = True
                                if vp_name:
                                    prev = getattr(it, vp_name, None)
                                    newv = _safe_valor_presente(valor_mensal, int(mr or 0), indice_anual)
                                    if force or prev != newv:
                                        setattr(it, vp_name, newv)
                                        changed = True

                                if changed:
                                    db_write.flush()
                                    atualizados += 1
                                else:
                                    inalterados += 1
                    except Exception as e:
                        kind = e.__class__.__name__
                        log_error(kind, str(e), getattr(it, "id", None))
                db_write.commit()
            except Exception as e:
                log_error(e.__class__.__name__, str(e), None)
                try:
                    db_write.rollback()
                except Exception:
                    pass
            finally:
                db_write.close()

        while True:
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
            processar_lote(ids)
            last_id = ids[-1]

        if err_samples:
            os.makedirs("runtime", exist_ok=True)
            with open("runtime/sync_errors.jsonl", "a", encoding="utf-8") as f:
                for s in err_samples:
                    s["ts"] = datetime.utcnow().isoformat()
                    f.write(json.dumps(s, ensure_ascii=False) + "\n")

    finally:
        db_read.close()

    # Saída
    payload = {
        "version": VERSION,
        "dest": {"mr": mr_name, "vg": vg_name, "vp": vp_name},
        "ok": 1,
        "updated": atualizados,
        "unchanged": inalterados,
        "skip_sem_cab": pulados_sem_cab,
        "skip_ret": pulados_retornado,
        "skip_campos": skip_campos,
        "copiados_cod_cli": copiados_cod_cli,
        "errors": erros,
    }
    if debug and err_types:
        payload["err_types"] = err_types
    if as_json:
        return JSONResponse(payload)

    url = request.headers.get("referer") or "/contratos"
    qp = (
        f"?ok=1&n={atualizados}&inalterados={inalterados}&skip_sem_cab={pulados_sem_cab}"
        f"&skip_ret={pulados_retornado}&codcli={copiados_cod_cli}&skip_campos={skip_campos}&err={erros}"
    )
    return RedirectResponse(url + qp, status_code=303)
