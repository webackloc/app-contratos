# =============================================================================
# routers/contratos_sync.py
# Versão: v2.4.11 (2025-08-28)
#
# Objetivo: facilitar o DEBUG no ambiente produtivo sem DevTools/Console.
# - Mantém tudo da v2.4.10 (_to_int seguro, fallback de template, /_diag, json=1, force=1, etc.)
# - NOVO: GET /contratos/sincronizar_debug  → executa o batch e retorna JSON (sem precisar usar POST)
# - NOVO: GET /contratos/sincronizar_dry    → prévia (não escreve no banco), mostra o que SERIA atualizado
# - Loga no servidor um resumo dos erros quando errors>0
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
import json, os, logging, re

VERSION = "v2.4.11"
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
    mr_colname = _first_existing_name(Contrato, ["meses_restantes","meses_rest","meses_restante"]) or "meses_restantes"
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
        "valor_mensal_total": valor_mensal_sum,
        "backlog_total": backlog_sum,
    }
    return _template_response(ctx)

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
    q = q.filter(True)  # placeholder
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

# ------------------ Núcleo comum do batch ------------------

def _run_batch(force: bool, dry: bool, debug: bool):
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
            prazo_raw = getattr(cab, prazo_name, None) if prazo_name else None
            prazo = _to_int(prazo_raw, 0) if prazo_raw is not None else None
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
            if len(err_samples) < 20:
                err_samples.append({"id": item_id, "kind": kind, "msg": msg[:300]})

        def processar_lote(ids: list[int]):
            nonlocal atualizados, inalterados, pulados_sem_cab, pulados_retornado, copiados_cod_cli, skip_campos
            if not ids:
                return
            db_write = SessionLocal()
            try:
                db_write.execute(text("PRAGMA journal_mode=WAL")) if 'sqlite' in str(db_write.bind.engine.url) else None
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
                        if not dry:
                            ctx_mgr = db_write.begin_nested()
                        else:
                            class _Dummy:
                                def __enter__(self,*a,**k): return self
                                def __exit__(self,*a,**k): return False
                            ctx_mgr = _Dummy()

                        with ctx_mgr:
                            if hasattr(it, "periodo_contratual") and prazo is not None and not dry:
                                it.periodo_contratual = prazo

                            if codcli_item_exists and cab_cod_cli and getattr(it, "cod_cli", None) != cab_cod_cli and not dry:
                                it.cod_cli = cab_cod_cli
                                copiados_cod_cli += 1

                            periodo_raw = getattr(it, "periodo_contratual", None)
                            periodo = _to_int(periodo_raw if periodo_raw is not None else prazo, 0)
                            if periodo < 0:
                                periodo = 0

                            data_inicio = _parse_date_any(
                                getattr(it, "data_envio", None) or getattr(it, "data_inicio", None) or getattr(it, "data", None)
                            )
                            valor_mensal = _to_float(getattr(it, "valor_mensal", 0.0)) or 0.0

                            try:
                                mr = calc_meses_restantes(data_inicio, periodo) if data_inicio else 0
                            except Exception:
                                mr = 0

                            if not any([mr_name, vg_name, vp_name]):
                                skip_campos += 1
                            else:
                                changed = False
                                if mr_name:
                                    prev = getattr(it, mr_name, None)
                                    newv = int(mr or 0)
                                    if dry:
                                        changed = changed or (prev != newv)
                                    else:
                                        if force or prev != newv:
                                            setattr(it, mr_name, newv)
                                            changed = True
                                if vg_name:
                                    prev = getattr(it, vg_name, None)
                                    newv = calc_valor_global(valor_mensal, periodo)
                                    if dry:
                                        changed = changed or (prev != newv)
                                    else:
                                        if force or prev != newv:
                                            setattr(it, vg_name, newv)
                                            changed = True
                                if vp_name:
                                    prev = getattr(it, vp_name, None)
                                    newv = _safe_valor_presente(valor_mensal, mr, indice_anual)
                                    if dry:
                                        changed = changed or (prev != newv)
                                    else:
                                        if force or prev != newv:
                                            setattr(it, vp_name, newv)
                                            changed = True

                                if changed:
                                    if not dry:
                                        db_write.flush()
                                    atualizados += 1
                                else:
                                    inalterados += 1
                    except Exception as e:
                        kind = e.__class__.__name__
                        log_error(kind, str(e), getattr(it, "id", None))
                if not dry:
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
    if debug:
        if err_types:
            payload["err_types"] = err_types
        if err_samples:
            payload["error_samples"] = err_samples
    
    if erros:
        log.warning("[contratos_sync][debug] errors=%s types=%s", erros, err_types)
        if err_samples:
            log.warning("[contratos_sync][debug] samples=%s", err_samples[:3])

    return payload

# ------------------ SINCRONIZAÇÕES ------------------

@router.post("/sincronizar")
def sincronizar_todos(request: Request):
    debug = request.query_params.get("debug") in {"1", "true", "True"}
    as_json = request.query_params.get("json") in {"1", "true", "True"} or request.query_params.get("format") == "json"
    force = request.query_params.get("force") in {"1","true","True"}

    payload = _run_batch(force=force, dry=False, debug=debug)
    if as_json:
        return JSONResponse(payload)

    url = request.headers.get("referer") or "/contratos"
    qp = (
        f"?ok=1&n={payload['updated']}&inalterados={payload['unchanged']}&skip_sem_cab={payload['skip_sem_cab']}"
        f"&skip_ret={payload['skip_ret']}&codcli={payload['copiados_cod_cli']}&skip_campos={payload['skip_campos']}&err={payload['errors']}"
    )
    return RedirectResponse(url + qp, status_code=303)

# NOVO: GET que executa o batch e retorna JSON (sem POST / sem DevTools)
@router.get("/sincronizar_debug")
def sincronizar_debug(force: bool = Query(default=False)):
    payload = _run_batch(force=bool(force), dry=False, debug=True)
    return JSONResponse(payload)

# NOVO: GET de DRY-RUN (não escreve nada) — mostra o que seria alterado
@router.get("/sincronizar_dry")
def sincronizar_dry(force: bool = Query(default=False)):
    payload = _run_batch(force=bool(force), dry=True, debug=True)
    return JSONResponse(payload)
