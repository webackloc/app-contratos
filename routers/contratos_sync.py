# =============================================================================
# routers/contratos_sync.py
# Versão: v2.4.1 (2025-08-28)
#
# Mudanças v2.4.1:
#   • Corrige caso "atualizados=0" quando existem erros no lote: usa SAVEPOINT
#     (begin_nested) por item, evitando que um erro cause rollback do lote todo.
#   • Coleta amostras de erros em runtime/sync_errors.jsonl (até 50 por execução).
#   • Cálculos mais tolerantes: datas e valores com fallback seguro.
#   • Parâmetro opcional debug=1 para intensificar logs e retornar err_types.
#
# Mudanças v2.4.0 (base):
#   • Removido streaming server-side; paginação por PK (id > last_id LIMIT N).
#   • Sessões separadas: leitura e escrita por lote.
# =============================================================================

from fastapi import APIRouter, Depends, HTTPException, Request, Query
from starlette.responses import RedirectResponse, StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, Numeric, String, desc, asc, and_, or_, text, select
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
import json, os


templates = Jinja2Templates(directory="templates")
router = APIRouter(tags=["Contratos"])

# --------------------- helpers ---------------------

def _pick_attr(model, *names):
    for n in names:
        attr = getattr(model, n, None)
        if attr is not None:
            return n, attr
    return None, None

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
        s = str(val).strip().replace(" ", "").replace(".", "").replace(",", ".") if isinstance(val, str) else str(val)
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
            col = cast(getattr(Contrato, "cabecalho_id"), String())
        q = q.filter(_ilike_ci(col, contrato))
    if ativo and hasattr(Contrato, "ativo"):
        q = q.filter(_ilike_ci(Contrato.ativo, ativo))
    return q


def _filtrar_apenas_em_carteira(q, db: Session):
    if hasattr(Contrato, "status") or hasattr(Contrato, "data_retorno"):
        conds = []
        if hasattr(Contrato, "status"):
            conds.append(or_(Contrato.status.is_(None), func.upper(Contrato.status) != "RETORNADO"))
        if hasattr(Contrato, "data_retorno"):
            conds.append(or_(Contrato.data_retorno.is_(None), cast(Contrato.data_retorno, String) == ""))
        if conds:
            from sqlalchemy import and_ as _and
            return q.filter(_and(*conds))

    if not ContratoLog:
        return q

    date_col = None
    for c in ("data_mov", "data", "created_at", "timestamp"):
        if hasattr(ContratoLog, c):
            date_col = getattr(ContratoLog, c)
            break
    if date_col is None:
        return q

    keys = []
    if hasattr(ContratoLog, "contrato_num"): keys.append(ContratoLog.contrato_num)
    if hasattr(ContratoLog, "ativo"):        keys.append(ContratoLog.ativo)
    if hasattr(ContratoLog, "cod_cli"):      keys.append(ContratoLog.cod_cli)

    sub = (
        db.query(*(k.label(f"k{i}") for i, k in enumerate(keys)), func.max(date_col).label("last_dt"))
        .group_by(*keys)
    ).subquery("lasts")

    from sqlalchemy.orm import aliased
    L = aliased(ContratoLog)
    join_conds = []
    if hasattr(ContratoLog, "contrato_num"):
        join_conds.append(L.contrato_num == (sub.c.k0 if "k0" in sub.c else L.contrato_num))
    if hasattr(ContratoLog, "ativo"):
        if "k1" in sub.c: join_conds.append(L.ativo == sub.c.k1)
        elif "k0" in sub.c: join_conds.append(L.ativo == sub.c.k0)
    if hasattr(ContratoLog, "cod_cli"):
        for k in ("k2", "k1", "k0"):
            if k in sub.c:
                join_conds.append(L.cod_cli == sub.c[k])
                break
    join_conds.append(L.__table__.c[date_col.key] == sub.c.last_dt)

    q = (
        q.join(
            sub,
            and_(
                (Contrato.contrato_num == sub.c.k0) if "k0" in sub.c and hasattr(Contrato, "contrato_num") else True,
                (Contrato.ativo == sub.c.k1) if "k1" in sub.c and hasattr(Contrato, "ativo") else True,
                (Contrato.cod_cli == sub.c.k2) if "k2" in sub.c and hasattr(Contrato, "cod_cli") else True,
            )
        )
        .join(L, and_(*join_conds))
        .filter(L.tp_transacao != "RETORNO")
    )
    return q


def _template_response(context: dict):
    for name in ["contratos_lista.html", "contratos.html", "contratos_list.html"]:
        try:
            templates.env.get_template(name)
            return templates.TemplateResponse(name, context)
        except TemplateNotFound:
            continue
    raise HTTPException(500, detail="Templates não encontrados: contratos_lista.html, contratos.html ou contratos_list.html")


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
    usar_carteira = not incluir_retornados
    if usar_carteira:
        q_base = _filtrar_apenas_em_carteira(q_base, db)

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

    usando_retornados = False
    if total == 0 and usar_carteira:
        q_base = db.query(Contrato)
        q_base = _apply_filtros(q_base, cliente, contrato, ativo)
        total = q_base.count()
        q_rows = q_base.order_by(desc(col) if str(order_dir).lower() == "desc" else asc(col))
        rows = q_rows.offset(offset).limit(per_page).all()
        usando_retornados = True
        incluir_retornados = True

    sq = q_base.with_entities(
        cast(Contrato.valor_mensal, Numeric).label("vm"),
        cast(Contrato.meses_restantes, Numeric).label("mr"),
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
        "incluirRetornados": incluir_retornados,
        "usando_retornados": usando_retornados,
        "valor_mensal_sum": float(valor_mensal_sum),
        "backlog_sum": float(backlog_sum),
        "valor_mensal_total": float(valor_mensal_sum),
        "backlog_total": float(backlog_sum),
    }
    return _template_response(ctx)

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
        q = _filtrar_apenas_em_carteira(q, db)

    col = getattr(Contrato, order_by, None)
    if col is None:
        col = getattr(Contrato, "data_envio", None) or getattr(Contrato, "id")
    q = q.order_by(desc(col) if str(order_dir).lower() == "desc" else asc(col)).limit(limit)

    def _row(c: Contrato):
        contrato_num = getattr(c, "contrato_num", getattr(c, "contrato_n", None))
        data_envio = getattr(c, "data_envio", None)
        vm = float(getattr(c, "valor_mensal", 0) or 0)
        mr = int(getattr(c, "meses_restantes", 0) or 0)
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
    sio.write(";".join(headers) + "
")
    for r in rows:
        vals = [str(r.get(h, "")) for h in headers]
        sio.write(";".join(vals) + "
")
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

    itens = db.query(Contrato).filter(item_num_col == contrato_num).all()
    itens = [it for it in itens if not _is_retornado(it)]

    if not itens:
        url = request.headers.get("referer") or "/contratos"
        return RedirectResponse(url=f"{url}?ok=0&msg=sem_itens", status_code=303)

    atualizados = 0
    copiados_cod_cli = 0

    for it in itens:
        if hasattr(it, "cod_cli") and cab_cod_cli and getattr(it, "cod_cli", None) != cab_cod_cli:
            it.cod_cli = cab_cod_cli
            copiados_cod_cli += 1

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
        it.meses_restantes = int(mr or 0)
        it.valor_global_contrato = calc_valor_global(valor_mensal, int(periodo or 0))
        it.valor_presente_contrato = _safe_valor_presente(valor_mensal, it.meses_restantes, indice_anual)

        atualizados += 1

    db.commit()
    url = request.headers.get("referer") or f"/contratos/{contrato_num}"
    return RedirectResponse(url=f"{url}?ok=1&n={atualizados}&codcli={copiados_cod_cli}", status_code=303)


@router.post("/sincronizar")
def sincronizar_todos(request: Request, db: Session = Depends(get_db)):
    """Batch robusto sem server-side cursor.
    - Paginação por PK (id > last_id LIMIT N)
    - Transação por item via SAVEPOINT (begin_nested), para que erros não derrubem o lote.
    - Loga amostras de erros em runtime/sync_errors.jsonl
    """
    debug = request.query_params.get("debug") in {"1", "true", "True"}

    # --------- Sessão de LEITURA ---------
    db_read = SessionLocal()
    try:
        # 1) Mapear cabeçalhos
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

        # 2) Paginação
        batch_size = 500
        atualizados = 0
        pulados_sem_cab = 0
        pulados_retornado = 0
        copiados_cod_cli = 0
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
            nonlocal atualizados, pulados_sem_cab, pulados_retornado, copiados_cod_cli
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
                        with db_write.begin_nested():  # SAVEPOINT por item
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

                            it.meses_restantes = int(mr or 0)
                            it.valor_global_contrato = calc_valor_global(valor_mensal, int(periodo or 0))
                            it.valor_presente_contrato = _safe_valor_presente(valor_mensal, it.meses_restantes, indice_anual)

                            # flush dentro do savepoint (se quebrar, só esse item volta)
                            db_write.flush()
                            atualizados += 1
                    except Exception as e:
                        kind = e.__class__.__name__
                        log_error(kind, str(e), getattr(it, "id", None))
                        # após rollback do savepoint, seguimos p/ próximo item
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

        # Grava amostras de erros
        if err_samples:
            os.makedirs("runtime", exist_ok=True)
            with open("runtime/sync_errors.jsonl", "a", encoding="utf-8") as f:
                for s in err_samples:
                    s["ts"] = datetime.utcnow().isoformat()
                    f.write(json.dumps(s, ensure_ascii=False) + "
")

    finally:
        db_read.close()

    url = request.headers.get("referer") or "/contratos"
    qp = (
        f"?ok=1&n={atualizados}&skip_sem_cab={pulados_sem_cab}&skip_ret={pulados_retornado}"
        f"&codcli={copiados_cod_cli}&err={erros}"
    )
    if debug and erros:
        # inclui classes de erro para inspeção rápida
        err_types_str = ",".join(f"{k}:{v}" for k,v in sorted(err_types.items()))
        qp += f"&err_types={err_types_str}"
    return RedirectResponse(url + qp, status_code=303)
