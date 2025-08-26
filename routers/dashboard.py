# Módulo: Dashboard
# Versão: 1.10.0
# Data: 2025-08-25
# Autor: Leonardo Muller
#
# Novidades (1.10.0):
#   • "valor_presente" foi substituído por **backlog** = valor_mensal × meses_restantes
#     (somando apenas itens com meses_restantes > 0).
#   • Totais do mês atual (baseados em ContratoLog):
#       - devolvidos_mes_atual: {quantidade, valor_mensal}
#       - entregues_mes_atual: {quantidade, valor_mensal}
#   • Totais de contratos vencidos:
#       - contratos_vencidos: {quantidade, valor_mensal}
#   • Compatibilidade: mantém chaves antigas valor_presente_total e valor_presente_por_cliente,
#     espelhando o backlog.
#
# Histórico recente:
#   • 1.9.0: Última importação via utils.runtime; /dashboard/importacoes (histórico JSONL);
#            payloads compatíveis e view server-side.
#   • 1.8.x e anteriores: KPIs, séries, top-10, etc.

from datetime import date, datetime
from fastapi import APIRouter, Depends, Query, Response, HTTPException, Request
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, Numeric, String, distinct, case, and_

from database import get_db
from models import Contrato, ContratoCabecalho, ContratoLog  # <— adiciona ContratoLog
from utils.versioning import version, set_version_header

# --- Templates server-side ---
from fastapi.templating import Jinja2Templates
templates = Jinja2Templates(directory="templates")

# --- Suporte a arquivos em runtime via utils.runtime (com fallback seguro) ---
import os, json
from pathlib import Path

def _resolve_paths():
    try:
        from utils.runtime import path_ultima_importacao as _rt_json
        from utils.runtime import path_importacoes_jsonl as _rt_jsonl

        def _json_path() -> Path:
            env = os.environ.get("ULTIMA_IMPORTACAO_PATH")
            return Path(env) if env else _rt_json()

        def _jsonl_path() -> Path:
            env = os.environ.get("IMPORTACOES_JSONL_PATH")
            return Path(env) if env else _rt_jsonl()

        return _json_path, _jsonl_path
    except Exception:
        BASE_DIR = Path(__file__).resolve().parents[1]
        RUNTIME_DIR = BASE_DIR / "runtime"
        RUNTIME_DIR.mkdir(exist_ok=True)

        def _json_path() -> Path:
            env = os.environ.get("ULTIMA_IMPORTACAO_PATH")
            return Path(env) if env else (RUNTIME_DIR / "ultima_importacao.json")

        def _jsonl_path() -> Path:
            env = os.environ.get("IMPORTACOES_JSONL_PATH")
            return Path(env) if env else (RUNTIME_DIR / "importacoes.jsonl")

        return _json_path, _jsonl_path

_json_path, _jsonl_path = _resolve_paths()

router = APIRouter(
    prefix="/dashboard",
    tags=["Dashboard"],
    dependencies=[Depends(set_version_header)],
)

# ----------------- utilitários -----------------
def month_bucket(db: Session, date_col):
    dialect = db.bind.dialect.name if db.bind else "sqlite"
    if dialect == "postgresql":
        return func.to_char(func.date_trunc("month", date_col), "YYYY-MM")
    return func.strftime("%Y-%m", date_col)

def ilike_ci(column, term: str):
    term = f"%{(term or '').strip().lower()}%"
    return func.lower(column).like(term)

def pick_contract_number_col():
    try:
        cols = [(c.name, getattr(Contrato, c.name)) for c in Contrato.__table__.columns]
        lower = [(name.lower(), col) for name, col in cols]
        pri = [col for name, col in lower if "contrat" in name and ("num" in name or "numero" in name or name.endswith("_n"))]
        if pri: return pri[0]
        sec = [col for name, col in lower if "contrat" in name]
        if sec: return sec[0]
    except Exception:
        pass
    return None

def last_12_month_keys(today: date | None = None):
    if today is None:
        today = date.today()
    y, m = today.year, today.month
    keys = []
    for i in range(11, -1, -1):
        yy = y
        mm = m - i
        while mm <= 0:
            mm += 12
            yy -= 1
        keys.append(f"{yy:04d}-{mm:02d}")
    return keys

def _normalize_ultima(d: dict | None, path: Path) -> dict | None:
    if not d:
        return None
    out = {
        "arquivo": d.get("arquivo"),
        "linhas_total": d.get("linhas_total", 0),
        "inseridos": d.get("inseridos", 0),
        "atualizados": d.get("atualizados", 0),
        "trocas": d.get("trocas", 0),
        "retornos": d.get("retornos", 0),
        "erros": d.get("erros", 0),
        "timestamp": d.get("timestamp"),
        "lote_id": d.get("lote_id"),
        "status": d.get("status"),
        "mensagem": d.get("mensagem"),
        "processado_em": d.get("processado_em"),
        "_meta": {
            "path": str(path.resolve()),
            "updated_at": None,
            "router_version": "1.10.0",
        }
    }
    try:
        stat = path.stat()
        out["_meta"]["updated_at"] = datetime.fromtimestamp(stat.st_mtime).isoformat()
    except Exception:
        pass
    return out

def _ler_ultima_importacao() -> dict | None:
    p = _json_path()
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return _normalize_ultima(data, p)
    except Exception:
        return None

def _ler_historico(limit: int = 200) -> list[dict]:
    p = _jsonl_path()
    itens: list[dict] = []
    if p.exists():
        with p.open(encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    row = json.loads(ln)
                    itens.append(_normalize_ultima(row, p))
                except Exception:
                    continue
    itens = list(reversed(itens))
    return itens[:limit] if limit > 0 else itens

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return 0.0

# Helpers para mês atual com base em ContratoLog
def _log_date_column():
    for name in ("data_mov", "data", "created_at", "dt", "timestamp"):
        if hasattr(ContratoLog, name):
            return getattr(ContratoLog, name)
    return None

def _build_log_join_keys():
    """Monta condições de join Log→Contrato com o que existir no schema."""
    conds = []
    if hasattr(ContratoLog, "ativo") and hasattr(Contrato, "ativo"):
        conds.append(ContratoLog.ativo == Contrato.ativo)
    if hasattr(ContratoLog, "cod_cli") and hasattr(Contrato, "cod_cli"):
        conds.append(ContratoLog.cod_cli == Contrato.cod_cli)
    if hasattr(ContratoLog, "contrato_num") and hasattr(Contrato, "contrato_num"):
        conds.append(ContratoLog.contrato_num == Contrato.contrato_num)
    return conds

def _current_month_range(today: date | None = None):
    today = today or date.today()
    start = date(today.year, today.month, 1)
    # próximo 1º dia
    ny, nm = (today.year + 1, 1) if today.month == 12 else (today.year, today.month + 1)
    end = date(ny, nm, 1)
    return start, end

# ----------------- endpoints -----------------

@router.get("/clientes")
@version("1.10.0")
def autocomplete_clientes(
    db: Session = Depends(get_db),
    q: str | None = Query(default=None, description="Trecho do nome do cliente (case-insensitive)"),
    limit: int = Query(default=20, ge=1, le=200),
):
    rows = (
        db.query(
            func.coalesce(Contrato.nome_cli, "N/D").label("cliente"),
            func.count(Contrato.id).label("qtd")
        )
        .filter(ilike_ci(Contrato.nome_cli, q) if q else True)
        .group_by("cliente")
        .order_by(func.count(Contrato.id).desc(), func.coalesce(Contrato.nome_cli, "N/D"))
        .limit(limit)
        .all()
    )
    return {"clientes": [r.cliente for r in rows]}

@router.get("/")
@version("1.10.0")
def dashboard_data(
    response: Response,
    db: Session = Depends(get_db),
    de: date | None = Query(default=None),
    ate: date | None = Query(default=None),
    cliente: str | None = Query(default=None),
    somente_com_itens: bool = Query(default=False),
    incluir_ultima: bool = Query(default=True, description="Inclui objeto 'ultima_importacao' no payload"),
):
    q_itens = db.query(Contrato)
    if cliente: q_itens = q_itens.filter(ilike_ci(Contrato.nome_cli, cliente))
    if de: q_itens = q_itens.filter(Contrato.data_envio.isnot(None), Contrato.data_envio >= de)
    if ate: q_itens = q_itens.filter(Contrato.data_envio.isnot(None), Contrato.data_envio <= ate)

    filtros_aplicados = bool(cliente or de or ate or somente_com_itens)

    contrato_col = pick_contract_number_col()
    contrato_col_name = getattr(contrato_col, "key", None)
    if contrato_col_name:
        response.headers["X-Contrato-Col"] = str(contrato_col_name)
    contrato_key = func.nullif(func.trim(cast(contrato_col, String())), "") if contrato_col is not None else None

    if filtros_aplicados:
        total_contratos = q_itens.with_entities(distinct(contrato_key if contrato_key is not None else Contrato.cabecalho_id)).count()
        total_contratos_com_itens = total_contratos
    else:
        if contrato_key is not None:
            total_contratos = db.query(distinct(contrato_key)).count()
            if total_contratos == 0:
                total_contratos = db.query(func.count(distinct(Contrato.cabecalho_id))).scalar() or 0
        else:
            total_contratos = db.query(func.count(distinct(Contrato.cabecalho_id))).scalar() or 0
            if total_contratos == 0:
                total_contratos = db.query(func.count(ContratoCabecalho.id)).scalar() or 0
        total_contratos_com_itens = db.query(func.count(distinct(Contrato.cabecalho_id))).scalar() or 0

    total_itens_contrato = q_itens.count()

    # Totais básicos já existentes
    valor_mensal_total = q_itens.with_entities(func.coalesce(func.sum(cast(Contrato.valor_mensal, Numeric)), 0)).scalar() or 0
    valor_global_total = q_itens.with_entities(
        func.coalesce(func.sum(cast(Contrato.valor_global_contrato, Numeric)), 0)
    ).scalar() or 0

    # -------- BACKLOG (substitui 'valor_presente'): valor_mensal × meses_restantes, apenas ativos (meses_restantes > 0)
    backlog_total = q_itens.with_entities(
        func.coalesce(func.sum(cast(Contrato.valor_mensal, Numeric) * cast(Contrato.meses_restantes, Numeric)), 0)
    ).filter(Contrato.meses_restantes > 0).scalar() or 0

    # Série mensal (últimos 12)
    mes = month_bucket(db, Contrato.data_envio)
    bruto = (
        q_itens.filter(Contrato.data_envio.isnot(None))
        .with_entities(mes.label("mes"), func.coalesce(func.sum(cast(Contrato.valor_mensal, Numeric)), 0).label("valor"))
        .group_by("mes").all()
    )
    mapa = {r.mes: float(r.valor or 0) for r in bruto}
    mensal_12 = [{"mes": k, "valor": mapa.get(k, 0.0)} for k in last_12_month_keys()]

    # Top 10 por cliente (backlog por cliente)
    backlog_por_cliente_rows = (
        q_itens.filter(Contrato.meses_restantes > 0)
        .with_entities(
            func.coalesce(Contrato.nome_cli, "N/D").label("cliente"),
            func.coalesce(func.sum(cast(Contrato.valor_mensal, Numeric) * cast(Contrato.meses_restantes, Numeric)), 0).label("valor")
        )
        .group_by("cliente")
        .order_by(func.sum(cast(Contrato.valor_mensal, Numeric) * cast(Contrato.meses_restantes, Numeric)).desc())
        .limit(10)
        .all()
    )

    qtd_por_cliente_rows = (
        q_itens.with_entities(func.coalesce(Contrato.nome_cli, "N/D").label("cliente"), func.count(Contrato.id).label("qtd"))
        .group_by("cliente")
        .order_by(func.count(Contrato.id).desc())
        .limit(10)
        .all()
    )

    # Buckets por trimestre (mantido)
    bucket = case(
        (Contrato.meses_restantes <= 3,  "T1"),
        (Contrato.meses_restantes <= 6,  "T2"),
        (Contrato.meses_restantes <= 9,  "T3"),
        (Contrato.meses_restantes <= 12, "T4"),
        else_="GT12",
    )
    tri_rows = (
        q_itens.with_entities(
            bucket.label("bucket"),
            func.coalesce(func.sum(cast(Contrato.valor_mensal, Numeric)), 0).label("valor")
        )
        .group_by("bucket")
        .all()
    )
    tri_map = {r.bucket: float(r.valor or 0.0) for r in tri_rows}
    vencimento_trimestres = [
        {"bucket": "1º trimestre (0–3m)",   "valor": tri_map.get("T1", 0.0)},
        {"bucket": "2º trimestre (4–6m)",   "valor": tri_map.get("T2", 0.0)},
        {"bucket": "3º trimestre (7–9m)",   "valor": tri_map.get("T3", 0.0)},
        {"bucket": "4º trimestre (10–12m)", "valor": tri_map.get("T4", 0.0)},
        {"bucket": "Restante > 12m",        "valor": tri_map.get("GT12", 0.0)},
    ]

    # -------- Mês atual: devolvidos e entregues (via ContratoLog)
    devolvidos_mes = {"quantidade": 0, "valor_mensal": 0.0}
    entregues_mes = {"quantidade": 0, "valor_mensal": 0.0}
    vencidos = {"quantidade": 0, "valor_mensal": 0.0}

    date_col = _log_date_column()
    if date_col is not None:
        start, end = _current_month_range()
        base = db.query(ContratoLog).filter(date_col >= start, date_col < end)

        # Quantidades (conta eventos)
        try:
            devolvidos_mes["quantidade"] = base.filter(ContratoLog.tp_transacao == "RETORNO").count()
            entregues_mes["quantidade"] = base.filter(ContratoLog.tp_transacao == "ENVIO").count()
        except Exception:
            pass

        # Valores mensais somando os contratos envolvidos (join condicional com o que existir)
        join_conds = _build_log_join_keys()
        if join_conds:
            try:
                devolvidos_mes["valor_mensal"] = (
                    db.query(func.coalesce(func.sum(cast(Contrato.valor_mensal, Numeric)), 0))
                    .select_from(ContratoLog)
                    .join(Contrato, and_(*join_conds))
                    .filter(date_col >= start, date_col < end, ContratoLog.tp_transacao == "RETORNO")
                    .scalar() or 0
                )
                entregues_mes["valor_mensal"] = (
                    db.query(func.coalesce(func.sum(cast(Contrato.valor_mensal, Numeric)), 0))
                    .select_from(ContratoLog)
                    .join(Contrato, and_(*join_conds))
                    .filter(date_col >= start, date_col < end, ContratoLog.tp_transacao == "ENVIO")
                    .scalar() or 0
                )
            except Exception:
                # mantém zeros se join falhar
                pass

    # -------- Contratos vencidos: meses_restantes == 0 (quantidade e soma do valor_mensal)
    try:
        vencidos_q = db.query(Contrato).filter(Contrato.meses_restantes == 0)
        if cliente:
            vencidos_q = vencidos_q.filter(ilike_ci(Contrato.nome_cli, cliente))
        vencidos["quantidade"] = vencidos_q.count()
        vencidos["valor_mensal"] = (
            vencidos_q.with_entities(func.coalesce(func.sum(cast(Contrato.valor_mensal, Numeric)), 0)).scalar() or 0
        )
    except Exception:
        pass

    # ------ Payload final
    payload = {
        "total_contratos": int(total_contratos),
        "total_contratos_com_itens": int(total_contratos_com_itens),
        "total_itens_contrato": int(total_itens_contrato),

        "valor_mensal_total": _to_float(valor_mensal_total),
        "valor_global_total": _to_float(valor_global_total),

        # NOVO: backlog
        "backlog_total": _to_float(backlog_total),

        "mensal_por_mes": mensal_12,
        "qtd_por_cliente": [{"cliente": r.cliente or "N/D", "quantidade": int(r.qtd)} for r in qtd_por_cliente_rows],

        # NOVO: backlog por cliente (mantemos compat abaixo)
        "backlog_por_cliente": [{"cliente": r.cliente or "N/D", "valor": _to_float(r.valor)} for r in backlog_por_cliente_rows],

        "vencimento_trimestres": vencimento_trimestres,

        # NOVOS totais
        "devolvidos_mes_atual": {
            "quantidade": int(devolvidos_mes["quantidade"]),
            "valor_mensal": _to_float(devolvidos_mes["valor_mensal"]),
        },
        "entregues_mes_atual": {
            "quantidade": int(entregues_mes["quantidade"]),
            "valor_mensal": _to_float(entregues_mes["valor_mensal"]),
        },
        "contratos_vencidos": {
            "quantidade": int(vencidos["quantidade"]),
            "valor_mensal": _to_float(vencidos["valor_mensal"]),
        },
    }

    # Compatibilidade com versões antigas do front
    payload["valor_presente_total"] = payload["backlog_total"]
    payload["valor_presente_por_cliente"] = payload["backlog_por_cliente"]

    if incluir_ultima:
        payload["ultima_importacao"] = _ler_ultima_importacao()

    return payload

# JSON isolado para o card “Última importação” (mantido)
@router.get("/ultima_importacao")
@version("1.10.0")
def ultima_importacao():
    data = _ler_ultima_importacao()
    if data is None:
        return {"exists": False}
    return {"exists": True, **data}

# Histórico de importações (mantido)
@router.get("/importacoes")
@version("1.10.0")
def importacoes(limit: int = Query(200, ge=1, le=1000)):
    try:
        itens = _ler_historico(limit)
        return {
            "exists": True if itens else False,
            "path_jsonl": str(_jsonl_path().resolve()),
            "total": len(itens),
            "itens": itens,
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao ler histórico: {e}")

# View server-side do dashboard (mantida)
@router.get("/view")
@version("1.10.0")
def dashboard_view(request: Request):
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "ultima_importacao": _ler_ultima_importacao(),
        },
    )
