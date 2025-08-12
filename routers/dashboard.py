"""
Módulo: Dashboard
Versão: 1.6.0
Data: 2025-08-11
Autor: Leonardo Muller

Novidades (1.6.0):
  • 'qtd_por_cliente' agora retorna somente TOP 10 (por quantidade de itens).
  • Novo agregado 'vencimento_trimestres': soma de valor_mensal por faixas de meses_restantes
    (0–3, 4–6, 7–9, 10–12 e >12). Substitui o gráfico de distribuição.
  • Mantida compatibilidade com chaves antigas do front (quando aplicável).

Baseado em v1.5.1: autocomplete de clientes, série 12 meses, KPI robusto e
"Valor Presente por Cliente (Top 10)". (Fonte: última versão enviada)
"""

from datetime import date
from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, Numeric, String, distinct, case

from database import get_db
from models import Contrato, ContratoCabecalho
from utils.versioning import version, set_version_header

router = APIRouter(
    prefix="/dashboard",
    tags=["Dashboard"],
    dependencies=[Depends(set_version_header)],
)

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

@router.get("/clientes")
@version("1.6.0")
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
@version("1.6.0")
def dashboard_data(
    response: Response,
    db: Session = Depends(get_db),
    de: date | None = Query(default=None),
    ate: date | None = Query(default=None),
    cliente: str | None = Query(default=None),
    somente_com_itens: bool = Query(default=False),
):
    # Base (itens) com filtros
    q_itens = db.query(Contrato)
    if cliente: q_itens = q_itens.filter(ilike_ci(Contrato.nome_cli, cliente))
    if de: q_itens = q_itens.filter(Contrato.data_envio.isnot(None), Contrato.data_envio >= de)
    if ate: q_itens = q_itens.filter(Contrato.data_envio.isnot(None), Contrato.data_envio <= ate)

    filtros_aplicados = bool(cliente or de or ate or somente_com_itens)

    # coluna dinâmica do nº do contrato (para total_contratos)
    contrato_col = pick_contract_number_col()
    contrato_col_name = getattr(contrato_col, "key", None)
    if contrato_col_name:
        response.headers["X-Contrato-Col"] = str(contrato_col_name)
    contrato_key = func.nullif(func.trim(cast(contrato_col, String())), "") if contrato_col is not None else None

    # Totais
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

    # Somatórios (conjunto filtrado)
    valor_mensal_total = q_itens.with_entities(func.coalesce(func.sum(cast(Contrato.valor_mensal, Numeric)), 0)).scalar() or 0
    valor_global_total = q_itens.with_entities(func.coalesce(func.sum(cast(Contrato.valor_global_contrato, Numeric)), 0)).scalar() or 0
    valor_presente_total = q_itens.with_entities(func.coalesce(func.sum(cast(Contrato.valor_presente_contrato, Numeric)), 0)).scalar() or 0

    # Série mensal (12 meses fixos)
    mes = month_bucket(db, Contrato.data_envio)
    bruto = (
        q_itens.filter(Contrato.data_envio.isnot(None))
        .with_entities(mes.label("mes"), func.coalesce(func.sum(cast(Contrato.valor_mensal, Numeric)), 0).label("valor"))
        .group_by("mes").all()
    )
    mapa = {r.mes: float(r.valor or 0) for r in bruto}
    mensal_12 = [{"mes": k, "valor": mapa.get(k, 0.0)} for k in last_12_month_keys()]

    # Valor Presente por Cliente (Top 10)
    valor_presente_por_cliente_rows = (
        q_itens.with_entities(
            func.coalesce(Contrato.nome_cli, "N/D").label("cliente"),
            func.coalesce(func.sum(cast(Contrato.valor_presente_contrato, Numeric)), 0).label("valor")
        )
        .group_by("cliente")
        .order_by(func.sum(cast(Contrato.valor_presente_contrato, Numeric)).desc())
        .limit(10)
        .all()
    )

    # Quantidade de Itens por Cliente (Top 10)
    qtd_por_cliente_rows = (
        q_itens.with_entities(func.coalesce(Contrato.nome_cli, "N/D").label("cliente"), func.count(Contrato.id).label("qtd"))
        .group_by("cliente")
        .order_by(func.count(Contrato.id).desc())
        .limit(10)
        .all()
    )

    # NOVO: Valores Mensais a Vencer por Trimestre (por meses_restantes, somando valor_mensal)
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

    # Compat: chave antiga mantida com o mesmo conteúdo do "presente"
    valor_global_por_cliente_rows = valor_presente_por_cliente_rows

    def to_float(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    return {
        "total_contratos": int(total_contratos),
        "total_contratos_com_itens": int(total_contratos_com_itens),
        "total_itens_contrato": int(total_itens_contrato),

        "valor_mensal_total": to_float(valor_mensal_total),
        "valor_global_total": to_float(valor_global_total),
        "valor_presente_total": to_float(valor_presente_total),

        "mensal_por_mes": mensal_12,
        "qtd_por_cliente": [{"cliente": r.cliente or "N/D", "quantidade": int(r.qtd)} for r in qtd_por_cliente_rows],

        "valor_presente_por_cliente": [{"cliente": r.cliente or "N/D", "valor": to_float(r.valor)} for r in valor_presente_por_cliente_rows],
        "valor_global_por_cliente":  [{"cliente": r.cliente or "N/D", "valor": to_float(r.valor)} for r in valor_global_por_cliente_rows],

        # Novo dataset para o gráfico de colunas por trimestres
        "vencimento_trimestres": vencimento_trimestres,
    }
