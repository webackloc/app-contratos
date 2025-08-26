# utils/recalc.py
# Versão: 2.0.0 (2025-08-14)
# CHANGELOG:
# - Unificação com utils/recalculo_contratos.py (sem quebrar fórmulas existentes).
# - Novo orquestrador recalc_contrato(session, cabecalho_id) que:
#   * carrega cabeçalho e itens,
#   * calcula meses_restantes, valor_global_contrato, valor_presente_contrato
#     usando as rotinas já existentes,
#   * acumula totais úteis para o dashboard,
#   * retorna um resumo.
#
# Observações:
# - Mantém utils/recalculo_contratos.py intocado (backward compatible).
# - Índice de reajuste anual do CABEÇALHO é usado no valor presente.
# - Valor mensal por item pode ser None/str com vírgula; a função
#   _to_float existente lida com isso.
#
# Dependências: models.Contrato, models.ContratoCabecalho, SQLAlchemy Session.

from __future__ import annotations
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import Contrato, ContratoCabecalho

# Importa as rotinas existentes (preservadas) — NÃO alterar essas fórmulas.
from utils.recalculo_contratos import (  # noqa: F401
    calc_meses_restantes,
    calc_valor_global,
    calc_valor_presente,
)

def _safe_date(d) -> date | None:
    return d if isinstance(d, date) else None

def recalc_contrato(session: Session, cabecalho_id: int, hoje: date | None = None) -> dict:
    """
    Recalcula campos derivados de TODOS os itens de um contrato (cabecalho_id):
      - meses_restantes
      - valor_global_contrato
      - valor_presente_contrato
    Usando as fórmulas preservadas em utils/recalculo_contratos.py.

    Retorna um resumo:
      {
        "cabecalho_id": int,
        "itens": int,
        "ativos_abertos": int,
        "valor_mensal_total": float,
        "prazo_contratual": int,
      }
    """
    hoje = hoje or date.today()

    cab = session.execute(
        select(ContratoCabecalho).where(ContratoCabecalho.id == cabecalho_id)
    ).scalar_one_or_none()
    if not cab:
        return {
            "cabecalho_id": cabecalho_id,
            "itens": 0,
            "ativos_abertos": 0,
            "valor_mensal_total": 0.0,
            "prazo_contratual": 0,
        }

    prazo = int(cab.prazo_contratual or 0)
    indice_anual = cab.indice_reajuste  # pode ser "6%", "0,06", 0.06 etc.

    itens = session.execute(
        select(Contrato).where(Contrato.cabecalho_id == cabecalho_id)
    ).scalars().all()

    valor_mensal_total = 0.0
    ativos_abertos = 0

    for it in itens:
        # Meses restantes com base no início (data_envio) e no PRAZO do cabeçalho.
        inicio = _safe_date(it.data_envio) or hoje
        it.meses_restantes = calc_meses_restantes(
            data_inicio=inicio,
            periodo_contratual_meses=prazo
        )

        # Valor global do contrato (por item) = valor_mensal * prazo
        it.valor_global_contrato = calc_valor_global(
            valor_mensal=it.valor_mensal,
            periodo_contratual_meses=prazo
        )

        # Valor presente (por item) descontando pela taxa mensal equivalente do índice anual
        it.valor_presente_contrato = calc_valor_presente(
            valor_mensal=it.valor_mensal,
            meses_restantes=it.meses_restantes,
            indice_reajuste_anual=indice_anual
        )

        if it.status == "ATIVO":
            ativos_abertos += 1
            # Total mensal corrente (somatório apenas dos itens ATIVOS)
            try:
                vm = float(it.valor_mensal or 0.0)
            except Exception:
                vm = 0.0
            valor_mensal_total += vm

        session.add(it)

    return {
        "cabecalho_id": cabecalho_id,
        "itens": len(itens),
        "ativos_abertos": ativos_abertos,
        "valor_mensal_total": round(valor_mensal_total, 2),
        "prazo_contratual": prazo,
    }
