# utils/recalculo_contratos.py
from datetime import date
from typing import Optional
from decimal import Decimal

def _to_float(value) -> float:
    """Converte int/float/Decimal/str (ex: '0,06', '6%', '6') em float; defaults p/ 0.0."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, str):
        s = value.strip().replace('%', '').replace(' ', '')
        s = s.replace(',', '.')  # aceita "0,06"
        if not s:
            return 0.0
        try:
            f = float(s)
        except ValueError:
            return 0.0
        # Se vier "6" (6%), normaliza para 0.06
        if f > 1.0:
            f = f / 100.0
        return f
    try:
        return float(value)
    except Exception:
        return 0.0

def _diff_meses(inicio: date, fim: date) -> int:
    if not inicio:
        return 0
    if not fim:
        fim = date.today()
    y = fim.year - inicio.year
    m = fim.month - inicio.month
    meses = y * 12 + m
    if fim.day < inicio.day:
        meses -= 1
    return max(0, meses)

def calc_meses_restantes(data_inicio: Optional[date], periodo_contratual_meses: int) -> int:
    p = int(periodo_contratual_meses or 0)
    if p <= 0:
        return 0
    passados = _diff_meses(data_inicio or date.today(), date.today())
    return max(0, p - passados)

def calc_valor_global(valor_mensal, periodo_contratual_meses: int) -> float:
    vm = _to_float(valor_mensal)
    p = int(periodo_contratual_meses or 0)
    return round(vm * p, 2)

def _taxa_mensal(indice_reajuste_anual) -> float:
    """Converte taxa anual (0.06, '6%', '0,06') em taxa mensal equivalente."""
    anual = _to_float(indice_reajuste_anual)
    if anual <= 0:
        return 0.0
    return (1.0 + anual) ** (1/12) - 1

def calc_valor_presente(valor_mensal, meses_restantes: int, indice_reajuste_anual) -> float:
    vm = _to_float(valor_mensal)
    n = int(meses_restantes or 0)
    if n <= 0 or vm <= 0:
        return 0.0
    i = _taxa_mensal(indice_reajuste_anual)
    if i <= 0:
        return round(vm * n, 2)
    pv = vm * (1 - (1 + i) ** (-n)) / i
    return round(pv, 2)
