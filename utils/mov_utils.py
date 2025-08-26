# utils/mov_utils.py
# Versão: 1.1.0 (2025-08-14)
# CHANGELOG:
# - parse_data_mov: agora aceita objetos date/datetime, inteiros/floats (serial do Excel),
#   e mais formatos de data (%Y/%m/%d, %d.%m.%Y), mantendo compatibilidade.
# - norm_tp: mapeia abreviações e variações comuns (e.g., "env", "e", "ret", "r", "trc", "t")
#   para os canônicos "ENVIO", "RETORNO", "TROCA" (sem quebrar o comportamento anterior).
# - make_mov_hash: mantém a MESMA assinatura e concatenação base (compatível com hashes já gerados).
# - Novos helpers opcionais (não usados obrigatoriamente): date_to_iso, try_parse_decimal_to_float,
#   normalize_text_basic. Podem ser usados em pré-validações sem afetar a idempotência.

from __future__ import annotations

from hashlib import sha256
from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation
import re
from typing import Optional


# ---------- Datas ----------

def _excel_serial_to_date(value: float) -> date:
    """
    Converte serial do Excel em date.
    Regra: Excel em Windows começa em 1899-12-30 (corrigindo bug do 1900/02/29).
    """
    base = date(1899, 12, 30)
    return base + timedelta(days=int(value))


def parse_data_mov(s: object) -> datetime:
    """
    Converte vários formatos de entrada para datetime (00:00).
    Aceita:
      - str: "YYYY-MM-DD", "DD/MM/YYYY", "DD-MM-YYYY", "YYYY/MM/DD", "DD.MM.YYYY"
      - int/float: serial do Excel (dias desde 1899-12-30)
      - datetime/date: retorna normalizado
    Levanta ValueError em caso inválido.
    """
    if isinstance(s, datetime):
        return s.replace(hour=0, minute=0, second=0, microsecond=0)
    if isinstance(s, date):
        return datetime(s.year, s.month, s.day)
    if isinstance(s, (int, float)):
        # Trata como serial do Excel
        d = _excel_serial_to_date(float(s))
        return datetime(d.year, d.month, d.day)

    text = (s or "").strip()
    if not text:
        raise ValueError("data_mov inválida: ''")

    # Tenta detectar AAAA-MM-DD ou similares rapidamente
    fmts = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%d.%m.%Y",
    )
    for fmt in fmts:
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        except ValueError:
            pass

    # Última tentativa: números puros como serial do Excel em string
    if text.isdigit():
        d = _excel_serial_to_date(int(text))
        return datetime(d.year, d.month, d.day)

    raise ValueError(f"data_mov inválida: {text!r}")


def date_to_iso(d: date | datetime) -> str:
    """Converte date/datetime para 'YYYY-MM-DD'."""
    if isinstance(d, datetime):
        d = d.date()
    return d.isoformat()


# ---------- Transações ----------

def norm_tp(tp: Optional[str]) -> str:
    """
    Normaliza o tipo de transação para {ENVIO, RETORNO, TROCA}.
    Compatível com versões anteriores (se já era 'ENVIO' etc., permanece igual).
    Também aceita abreviações comuns: e/env -> ENVIO, r/ret -> RETORNO, t/trc -> TROCA.
    """
    t = (tp or "").strip()
    if not t:
        return ""
    t_cf = t.casefold()

    # Mapeamentos de abreviações/variações
    aliases = {
        "envio": "ENVIO", "env": "ENVIO", "e": "ENVIO",
        "retorno": "RETORNO", "ret": "RETORNO", "r": "RETORNO",
        "troca": "TROCA", "trc": "TROCA", "t": "TROCA",
    }
    if t_cf in aliases:
        return aliases[t_cf]

    # Mantém comportamento anterior (apenas maiúsculas)
    return t.upper()


# ---------- Hash (idempotência) ----------

def make_mov_hash(
    contrato_num: str,
    cod_cli: str,
    tp: str,
    ativo: str,
    data_mov_iso: str,
    ativo_novo: str = "",
) -> str:
    """
    Gera hash idempotente da movimentação.
    IMPORTANTE: Mantém a mesma concatenação e normalização já usadas anteriormente
    para não invalidar hashes existentes.
    """
    base = f"{(contrato_num or '').strip()}|{(cod_cli or '').strip()}|{norm_tp(tp)}|{(ativo or '').strip()}|{(ativo_novo or '').strip()}|{data_mov_iso}"
    return sha256(base.encode("utf-8")).hexdigest()


# ---------- Utilidades (opcionais) ----------

_ws_re = re.compile(r"\s+")

def normalize_text_basic(s: Optional[str]) -> str:
    """strip + colapso de espaços internos; útil para campos livres (não afeta hash)."""
    return _ws_re.sub(" ", (s or "").strip())

def try_parse_decimal_to_float(s: Optional[str]) -> Optional[float]:
    """
    Converte strings monetárias como '1.234,56' ou '1,234.56' em float.
    Retorna None se vazio. Lança ValueError se inválido.
    """
    txt = (s or "").strip()
    if txt == "":
        return None
    # Heurística: troca separadores para formato Decimal padrão
    # 1) remove espaços
    txt = txt.replace(" ", "")
    # 2) se vírgula e ponto presentes, assume vírgula decimal (pt-BR): 1.234,56 -> 1234.56
    if "," in txt and "." in txt:
        txt = txt.replace(".", "").replace(",", ".")
    # 3) se só vírgula, troca por ponto
    elif "," in txt and "." not in txt:
        txt = txt.replace(",", ".")
    try:
        return float(Decimal(txt))
    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"valor monetário inválido: {s!r}") from e
