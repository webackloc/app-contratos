# services/movimentacao_service.py
# Versão: 4.5.0 (2025-09-02)
#
# NOVIDADES (desde 4.3.0):
# - [FIX] Troca de "delete físico" por SOFT DELETE em ENVIO: itens existentes com mesmo
#   "ativo" são marcados como RETORNADO (status/data_retorno) em vez de remover do banco.
#   Isso elimina o IntegrityError por FK em contratos_logs e preserva histórico.
# - [IMP] Usa cabecalho_id_resolvido vindo do preview (quando presente), evitando buscas
#   repetidas e removendo fonte de DetachedInstanceError.
# - [IMP] Cacheia apenas IDs de cabeçalho (não objetos ORM), e sempre reobtém via sess.get().
# - [IMP] Herdar data_envio do item que retorna ao fazer TROCA, e setar data_troca no novo.
# - [IMP] Proteções adicionais para campos opcionais (descricao_produto, etc.).
#
# Mantém:
# - Idempotência por mov_hash e reaplicação inteligente.
# - Cópia de período e meta-campos do cabeçalho para o item.
# - Compatibilidade com modelos de coluna variados (detecção heurística).
#
# Histórico anterior (4.3.0): ver arquivo anterior.

from __future__ import annotations

import re
import calendar
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, select, update
from sqlalchemy.orm import Session
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.exc import SQLAlchemyError

from models import (
    Contrato,
    ContratoCabecalho,
    ContratoLog,
    MovimentacaoItem,
    MovimentacaoLote,
)
from utils.mov_utils import norm_tp, parse_data_mov, make_mov_hash

# -------------------------------------------------------------------
# Cache de cabeçalhos por chave (contrato_num[, cod_cli]) -> ID (ou None)
# (não cacheia objetos ORM para evitar DetachedInstanceError)
_CAB_ID_CACHE: Dict[Tuple[str, Optional[str]], Optional[int]] = {}


# ------------------------------ utils ---------------------------------

def _as_str(v: Any) -> str:
    return (str(v) if v is not None else "").strip()

def _row_get_str(row: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        if k in row and row[k] is not None:
            return _as_str(row[k])
    return ""

def _tp(row: Dict[str, Any]) -> str:
    return norm_tp(_row_get_str(row, "tp_transacao", "tipo", "tipo_movimento", "tp_norm"))

def _tipo_troca(row: Dict[str, Any]) -> str:
    return norm_tp(
        _row_get_str(
            row,
            "tipo_mov_troca",
            "tipodemovimentotroca",
            "tipo_troca",
            "subtipo_troca",
            "papel_troca",
        )
    )

def _os_key(row: Dict[str, Any]) -> str:
    return _row_get_str(row, "os", "ordem_servico", "ordemservico", "ordem", "os_norm")

def _sa_column_keys(model) -> List[str]:
    try:
        insp = sa_inspect(model)
        return [attr.key for attr in insp.mapper.column_attrs]
    except Exception:
        return [n for n in dir(model) if not n.startswith("_")]

def _norm_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())

def _get_model_col_by_keywords(model, keywords_any: List[List[str]]):
    keys = _sa_column_keys(model)
    for k in keys:
        nk = _norm_name(k)
        for group in keywords_any:
            if all(term in nk for term in group):
                return getattr(model, k, None)
    return None

# Detecção **estrita** de coluna de cliente (evita confundir com "nome_cliente").
def _detect_cli_col(model):
    keys = _sa_column_keys(model)
    for k in keys:
        nk = _norm_name(k)
        if "nomecliente" in nk:
            continue
        has_cli = ("cli" in nk) or ("cliente" in nk)
        has_key = ("cod" in nk) or ("codigo" in nk) or ("id" in nk)
        if has_cli and has_key:
            return getattr(model, k, None)
    return (
        getattr(model, "cod_cli", None)
        or getattr(model, "cod_cliente", None)
        or getattr(model, "cliente_id", None)
        or getattr(model, "id_cliente", None)
    )


# ---------------------- datas / período ---------------------------

_DATE_FMTS = ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"]

def _parse_date(val):
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return val.date() if isinstance(val, datetime) else val
    s = str(val).strip()
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def _add_months(d: date, months: int) -> date:
    m = d.month - 1 + int(months)
    y = d.year + m // 12
    m = m % 12 + 1
    day = min(d.day, calendar.monthrange(y, m)[1])
    return date(y, m, day)

def _pick_attr(obj, *candidates):
    for c in candidates:
        if hasattr(obj, c):
            v = getattr(obj, c)
            if v is not None:
                return v
    return None

def _get_periodo_from_cab(cab: ContratoCabecalho) -> tuple[date | None, date | None]:
    """Extrai inicio/fim do cabeçalho; se fim ausente, calcula por prazo_contratual."""
    ini = _pick_attr(
        cab,
        "periodo_inicio", "vigencia_inicio", "dt_inicio",
        "inicio_vigencia", "data_inicio", "inicio_contrato",
    )
    fim = _pick_attr(
        cab,
        "periodo_fim", "vigencia_fim", "dt_fim",
        "fim_vigencia", "data_fim", "fim_contrato",
    )
    ini_d = _parse_date(ini)
    fim_d = _parse_date(fim)
    if not fim_d:
        prazo = _pick_attr(cab, "prazo_contratual", "prazo", "meses_contrato", "tempo_contrato")
        if ini_d and prazo is not None:
            try:
                fim_d = _add_months(ini_d, int(prazo))
            except Exception:
                pass
    return ini_d, fim_d

def _set_periodo_on_item(item: Contrato, ini: date | None, fim: date | None):
    """Aplica periodo_inicio/periodo_fim nos nomes existentes do modelo de item."""
    ini_targets = (
        "periodo_inicio", "vigencia_inicio", "dt_inicio", "competencia_inicio",
        "inicio_vigencia", "inicio_contrato",
    )
    fim_targets = (
        "periodo_fim", "vigencia_fim", "dt_fim", "competencia_fim",
        "fim_vigencia", "fim_contrato",
    )
    if ini:
        for t in ini_targets:
            if hasattr(item, t):
                setattr(item, t, ini)
                break
    if fim:
        for t in fim_targets:
            if hasattr(item, t):
                setattr(item, t, fim)
                break


# ---------------------- números/moedas do arquivo ------------------

def _parse_decimal_br(val: Any) -> Optional[float]:
    if val is None:
        return None
    s = _as_str(val)
    if not s:
        return None
    s = s.replace("R$", "").replace(" ", "")
    # 1.234,56 -> 1234.56 ; 1234,56 -> 1234.56 ; 1234.56 -> 1234.56
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def _row_get_valor_mensal(row: Dict[str, Any]) -> Optional[float]:
    # tenta chaves mais comuns; mantém compat com versões antigas
    for key in [
        "valor_mensal_item",
        "valor_mensal",
        "vl_mensal",
        "vl_mensal_item",
        "valor_unitario",
        "vl_unitario",
        "valor",
        "preco",
    ]:
        if key in row and row[key] is not None:
            v = _parse_decimal_br(row[key])
            if v is not None:
                return v
    return None

def _coerce_for_column(value: Any, column_attr):
    if column_attr is None:
        return value
    try:
        coltype = getattr(column_attr, "type", None)
        pytype = getattr(coltype, "python_type", None)
        if pytype is int:
            try:
                return int(value)
            except Exception:
                return 0
        return value
    except Exception:
        return value


# --------------------- cabeçalho (via ID resolvido) -----------------

def _cabecalho_from_payload(sess: Session, row: Dict[str, Any]) -> Optional[ContratoCabecalho]:
    """
    Se o preview já colocou 'cabecalho_id_resolvido', usa ele (mais rápido e seguro).
    """
    cid = row.get("cabecalho_id_resolvido")
    try:
        cid_int = int(cid) if cid is not None and str(cid).strip() != "" else None
    except Exception:
        cid_int = None
    if cid_int:
        return sess.get(ContratoCabecalho, cid_int)
    return None

def _find_cabecalho(sess: Session, contrato_num: str, cod_cli: Optional[str]) -> Optional[ContratoCabecalho]:
    """
    Busca por número (e cod_cli se existir no modelo). Cacheia apenas o ID.
    """
    key = (str(contrato_num), _as_str(cod_cli) or None)
    if key in _CAB_ID_CACHE:
        cid = _CAB_ID_CACHE[key]
        return sess.get(ContratoCabecalho, cid) if cid else None

    contrato_col = (
        _get_model_col_by_keywords(ContratoCabecalho, [["contrato","num"], ["contrato","numero"], ["num","contrato"], ["numero","contrato"], ["contrato","id"]])
        or getattr(ContratoCabecalho, "contrato_num", None)
        or getattr(ContratoCabecalho, "contrato_n", None)
        or getattr(ContratoCabecalho, "numero", None)
        or getattr(ContratoCabecalho, "contrato", None)
    )
    cli_col = _detect_cli_col(ContratoCabecalho)

    cab = None
    if contrato_col is not None:
        v_contrato = _coerce_for_column(contrato_num, contrato_col)
        if cli_col is not None and (cod_cli is not None and _as_str(cod_cli) != ""):
            v_cli = _coerce_for_column(cod_cli, cli_col)
            stmt = select(ContratoCabecalho).where(and_(contrato_col == v_contrato, cli_col == v_cli))
            cab = (
                sess.execute(stmt.order_by(getattr(ContratoCabecalho, "id", contrato_col).asc()).limit(1))
                .scalars()
                .first()
            )
        if cab is None:
            stmt2 = select(ContratoCabecalho).where(contrato_col == v_contrato)
            cab = (
                sess.execute(stmt2.order_by(getattr(ContratoCabecalho, "id", contrato_col).asc()).limit(1))
                .scalars()
                .first()
            )

    _CAB_ID_CACHE[key] = getattr(cab, "id", None) if cab else None
    return cab

def _require_cabecalho(sess: Session, row: Dict[str, Any], contrato_num: str, cod_cli: Optional[str]) -> ContratoCabecalho:
    cab = _cabecalho_from_payload(sess, row)
    if not cab:
        cab = _find_cabecalho(sess, contrato_num, cod_cli)
    if not cab:
        extra = f" (cod_cli '{cod_cli}' ignorado)" if (cod_cli is not None and _as_str(cod_cli) != "") else ""
        raise ValueError(f"Cabeçalho não encontrado para contrato='{contrato_num}'." + extra)
    return cab


# --------- helpers: número do contrato ----------------------------

def _extract_contrato_num_from_cab(cab: ContratoCabecalho) -> Optional[str]:
    for name in [
        "contrato_num", "contrato_n", "numero", "contrato", "numero_contrato",
        "num_contrato", "n_contrato", "contratoid",
    ]:
        if hasattr(cab, name):
            v = getattr(cab, name)
            if v is not None and _as_str(v) != "":
                return _as_str(v)
    return None

def _extract_contrato_num_from_row(row: Dict[str, Any]) -> Optional[str]:
    s = _row_get_str(row, "contrato_num_norm", "contrato_num", "contraton", "contrato", "numero_contrato", "num_contrato", "n_contrato")
    return s or None

def _apply_contrato_num_on_item(item: Contrato, contrato_num: Optional[str]) -> None:
    if not contrato_num:
        return
    possible_names = [
        "contrato_num", "contrato_n", "numero_contrato", "num_contrato",
        "n_contrato", "contrato", "contratoid", "numero",
    ]
    for name in possible_names:
        if hasattr(item, name):
            try:
                setattr(item, name, contrato_num)
                break
            except Exception:
                pass


# --------- período (string / meta) a partir do cabeçalho ------------

def _get_attr(obj: Any, *names: str):
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v is not None and (str(v).strip() != ""):
                return v
    return None

def _periodo_fields_from_cab(cab: ContratoCabecalho) -> Dict[str, Any]:
    """Retém campos 'periodo'/'prazo*'/'indice_reajuste' se existirem no modelo do item."""
    out: Dict[str, Any] = {}
    try:
        cols = {attr.key for attr in sa_inspect(Contrato).mapper.column_attrs}
    except Exception:
        cols = set()

    # periodo (texto)
    if "periodo" in cols:
        val = _get_attr(cab, "periodo", "vigencia")
        if not val:
            ini = _get_attr(cab, "inicio_vigencia", "vigencia_inicio", "inicio", "periodo_inicio", "dt_inicio")
            fim = _get_attr(cab, "fim_vigencia", "vigencia_fim", "fim", "periodo_fim", "dt_fim")
            if ini or fim:
                val = f"{_as_str(ini)} a {_as_str(fim)}".strip()
        if not val:
            meses = _get_attr(cab, "prazo_contratual", "meses_contrato", "tempo_contrato", "prazo")
            if meses is not None:
                val = f"{meses}m"
        if val:
            out["periodo"] = _as_str(val)

    # prazo numérico
    prazo_val = _get_attr(cab, "prazo_contratual", "meses_contrato", "tempo_contrato", "prazo")
    if prazo_val is not None:
        for cand in ["prazo_contratual", "meses_contrato", "tempo_contrato"]:
            if cand in cols and cand not in out:
                out[cand] = prazo_val
                break

    # índice de reajuste
    if "indice_reajuste" in cols:
        ir = _get_attr(cab, "indice_reajuste")
        if ir is not None:
            out["indice_reajuste"] = ir

    return out


# --------------------------- operações ----------------------------

def _find_item_aberto(sess: Session, cab_id: int, cod_cli: Optional[str], ativo: str) -> Optional[Contrato]:
    """Procura o item ATIVO.
    1) cab+cli+ativo (quando houver coluna de cliente e valor)
    2) cab+ativo (ignora cli)
    3) **global**: ativo (ignora cab/cli)
    """
    base_filters = [Contrato.ativo == ativo]
    if hasattr(Contrato, "status"):
        base_filters.append(Contrato.status == "ATIVO")

    cli_col = _detect_cli_col(Contrato)

    if cli_col is not None and (cod_cli is not None and _as_str(cod_cli) != ""):
        v_cli = _coerce_for_column(cod_cli, cli_col)
        stmt1 = (
            select(Contrato)
            .where(and_(Contrato.cabecalho_id == cab_id, *(base_filters + [cli_col == v_cli])))
            .order_by(getattr(Contrato, "data_envio", getattr(Contrato, "id")).asc(), getattr(Contrato, "id").asc())
            .limit(1)
        )
        found = sess.execute(stmt1).scalars().first()
        if found:
            return found

    stmt2 = (
        select(Contrato)
        .where(and_(Contrato.cabecalho_id == cab_id, *base_filters))
        .order_by(getattr(Contrato, "data_envio", getattr(Contrato, "id")).asc(), getattr(Contrato, "id").asc())
        .limit(1)
    )
    found = sess.execute(stmt2).scalars().first()
    if found:
        return found

    stmt3 = (
        select(Contrato)
        .where(and_(*base_filters))
        .order_by(getattr(Contrato, "data_envio", getattr(Contrato, "id")).asc(), getattr(Contrato, "id").asc())
        .limit(1)
    )
    return sess.execute(stmt3).scalars().first()

def _filter_model_kwargs(model, data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        colnames = {attr.key for attr in sa_inspect(model).mapper.column_attrs}
    except Exception:
        colnames = set()
    return {k: v for k, v in data.items() if k in colnames}

def _ensure_min_fields(model, data: Dict[str, Any], minimo: Dict[str, Any]) -> Dict[str, Any]:
    try:
        colnames = {attr.key for attr in sa_inspect(model).mapper.column_attrs}
    except Exception:
        colnames = set()
    for k, v in minimo.items():
        if k in colnames and k not in data:
            data[k] = v
    return data

def _soft_close_itens_por_ativo(sess: Session, ativo: str, data_retorno: date) -> int:
    """
    SOFT DELETE: fecha itens com mesmo 'ativo' marcando RETORNADO + data_retorno.
    Retorna quantos itens foram atualizados.
    """
    if not ativo:
        return 0

    set_values = {}
    if hasattr(Contrato, "status"):
        set_values["status"] = "RETORNADO"
    if hasattr(Contrato, "data_retorno"):
        set_values["data_retorno"] = data_retorno
    if hasattr(Contrato, "tp_transacao"):
        set_values["tp_transacao"] = "RETORNO"

    if not set_values:
        # Sem colunas para soft close; não faz nada.
        return 0

    stmt = (
        update(Contrato)
        .where(Contrato.ativo == ativo)
        .values(**set_values)
    )
    res = sess.execute(stmt)
    return int(res.rowcount or 0)

def _envio(
    sess: Session,
    row: Dict[str, Any],
    cab: ContratoCabecalho,
    data_mov: datetime,
    mov_hash: str,
    *,
    override_data_envio: Optional[date] = None,
    data_troca: Optional[date] = None,
) -> Contrato:
    serial = _row_get_str(row, "serial", "ativo_serial") or None

    # campos “textuais/meta” derivados do cabeçalho (se existirem no modelo do item)
    derivados = _periodo_fields_from_cab(cab)

    # valor_mensal do arquivo
    valor_mensal_val = _row_get_valor_mensal(row)

    # tentar obter o número do contrato a ser gravado no item
    contrato_num_str = _extract_contrato_num_from_row(row) or _extract_contrato_num_from_cab(cab)

    raw_kwargs = dict(
        cabecalho_id=cab.id,
        ativo=row["ativo"],
        serial=serial,
        cod_pro=row.get("cod_pro"),
        descricao_produto=row.get("descricao_produto") or row.get("descricao") or row.get("descricao_item"),
        cod_cli=row.get("cod_cli"),
        nome_cli=row.get("nome_cli"),
        valor_mensal=valor_mensal_val,
        data_envio=(override_data_envio or data_mov.date()),
        tp_transacao="ENVIO" if hasattr(Contrato, "tp_transacao") else None,
        status="ATIVO" if hasattr(Contrato, "status") else None,
        mov_hash=mov_hash if hasattr(Contrato, "mov_hash") else None,
        **derivados,
    )
    raw_kwargs = {k: v for k, v in raw_kwargs.items() if v is not None}

    if hasattr(Contrato, "data_troca") and data_troca is not None:
        raw_kwargs["data_troca"] = data_troca

    item_kwargs = _filter_model_kwargs(Contrato, raw_kwargs)
    item_kwargs = _ensure_min_fields(
        Contrato,
        item_kwargs,
        minimo={"cabecalho_id": cab.id, "ativo": row["ativo"], "data_envio": (override_data_envio or data_mov.date())},
    )

    item = Contrato(**item_kwargs)

    # Número do contrato gravado diretamente no item (quando o modelo tiver a coluna)
    _apply_contrato_num_on_item(item, contrato_num_str)

    # Período “de/até” (datas) copiado do cabeçalho, se os campos existirem no item
    ini, fim = _get_periodo_from_cab(cab)
    _set_periodo_on_item(item, ini, fim)

    sess.add(item)
    sess.flush()
    return item

def _retorno(sess: Session, row: Dict[str, Any], cab: ContratoCabecalho, data_mov: datetime, mov_hash: str) -> Contrato:
    aberto = _find_item_aberto(sess, cab.id, row.get("cod_cli"), row["ativo"])
    if not aberto:
        raise ValueError(f"RETORNO sem item ATIVO para '{row['ativo']}'.")
    if hasattr(aberto, "status"):
        aberto.status = "RETORNADO"
    if hasattr(aberto, "data_retorno"):
        aberto.data_retorno = data_mov.date()
    if hasattr(aberto, "tp_transacao"):
        aberto.tp_transacao = "RETORNO"
    if hasattr(aberto, "mov_hash"):
        aberto.mov_hash = mov_hash
    sess.add(aberto)
    sess.flush()
    return aberto


# ----------------------- leitura canônica row ----------------------

def _canon(row: Dict[str, Any]) -> Dict[str, Any]:
    contrato_num = row.get("contrato_num_norm") or _row_get_str(row, "contrato_num", "contraton", "contrato", "numero_contrato", "num_contrato", "n_contrato")
    cod_cli = row.get("cod_cli_norm") or _row_get_str(row, "cod_cli", "cliente", "cod_cliente")
    ativo = row.get("ativo_norm") or _row_get_str(row, "ativo", "patrimonio", "equipamento", "serial")
    data_raw = row.get("data_mov_iso") or _row_get_str(row, "data_mov", "data", "data_movimento")
    data_mov = parse_data_mov(data_raw)
    return {"contrato_num": contrato_num, "cod_cli": cod_cli, "ativo": ativo, "data_mov": data_mov}

def _fmt_exc(e: BaseException) -> str:
    msg = f"{e.__class__.__name__}: {e}"
    orig = getattr(e, "orig", None)
    if orig:
        try:
            msg += f" | DB: {orig}"
        except Exception:
            pass
    return msg


# ----------------------------- lote -------------------------------

def aplicar_lote(sess: Session, lote_id: int) -> Dict[str, Any]:
    lote: MovimentacaoLote | None = sess.get(MovimentacaoLote, lote_id)
    if not lote:
        raise ValueError(f"Lote {lote_id} não encontrado.")

    itens: List[MovimentacaoItem] = list(
        sess.scalars(
            select(MovimentacaoItem)
            .where(MovimentacaoItem.lote_id == lote_id)
            .order_by(MovimentacaoItem.linha_idx.asc())
        )
    )

    ok = 0
    erros = 0
    contratos_afetados: set[int] = set()

    # separar trocas (parear por OS) e unitários
    trocas: Dict[tuple[str, str, str], Dict[str, MovimentacaoItem]] = {}
    unitarios: List[MovimentacaoItem] = []

    for it in itens:
        row = it.payload or {}
        tp = _tp(row)
        if tp == "TROCA":
            subtipo = _tipo_troca(row)
            if subtipo not in ("ENVIO", "RETORNO"):
                it.erro_msg = "TROCA: 'Tipo de Movimento Troca' inválido (ENVIO/RETORNO)."
                erros += 1
                continue
            contrato_num = _row_get_str(row, "contrato_num_norm", "contrato_num", "contraton", "contrato")
            cod_cli = _row_get_str(row, "cod_cli_norm", "cod_cli", "cliente", "cod_cliente")
            oskey = _os_key(row)
            if not contrato_num or not oskey:
                it.erro_msg = "TROCA: faltam contrato/OS."
                erros += 1
                continue
            key = (contrato_num, cod_cli, oskey)
            trocas.setdefault(key, {})
            if subtipo in trocas[key]:
                it.erro_msg = f"TROCA: linha duplicada ({subtipo}) na OS {oskey}."
                erros += 1
                continue
            trocas[key][subtipo] = it
        else:
            unitarios.append(it)

    # 1) unitários (ENVIO / RETORNO)
    for it in unitarios:
        row = it.payload or {}
        try:
            with sess.begin_nested():
                tp = _tp(row)
                c = _canon(row)
                contrato_num, cod_cli, ativo, data_mov = c["contrato_num"], c["cod_cli"], c["ativo"], c["data_mov"]
                if not tp or not contrato_num or not ativo:
                    raise ValueError("Campos obrigatórios: tp_transacao, contrato_num, ativo (cod_cli opcional).")

                cab = _require_cabecalho(sess, row, contrato_num, cod_cli or None)
                data_iso = data_mov.date().isoformat()
                mov_hash = make_mov_hash(contrato_num, cod_cli, tp, ativo, data_iso, "")

                # idempotência por hash — mas reprocessa quando necessário
                dup = sess.execute(select(ContratoLog.id).where(ContratoLog.mov_hash == mov_hash)).first()
                if dup:
                    precisa_reaplicar = False
                    if tp == "ENVIO":
                        # Reaplica se NÃO existe item ATIVO para esse ativo (foi fechado)
                        precisa_reaplicar = (_find_item_aberto(sess, cab.id, cod_cli, ativo) is None)
                    elif tp == "RETORNO":
                        # Reaplica se AINDA existe item ATIVO para esse ativo
                        precisa_reaplicar = (_find_item_aberto(sess, cab.id, cod_cli, ativo) is not None)

                    if not precisa_reaplicar:
                        it.erro_msg = ""  # duplicado silencioso
                        ok += 1  # conta como ok/idempotente
                        continue
                    else:
                        # Em vez de apagar logs, adicionamos um novo log consistente (histórico preservado).
                        pass

                if tp == "ENVIO":
                    # SOFT CLOSE de quaisquer itens existentes com mesmo ativo
                    _soft_close_itens_por_ativo(sess, ativo, data_mov.date())
                    _envio(sess, {"cod_cli": cod_cli, "ativo": ativo, **row}, cab, data_mov, mov_hash)
                elif tp == "RETORNO":
                    _retorno(sess, {"cod_cli": cod_cli, "ativo": ativo, **row}, cab, data_mov, mov_hash)
                else:
                    raise ValueError(f"tp_transacao inválido: {tp}")

                sess.add(ContratoLog(
                    contrato_cabecalho_id=cab.id,
                    cod_cli=cod_cli,
                    ativo=ativo,
                    tp_transacao=tp,
                    data_mov=data_mov.date(),
                    mov_hash=mov_hash,
                    status="OK",
                    mensagem="",
                ))
                sess.flush()
                contratos_afetados.add(cab.id)
            ok += 1
        except (SQLAlchemyError, Exception) as e:
            it.erro_msg = _fmt_exc(e)
            erros += 1

    # 2) trocas (pareadas por OS)
    for (contrato_num, cod_cli, oskey), sides in trocas.items():
        envio_it = sides.get("ENVIO")
        retorno_it = sides.get("RETORNO")
        if not envio_it or not retorno_it:
            if not envio_it and retorno_it:
                retorno_it.erro_msg = f"TROCA OS {oskey}: falta ENVIO."
                erros += 1
            if not retorno_it and envio_it:
                envio_it.erro_msg = f"TROCA OS {oskey}: falta RETORNO."
                erros += 1
            continue
        try:
            with sess.begin_nested():
                envio_row = envio_it.payload or {}
                retorno_row = retorno_it.payload or {}

                ativo_envio = envio_row.get("ativo_novo_resolvido") or _row_get_str(envio_row, "ativo", "patrimonio", "equipamento", "serial", "ativo_norm")
                ativo_retorno = retorno_row.get("ativo_antigo_resolvido") or _row_get_str(retorno_row, "ativo", "patrimonio", "equipamento", "serial", "ativo_norm")
                if not ativo_envio or not ativo_retorno:
                    raise ValueError("TROCA: cada linha deve trazer o 'ativo' correspondente (ENVIO/RETORNO).")

                data_mov = parse_data_mov(
                    retorno_row.get("data_mov_iso")
                    or _row_get_str(retorno_row, "data_mov", "data", "data_movimento")
                    or envio_row.get("data_mov_iso")
                    or _row_get_str(envio_row, "data_mov", "data", "data_movimento")
                )

                # Cabeçalho por payload resolvido (quando houver), senão busca por número
                cab = _require_cabecalho(sess, envio_row, contrato_num, cod_cli or None)
                data_iso = data_mov.date().isoformat()

                # Captura a data_envio do item que vai retornar (antes de fechar), para herdar no ENVIO
                aberto_ret_pre = _find_item_aberto(sess, cab.id, (cod_cli or _row_get_str(retorno_row, "cod_cli", "cliente", "cod_cliente")), ativo_retorno)
                data_envio_herdada = getattr(aberto_ret_pre, "data_envio", None) if aberto_ret_pre is not None else None

                h_ret = make_mov_hash(contrato_num, cod_cli, "TROCA-RETORNO", ativo_retorno, data_iso, oskey)
                h_env = make_mov_hash(contrato_num, cod_cli, "TROCA-ENVIO", ativo_envio, data_iso, oskey)

                # RETORNO - reprocessa se ainda houver item ativo
                dup_ret = sess.execute(select(ContratoLog.id).where(ContratoLog.mov_hash == h_ret)).first()
                ainda_ativo_ret = _find_item_aberto(sess, cab.id, (cod_cli or _row_get_str(retorno_row, "cod_cli", "cliente", "cod_cliente")), ativo_retorno) is not None
                if not dup_ret or ainda_ativo_ret:
                    _retorno(sess, {"cod_cli": (cod_cli or _row_get_str(retorno_row, "cod_cli", "cliente", "cod_cliente")), "ativo": ativo_retorno, **retorno_row}, cab, data_mov, h_ret)
                    sess.add(ContratoLog(
                        contrato_cabecalho_id=cab.id,
                        cod_cli=cod_cli,
                        ativo=ativo_retorno,
                        tp_transacao="TROCA-RETORNO",
                        data_mov=data_mov.date(),
                        mov_hash=h_ret,
                        status="OK",
                        mensagem=f"OS {oskey}",
                    ))
                    sess.flush()

                # ENVIO - reprocessa se não houver item ativo (após retorno, em geral não haverá)
                dup_env = sess.execute(select(ContratoLog.id).where(ContratoLog.mov_hash == h_env)).first()
                ainda_ativo_env = _find_item_aberto(sess, cab.id, (cod_cli or _row_get_str(envio_row, "cod_cli", "cliente", "cod_cliente")), ativo_envio) is not None
                if not dup_env or not ainda_ativo_env:
                    # SOFT CLOSE para qualquer item existente com este ativo novo (segurança)
                    _soft_close_itens_por_ativo(sess, ativo_envio, data_mov.date())
                    _envio(
                        sess,
                        {"cod_cli": (cod_cli or _row_get_str(envio_row, "cod_cli", "cliente", "cod_cliente")), "ativo": ativo_envio, **envio_row},
                        cab,
                        data_mov,
                        h_env,
                        override_data_envio=data_envio_herdada,  # <- herda do que retornou
                        data_troca=data_mov.date(),            # <- marca data_troca
                    )
                    sess.add(ContratoLog(
                        contrato_cabecalho_id=cab.id,
                        cod_cli=cod_cli,
                        ativo=ativo_envio,
                        tp_transacao="TROCA-ENVIO",
                        data_mov=data_mov.date(),
                        mov_hash=h_env,
                        status="OK",
                        mensagem=f"OS {oskey}",
                    ))
                    sess.flush()

                contratos_afetados.add(cab.id)
                ok += 2
        except (SQLAlchemyError, Exception) as e:
            if envio_it:
                envio_it.erro_msg = _fmt_exc(e)
            if retorno_it:
                retorno_it.erro_msg = _fmt_exc(e)
            erros += 1

    lote.status = "PROCESSADO_COM_ERROS" if erros else "PROCESSADO"

    return {
        "processados": ok + erros,
        "ok": ok,
        "erros": erros,
        "contratos_afetados": sorted(contratos_afetados),
        "erros_detalhes": [
            {"linha_idx": it.linha_idx, "mensagem": (it.erro_msg or "")} for it in itens if getattr(it, "erro_msg", None)
        ],
    }
