# routers/importar_movimentacao.py
# Versão: 2.6.0
# Data: 02/09/2025
#
# MUDANÇAS NESTA VERSÃO:
# - [2.6.0] Validação pró-ativa no PREVIEW:
#   * Resolve e anexa cabecalho_id_resolvido no payload quando existir.
#   * Marca ERRO se o cabeçalho não for encontrado (mensagem compatível).
#   * Em RETORNO, verifica existência de item ativo compatível; caso contrário,
#     marca ERRO ("RETORNO sem item ATIVO para '<ativo>'.").
#   * Em TROCA com par completo, valida se o item antigo existe/está ativo antes
#     de aceitar; registra flag em payload.
# - Mantida a lógica 2.5.2 de metacampos (TROCA por OS, idempotência por hash)
#   e o fix pós-commit (_post_commit_fixup).
#
# HISTÓRICO (principal):
# - 2.5.2: Pós-commit "fixup" preenchendo campos faltantes no Contrato e
#   gravando runtime/ultima_importacao.json.
# - 2.5.0: TROCA em 2 linhas, metacampos canônicos, idempotência por hash,
#   preview/commit/lote.
# - 2.4.x: Robustez em nomes de colunas, pareamento por OS, etc.

from typing import List, Dict, Any, Optional, Tuple, DefaultDict
from collections import defaultdict
from fastapi import APIRouter, Depends, HTTPException, Body
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select, and_, or_
from database import get_db

from services.movimentacao_service import aplicar_lote
from utils.mov_utils import parse_data_mov, norm_tp, make_mov_hash
# ⬇️ acrescenta Contrato e ContratoCabecalho para o fix pós-commit e validações
from models import MovimentacaoLote, MovimentacaoItem, ContratoLog, Contrato, ContratoCabecalho

import os, json
import datetime  # para datetime.date
from datetime import datetime

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    TZ = ZoneInfo("America/Sao_Paulo")
except Exception:
    TZ = None  # fallback sem timezone

RUNTIME_DIR = "runtime"
ULTIMO_JSON = os.path.join(RUNTIME_DIR, "ultima_importacao.json")

router = APIRouter(prefix="/importar_movimentacao", tags=["Movimentação"])

SEVERIDADE_OK = "OK"
SEVERIDADE_AVISO = "AVISO"
SEVERIDADE_ERRO = "ERRO"

# --- Normalizadores auxiliares (aceitam variações de nomes de coluna) ---

_OS_KEYS = ["os", "n_os", "num_os", "numero_os", "ordem_servico", "os_num"]
_TROCA_ROLE_KEYS = [
    "tipo_movimento_troca",
    "tp_movimento_troca",
    "tipo_mov_troca",
    "mov_troca",
    "tipo_troca",
]
_CONTRATO_NUM_KEYS = ["contrato_num", "contrato_n", "contrato", "n_contrato", "numero_contrato", "numero"]
_COD_CLI_KEYS = ["cod_cli", "codigo_cliente", "cliente", "cod_cliente"]
_ATIVO_KEYS = ["ativo", "serial", "numero_serie", "n_serie"]

def _get_str(x: Any) -> str:
    return "" if x is None else str(x).strip()

def _pick(row: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return _get_str(v)
    return ""

def _get_os(row: Dict[str, Any]) -> str:
    return _pick(row, _OS_KEYS)

def _get_troca_role(row: Dict[str, Any]) -> str:
    """Mapeia valor da coluna de papel de TROCA para ENVIO/RETORNO."""
    raw = _pick(row, _TROCA_ROLE_KEYS).upper()
    if not raw:
        return ""
    if raw in ("ENVIO", "E", "NOVO", "NOVA"):
        return "ENVIO"
    if raw in ("RETORNO", "R", "DEVOLUCAO", "DEVOLUÇÃO", "ANTIGO", "OLD"):
        return "RETORNO"
    return ""  # inválido

def _get_contrato_num(row: Dict[str, Any]) -> str:
    return _pick(row, _CONTRATO_NUM_KEYS)

def _get_cod_cli(row: Dict[str, Any]) -> str:
    return _pick(row, _COD_CLI_KEYS)

def _get_ativo(row: Dict[str, Any]) -> str:
    return _pick(row, _ATIVO_KEYS)

def _troca_pair_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    """Chave de pareamento da troca: (contrato_num, cod_cli, os)."""
    contrato_num = _get_contrato_num(row)
    cod_cli = _get_cod_cli(row)
    os_num = _get_os(row)
    return (contrato_num, cod_cli, os_num)

# -------------------- RESOLUÇÃO DE CABEÇALHO E CONTRATO ATIVO --------------------

def _pick_attr(obj_or_cls, *names):
    """Retorna o primeiro nome de atributo existente, senão None."""
    for n in names:
        if hasattr(obj_or_cls, n):
            return n
    return None

def _resolver_cabecalho_id(db, contrato_num: str, cod_cli: str) -> Optional[int]:
    """
    Tenta localizar o ContratoCabecalho por número (e cod_cli, se existir no modelo).
    Retorna ID se encontrar, senão None.
    """
    if not contrato_num:
        return None

    # Tenta diferentes nomes de campo para número do contrato
    num_field = _pick_attr(ContratoCabecalho, "contrato_num", "contrato_n", "numero", "numero_contrato")
    if not num_field:
        return None

    q = db.query(ContratoCabecalho).filter(getattr(ContratoCabecalho, num_field) == contrato_num)

    # Se existir cod_cli no cabeçalho, filtra também
    if _pick_attr(ContratoCabecalho, "cod_cli"):
        q = q.filter(getattr(ContratoCabecalho, "cod_cli") == cod_cli)

    cab = q.first()
    return getattr(cab, "id", None) if cab else None

def _existe_contrato_ativo(db, ativo: str, cabecalho_id: Optional[int]) -> bool:
    """
    Verifica a existência de um item de Contrato "ativo" para RETORNO/TROCA.
    Tenta usar 'status'='ATIVO' se o campo existir; fallback para 'data_retorno is null';
    fallback final: existência de qualquer contrato com o mesmo ativo (menos preciso).
    """
    if not ativo:
        return False

    q = db.query(Contrato).filter(getattr(Contrato, "ativo") == ativo)

    if cabecalho_id and _pick_attr(Contrato, "cabecalho_id", "contrato_cabecalho_id"):
        q = q.filter(or_(
            getattr(Contrato, "cabecalho_id", None) == cabecalho_id if hasattr(Contrato, "cabecalho_id") else False,
            getattr(Contrato, "contrato_cabecalho_id", None) == cabecalho_id if hasattr(Contrato, "contrato_cabecalho_id") else False
        ))

    status_attr = _pick_attr(Contrato, "status")
    data_retorno_attr = _pick_attr(Contrato, "data_retorno")

    if status_attr:
        q = q.filter(getattr(Contrato, "status") == "ATIVO")
    elif data_retorno_attr:
        q = q.filter(getattr(Contrato, "data_retorno") == None)  # noqa: E711

    return db.query(q.exists()).scalar() or False

# -------------------------------------------------------------------------------

@router.get("/version")
def version():
    return {"router": "importar_movimentacao", "version": "2.6.0", "date": "2025-09-02"}

def _prepair_trocas(linhas: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str], Dict[str, Dict[str, Any]]]:
    """
    Varre as linhas e, para as de TROCA, agrupa por (contrato_num, cod_cli, OS).
    Retorna: pair_key -> {"ENVIO": row_envio?, "RETORNO": row_retorno?}
    """
    grupos: DefaultDict[Tuple[str, str, str], Dict[str, Dict[str, Any]]] = defaultdict(dict)
    for row in linhas:
        tp = norm_tp(row.get("tp_transacao"))
        if tp != "TROCA":
            continue
        papel = _get_troca_role(row)
        if not papel:
            # ficará para validação apontar erro
            continue
        grupos[_troca_pair_key(row)][papel] = row
    return grupos

def _validar_linha_preview(
    db,
    row: Dict[str, Any],
    troca_par: Optional[Dict[str, Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Valida uma linha e retorna metadados:
      { severidade, erros[], avisos[], mov_hash, ...extras }
    Regras:
      - tp_transacao ∈ {ENVIO, RETORNO, TROCA}
      - contrato_num, cod_cli obrigatórios
      - data_mov aceita formatos diversos/serial Excel
      - ativo obrigatório exceto em TROCA (usaremos o par)
      - TROCA: exige OS e papel válido (ENVIO/RETORNO); quando par completo, resolve ativos e
               gera hash único para o evento; senão, gera hash provisório.
      - [2.6.0] Resolve cabecalho_id_resolvido; marca ERRO se não houver cabeçalho.
      - [2.6.0] Em RETORNO, exige existência de item ativo compatível.
    """
    erros: List[str] = []
    avisos: List[str] = []

    tp_norm = norm_tp(row.get("tp_transacao"))
    if tp_norm not in ("ENVIO", "RETORNO", "TROCA"):
        erros.append("tp_transacao inválido (use ENVIO/RETORNO/TROCA).")

    try:
        data_mov_dt = parse_data_mov(row.get("data_mov"))
        data_mov_iso = data_mov_dt.date().isoformat() if data_mov_dt else ""
    except Exception as e:
        erros.append(str(e))
        data_mov_dt = None
        data_mov_iso = ""

    contrato_num = _get_contrato_num(row)
    cod_cli = _get_cod_cli(row)
    ativo = _get_ativo(row)
    os_num = _get_os(row)

    if not contrato_num:
        erros.append("contrato_num (ou contrato_n) obrigatório.")
    if not cod_cli:
        erros.append("cod_cli (ou código do cliente) obrigatório.")

    # [2.6.0] Resolve cabeçalho desde já
    cabecalho_id_resolvido: Optional[int] = None
    if contrato_num and cod_cli:
        try:
            cabecalho_id_resolvido = _resolver_cabecalho_id(db, contrato_num, cod_cli)
        except Exception:
            cabecalho_id_resolvido = None

    if cabecalho_id_resolvido is None:
        # Mantém mensagem compatível com o que o serviço retornava, para UX igual
        erros.append(f"Cabeçalho não encontrado para contrato='{contrato_num}'. (cod_cli '{cod_cli}' ignorado)")

    mov_hash: Optional[str] = None
    extras: Dict[str, Any] = {
        "tp_norm": tp_norm,
        "contrato_num_norm": contrato_num,
        "cod_cli_norm": cod_cli,
        "os_norm": os_num,
        "ativo_norm": ativo,
        "data_mov_iso": data_mov_iso,
        # [2.6.0] novo metadado
        "cabecalho_id_resolvido": cabecalho_id_resolvido,
    }

    # ---------------------- TROCA ----------------------
    if tp_norm == "TROCA":
        papel = _get_troca_role(row)

        if not os_num:
            erros.append("TROCA requer número de OS (os/numero_os/num_os/ordem_servico...).")
        if not papel:
            erros.append("TROCA requer 'Tipo de Movimento Troca' = ENVIO ou RETORNO.")

        # Para TROCA não exigimos 'ativo' nesta linha (virá do par)
        ativo_antigo, ativo_novo = "", ""
        par_completo = False

        if troca_par:
            tem_env = "ENVIO" in troca_par
            tem_ret = "RETORNO" in troca_par
            if tem_env and tem_ret:
                par_completo = True
                linha_env = troca_par["ENVIO"]
                linha_ret = troca_par["RETORNO"]
                ativo_novo = _get_ativo(linha_env)
                ativo_antigo = _get_ativo(linha_ret)

                if not ativo_novo:
                    erros.append("TROCA: linha ENVIO do par não possui 'ativo/serial' (novo).")
                if not ativo_antigo:
                    erros.append("TROCA: linha RETORNO do par não possui 'ativo/serial' (antigo).")

                # [2.6.0] se cabeçalho resolvido e temos ativo_antigo, valide existência do contrato "ativo"
                if cabecalho_id_resolvido and ativo_antigo:
                    if not _existe_contrato_ativo(db, ativo_antigo, cabecalho_id_resolvido):
                        erros.append(f"TROCA: item antigo '{ativo_antigo}' não está ATIVO para este cabeçalho.")

                extras.update({
                    "troca_pair_status": "PAR_COMPLETO",
                    "troca_chave": f"{contrato_num}|{cod_cli}|{os_num}",
                    "ativo_antigo_resolvido": ativo_antigo,
                    "ativo_novo_resolvido": ativo_novo,
                    "papel_troca": papel,
                })
            else:
                extras.update({
                    "troca_pair_status": "AGUARDANDO_PAREAMENTO",
                    "papel_troca": papel,
                })
                avisos.append(
                    f"TROCA: par incompleto para OS={os_num} "
                    f"(falta {'RETORNO' if papel=='ENVIO' else 'ENVIO'})."
                )

        # Hash:
        if not erros and data_mov_dt:
            if par_completo and ativo_antigo and ativo_novo:
                mov_hash = make_mov_hash(
                    contrato_num=contrato_num,
                    cod_cli=cod_cli,
                    tp="TROCA",
                    ativo=ativo_antigo,  # ancora no antigo
                    data_mov_iso=data_mov_dt.date().isoformat(),
                    ativo_novo=ativo_novo,
                )
            else:
                # provisório por linha para não colidir
                mov_hash = make_mov_hash(
                    contrato_num=contrato_num,
                    cod_cli=cod_cli,
                    tp=f"TROCA-{papel or 'INDEFINIDO'}",
                    ativo=ativo or os_num,
                    data_mov_iso=data_mov_dt.date().isoformat(),
                )

        # Idempotência apenas quando par completo
        if mov_hash and extras.get("troca_pair_status") == "PAR_COMPLETO":
            dup = db.execute(select(ContratoLog.id).where(ContratoLog.mov_hash == mov_hash)).first()
            if dup:
                avisos.append("Duplicado (hash da TROCA) — será IGNORADO no commit.")

    # ----------------- ENVIO / RETORNO -----------------
    else:
        if not ativo:
            erros.append("ativo/serial obrigatório.")
        if not erros and data_mov_dt:
            mov_hash = make_mov_hash(
                contrato_num=contrato_num,
                cod_cli=cod_cli,
                tp=tp_norm,
                ativo=ativo,
                data_mov_iso=data_mov_dt.date().isoformat(),
            )
            dup = db.execute(select(ContratoLog.id).where(ContratoLog.mov_hash == mov_hash)).first()
            if dup:
                avisos.append("Duplicado (hash) — esta linha será IGNORADA no commit.")

        # [2.6.0] Em RETORNO, exija existência de item ativo:
        if tp_norm == "RETORNO" and not erros and cabecalho_id_resolvido:
            if not _existe_contrato_ativo(db, ativo, cabecalho_id_resolvido):
                erros.append(f"RETORNO sem item ATIVO para '{ativo}'.")

    severidade = SEVERIDADE_ERRO if erros else (SEVERIDADE_AVISO if avisos else SEVERIDADE_OK)
    return {"severidade": severidade, "erros": erros, "avisos": avisos, "mov_hash": mov_hash, **extras}

@router.post("/preview")
def preview_lote(
    linhas: List[Dict[str, Any]] = Body(..., embed=True, description="Linhas de movimentação (CSV já mapeado)"),
    db=Depends(get_db),
):
    """
    Cria um lote de pré-importação com validação por linha.
    Para TROCA (duas linhas por OS): pareia por OS e inclui no payload o status do par
    e os ativos resolvidos (antigo/novo) quando possível.
    [2.6.0] Inclui cabecalho_id_resolvido quando encontrado e validações adicionais.
    """
    if not linhas:
        raise HTTPException(status_code=400, detail="Nenhuma linha recebida.")

    # Pré-agrupamento de trocas por OS
    troca_groups = _prepair_trocas(linhas)

    try:
        with db.begin():
            lote = MovimentacaoLote(status="PREVIEW")
            db.add(lote)
            db.flush()  # garante lote.id

            total_erros = 0
            total_avisos = 0

            for idx, row in enumerate(linhas, start=1):
                tp_norm_val = norm_tp(row.get("tp_transacao"))
                pair = troca_groups.get(_troca_pair_key(row)) if tp_norm_val == "TROCA" else None

                meta = _validar_linha_preview(db, row, troca_par=pair)

                # payload base = linha original + metacampos
                payload = dict(row)
                payload.update({
                    "severidade": meta.get("severidade"),
                    "erros": meta.get("erros") or [],
                    "avisos": meta.get("avisos") or [],
                    "mov_hash": meta.get("mov_hash"),
                    # canônicos p/ serviço:
                    "tp_norm": meta.get("tp_norm"),
                    "contrato_num_norm": meta.get("contrato_num_norm"),
                    "cod_cli_norm": meta.get("cod_cli_norm"),
                    "os_norm": meta.get("os_norm"),
                    "ativo_norm": meta.get("ativo_norm"),
                    "data_mov_iso": meta.get("data_mov_iso"),
                    # [2.6.0]
                    "cabecalho_id_resolvido": meta.get("cabecalho_id_resolvido"),
                })
                # extras do meta (troca)
                for k in (
                    "troca_pair_status",
                    "troca_chave",
                    "ativo_antigo_resolvido",
                    "ativo_novo_resolvido",
                    "papel_troca",
                ):
                    if k in meta:
                        payload[k] = meta[k]

                item = MovimentacaoItem(
                    lote_id=lote.id,
                    linha_idx=idx,
                    payload=payload,
                    erro_msg="; ".join(meta.get("erros") or meta.get("avisos") or []),
                )
                db.add(item)

                if meta["severidade"] == SEVERIDADE_ERRO:
                    total_erros += 1
                elif meta["severidade"] == SEVERIDADE_AVISO:
                    total_avisos += 1

            resumo = {
                "linhas": len(linhas),
                "erros": total_erros,
                "avisos": total_avisos,
                "ok": len(linhas) - total_erros - total_avisos,
            }
            return {"lote_id": lote.id, "status": "PREVIEW", "resumo": resumo}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Erro de banco: {e.__class__.__name__}")

# ----------------- helpers fix pós-commit (mantidos de 2.5.2) -------------------

def _parse_money(val):
    """Converte '1.234,56' ou '1234.56' → float. Retorna None se não parsear."""
    try:
        if val is None or val == "":
            return None
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip().replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return None

def _db_get(db, Model, pk):
    """Compatibilidade para pegar por PK no SQLAlchemy."""
    try:
        return db.get(Model, pk)  # SA 1.4+
    except Exception:
        # modo legacy
        return db.query(Model).get(pk)

def _pick_attr(obj, *names):
    """Retorna o primeiro nome de atributo existente em obj, senão None."""
    for n in names:
        if hasattr(obj, n):
            return n
    return None

def _post_commit_fixup(db, lote_id: int) -> dict:
    """
    Após aplicar_lote, preenche campos faltantes nos itens de contrato:
     - numero do contrato (contrato_n/contrato_num/numero/numero_contrato)
     - cod_cli, data_envio, valor_mensal
     - periodo_contratual a partir do ContratoCabecalho
    Mapeamento é feito via ContratoLog.mov_hash (ligado aos itens do lote).
    """
    itens = db.query(MovimentacaoItem).filter(MovimentacaoItem.lote_id == lote_id).all()
    if not itens:
        return {"contratos_atualizados": 0}

    by_hash = {}
    for it in itens:
        payload = (it.payload or {}) if hasattr(it, "payload") else {}
        mh = payload.get("mov_hash")
        if not mh:
            continue
        by_hash[mh] = {
            "contrato_num": payload.get("contrato_num_norm") or payload.get("contrato_num"),
            "cod_cli": payload.get("cod_cli_norm") or payload.get("cod_cli"),
            "data_mov": payload.get("data_mov_iso"),
            "valor_mensal": _parse_money(payload.get("valor_mensal")),
        }

    if not by_hash:
        return {"contratos_atualizados": 0}

    logs = db.query(ContratoLog).filter(ContratoLog.mov_hash.in_(list(by_hash.keys()))).all()

    upd = 0
    set_num = set_cli = set_periodo = set_valor = set_data = 0

    for lg in logs:
        contrato_id = getattr(lg, "contrato_id", None)
        if not contrato_id:
            continue

        c = _db_get(db, Contrato, contrato_id)
        if not c:
            continue

        meta = by_hash.get(getattr(lg, "mov_hash", None), {})
        num = (meta.get("contrato_num") or "") and str(meta.get("contrato_num"))
        cli = (meta.get("cod_cli") or "") and str(meta.get("cod_cli"))
        val = meta.get("valor_mensal")
        dt_iso = meta.get("data_mov")

        # número do contrato (pega o primeiro atributo existente)
        num_attr = _pick_attr(c, "contrato_n", "contrato_num", "numero", "numero_contrato")
        if num_attr and num and not getattr(c, num_attr, None):
            setattr(c, num_attr, num)
            set_num += 1

        # cod_cli
        if hasattr(c, "cod_cli") and cli and not getattr(c, "cod_cli", None):
            c.cod_cli = cli
            set_cli += 1

        # valor_mensal
        if hasattr(c, "valor_mensal") and val is not None:
            atual = getattr(c, "valor_mensal", None)
            try:
                atual_f = float(atual or 0)
            except Exception:
                atual_f = 0.0
            if atual is None or atual_f == 0.0:
                c.valor_mensal = val
                set_valor += 1

        # data_envio
        if hasattr(c, "data_envio") and dt_iso and not getattr(c, "data_envio", None):
            try:
                y, m, d = map(int, dt_iso.split("-"))
                c.data_envio = datetime.date(y, m, d)
                set_data += 1
            except Exception:
                pass

        # periodo_contratual via cabeçalho
        periodo = None
        cab = None
        cab_id = getattr(lg, "contrato_cabecalho_id", None)
        if cab_id:
            cab = _db_get(db, ContratoCabecalho, cab_id)
        if not cab and num:
            # tenta por número
            cab_num_attr = _pick_attr(ContratoCabecalho, "contrato_num", "contrato_n", "numero")
            if cab_num_attr:
                cab = db.query(ContratoCabecalho).filter(getattr(ContratoCabecalho, cab_num_attr) == num).first()
        if cab:
            periodo = getattr(cab, "prazo_contratual", None) or getattr(cab, "periodo_contratual", None)

        if hasattr(c, "periodo_contratual") and periodo and not getattr(c, "periodo_contratual", None):
            try:
                c.periodo_contratual = int(periodo)
                set_periodo += 1
            except Exception:
                pass

        upd += 1

    db.commit()
    return {
        "contratos_atualizados": upd,
        "set_num": set_num,
        "set_cli": set_cli,
        "set_periodo": set_periodo,
        "set_valor": set_valor,
        "set_data": set_data,
    }

# ----------------- /helpers fix pós-commit -----------------------------------

@router.post("/commit/{lote_id}")
def commit_lote(lote_id: int, db=Depends(get_db)):
    """
    Aplica o lote com transação atômica.
    services.movimentacao_service.aplicar_lote deve:
      - TROCA: usar payload (ativo_antigo_resolvido, ativo_novo_resolvido, troca_pair_status, troca_chave);
               somente consolidar quando PAR_COMPLETO (hash único).
      - Idempotência por mov_hash (linhas duplicadas ⇒ ignoradas).
    Ao final: grava runtime/ultima_importacao.json com resumo + 'fixup'.
    """
    try:
        with db.begin():
            resultado = aplicar_lote(db, lote_id)
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Erro ao aplicar lote: {e.__class__.__name__}")

    # Fix pós-commit (fora do with begin, com commit próprio)
    try:
        fix = _post_commit_fixup(db, lote_id)
    except Exception:
        fix = {"erro_fixup": True}

    # Fora da transação: gravar “Última importação”
    try:
        os.makedirs(RUNTIME_DIR, exist_ok=True)
        ts = datetime.now(TZ).isoformat() if TZ else datetime.utcnow().isoformat() + "Z"
        payload = {
            "lote_id": lote_id,
            "timestamp": ts,
            "ok": int(resultado.get("ok", 0)),
            "erros": int(resultado.get("erros", 0)),
            "contratos_afetados": list(resultado.get("contratos_afetados", [])),
            # quando o serviço retornar estes campos, eles entram aqui:
            "linhas_total": int(resultado.get("linhas_total", 0)),
            "inseridos": int(resultado.get("inseridos", 0)),
            "atualizados": int(resultado.get("atualizados", 0)),
            "trocas": int(resultado.get("trocas", 0)),
            "retornos": int(resultado.get("retornos", 0)),
            "router_version": "2.6.0",
            "fixup": fix,
        }
        with open(ULTIMO_JSON, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        # Não falhar o commit por erro ao gravar o arquivo de resumo
        pass

    return {"lote_id": lote_id, **resultado, "fixup": fix}

@router.get("/lote/{lote_id}")
def obter_lote(lote_id: int, db=Depends(get_db)):
    """
    Recupera itens do lote para exibição na UI de preview/mapeamento.
    status/msg são derivados de erro_msg e do payload.severidade (sem colunas físicas).
    """
    try:
        itens = (
            db.execute(
                select(MovimentacaoItem)
                .where(MovimentacaoItem.lote_id == lote_id)
                .order_by(MovimentacaoItem.linha_idx.asc())
            )
            .scalars()
            .all()
        )

        def _status_msg(it) -> Dict[str, str]:
            payload = it.payload or {}
            erro_msg = (it.erro_msg or "").strip()
            if erro_msg:
                return {"status": SEVERIDADE_ERRO, "msg": erro_msg}
            sev = payload.get("severidade") or SEVERIDADE_OK
            if sev == SEVERIDADE_AVISO:
                return {"status": SEVERIDADE_AVISO, "msg": "; ".join(payload.get("avisos", []))}
            return {"status": SEVERIDADE_OK, "msg": ""}

        def _simplify(it):
            sm = _status_msg(it)
            return {
                "linha_idx": it.linha_idx,
                "status": sm["status"],
                "msg": sm["msg"],
                "payload": it.payload,
            }

        return {"lote_id": lote_id, "itens": [_simplify(i) for i in itens]}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Erro de banco: {e.__class__.__name__}")

# Fim do arquivo - v2.6.0
