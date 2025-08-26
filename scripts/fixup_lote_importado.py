# scripts/fixup_lote_importado.py
# Versão: 1.2 (2025-08-26)
# Uso:
#   python scripts/fixup_lote_importado.py --lote 38 [--dry-run] [--verbose] [--incluir-retorno-troca]
#
# O que faz:
#  - Para o lote informado, cruza MovimentacaoItem -> Contrato via múltiplas estratégias:
#      (A) mov_hash -> ContratoLog -> Contrato
#      (B) contrato_num + ativo
#      (C) contrato_num
#      (D) ativo (match único)
#  - Preenche se faltando: numero do contrato, cod_cli (nome flexível), periodo_contratual,
#    e (apenas para ENVIO/TROCA-ENVIO) valor_mensal e data_envio (nomes flexíveis).
#  - v1.2: normaliza ativo em notação científica do Excel (ex. "3,50176E+14").
#  - v1.2: opção para incluir RETORNO/TROCA no preenchimento de num/cli/período.
#  - Gera resumo JSON em runtime/fixup_lote_<id>_<timestamp>.json

import os
import sys
import json
import argparse
import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Tuple, List

# --- Ajuste do sys.path para achar seus módulos do app ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# --- Imports do seu app (ajuste se necessário) ---
from database import SessionLocal
import models as M  # modelos

RUNTIME_DIR = os.path.join(BASE_DIR, "runtime")

# --------- Helpers de introspecção ---------
def get_model(name: str):
    return getattr(M, name, None)

MovimentacaoItem = get_model("MovimentacaoItem")
ContratoLog      = get_model("ContratoLog") or get_model("LogMovimentacao") or get_model("LogsMovimentacao")
Contrato         = get_model("Contrato") or get_model("Contratos") or get_model("ContratoItem")
ContratoCab      = get_model("ContratoCabecalho") or get_model("ContratosCabecalho") or get_model("ContratoHeader")

def ensure_models():
    missing = []
    if MovimentacaoItem is None:
        missing.append("MovimentacaoItem")
    if Contrato is None:
        missing.append("Contrato (ou equivalente)")
    if ContratoCab is None:
        missing.append("ContratoCabecalho (ou equivalente)")
    if missing:
        raise RuntimeError(f"Modelos ausentes: {', '.join(missing)}. Ajuste os nomes no script.")

def pick_attr(obj, *candidates) -> Optional[str]:
    """Retorna o primeiro atributo existente no objeto/classe."""
    for c in candidates:
        if hasattr(obj, c):
            return c
    return None

# nomes flexíveis por campo
NUM_CONTRATO_ATTRS = ("contrato_n", "contrato_num", "numero", "numero_contrato")
COD_CLI_ATTRS      = ("cod_cli", "cod_cliente", "cliente_codigo", "codigo_cliente")
VALOR_MENSAL_ATTRS = ("valor_mensal", "valor", "mensalidade")
DATA_ENVIO_ATTRS   = ("data_envio", "dt_envio", "data_inicio", "inicio")
ATIVO_ATTRS        = ("ativo", "codigo_ativo", "id_ativo")

# --------- Normalizações ---------
def parse_money(val: Any) -> Optional[float]:
    try:
        if val is None or val == "":
            return None
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip().replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return None

def to_date_iso(s: Optional[str]) -> Optional[datetime.date]:
    if not s:
        return None
    try:
        y, m, d = map(int, s.split("-"))
        return datetime.date(y, m, d)
    except Exception:
        return None

def normalize_ativo(raw: Optional[str]) -> Optional[str]:
    """Normaliza ativos, inclusive valores vindos como notação científica do Excel."""
    if raw is None:
        return None
    s = str(raw).strip()

    # já está 'limpo'?
    if "E" not in s.upper():
        # mantém zeros à esquerda; tira apenas espaços
        return s

    # troca vírgula por ponto para Decimal
    s2 = s.replace(",", ".")
    try:
        # tenta converter e formatar sem expoente
        d = Decimal(s2)
        # remove casas decimais; ativo deve ser código inteiro (string)
        # quantize para inteiro (sem arredondar indevido)
        as_int_str = format(d.quantize(Decimal("1")), "f")
        # remove sinais ou espaços
        as_int_str = as_int_str.replace("+", "").replace("-", "").strip()
        if as_int_str:
            return as_int_str
    except InvalidOperation:
        pass

    # fallback: heurística — pega dígitos antes e depois da vírgula/ponto
    # ex.: "3,50176E+14" -> "350176"
    digits = []
    for ch in s:
        if ch.isdigit():
            digits.append(ch)
        elif ch in (",", "."):
            # separador decimal — ignorado, só cola
            continue
        elif ch.upper() == "E":
            break
    candidate = "".join(digits)
    if candidate:
        # evita cadeias insanas (ex. 14 dígitos de expoente grudados)
        # mantém até 8-9 dígitos, que é o que vemos em ativos reais
        return candidate[:9]
    return None

def safe_get_payload_field(payload: dict, *names) -> Optional[Any]:
    for n in names:
        if n in payload and payload[n] not in (None, ""):
            return payload[n]
    return None

# --------- Resolvedores ---------
def find_contrato_via_log(db, mov_hash: str) -> Optional[Any]:
    if not ContratoLog:
        return None
    log = db.query(ContratoLog).filter(getattr(ContratoLog, "mov_hash") == mov_hash).first()
    if not log:
        return None
    contrato_id = getattr(log, "contrato_id", None)
    if not contrato_id:
        return None
    try:
        c = db.get(Contrato, contrato_id)
    except Exception:
        c = db.query(Contrato).get(contrato_id)
    return c

def find_contrato_via_num_ativo(db, num: str, ativo: str) -> Optional[Any]:
    num_attr = pick_attr(Contrato, *NUM_CONTRATO_ATTRS)
    ativo_attr = pick_attr(Contrato, *ATIVO_ATTRS)
    if not num_attr or not ativo_attr:
        return None
    results = db.query(Contrato).filter(
        getattr(Contrato, num_attr) == num,
        getattr(Contrato, ativo_attr) == ativo
    ).all()
    if len(results) == 1:
        return results[0]
    return None

def find_contrato_via_num(db, num: str) -> Optional[Any]:
    num_attr = pick_attr(Contrato, *NUM_CONTRATO_ATTRS)
    if not num_attr:
        return None
    results = db.query(Contrato).filter(getattr(Contrato, num_attr) == num).all()
    if len(results) == 1:
        return results[0]
    return None

def find_contrato_via_ativo(db, ativo: str) -> Optional[Any]:
    ativo_attr = pick_attr(Contrato, *ATIVO_ATTRS)
    if not ativo_attr:
        return None
    results = db.query(Contrato).filter(getattr(Contrato, ativo_attr) == ativo).all()
    if len(results) == 1:
        return results[0]
    return None

def find_cabecalho_por_num(db, num: str) -> Optional[Any]:
    if not ContratoCab:
        return None
    cab_num_attr = pick_attr(ContratoCab, *NUM_CONTRATO_ATTRS)
    if not cab_num_attr:
        return None
    return db.query(ContratoCab).filter(getattr(ContratoCab, cab_num_attr) == num).first()

# --------- Atualização de um contrato com dados do payload ---------
def apply_enrichment(contrato, cab, payload, permitir_valor_data: bool) -> Dict[str, int]:
    """Aplica preenchimentos se faltando. Retorna contadores alterados."""
    changed = {"num": 0, "cli": 0, "periodo": 0, "valor": 0, "data": 0}

    # numero do contrato
    num_payload = safe_get_payload_field(payload, "contrato_num_norm", "contrato_num")
    num_attr = pick_attr(contrato, *NUM_CONTRATO_ATTRS)
    if num_attr and num_payload and not getattr(contrato, num_attr, None):
        setattr(contrato, num_attr, str(num_payload))
        changed["num"] += 1

    # cod_cli (nomes flexíveis)
    cli_payload = safe_get_payload_field(payload, "cod_cli_norm", "cod_cli")
    cli_attr = pick_attr(contrato, *COD_CLI_ATTRS)
    if cli_attr and cli_payload and not getattr(contrato, cli_attr, None):
        setattr(contrato, cli_attr, str(cli_payload))
        changed["cli"] += 1

    # valor_mensal (só quando permitido)
    if permitir_valor_data:
        val_attr = pick_attr(contrato, *VALOR_MENSAL_ATTRS)
        if val_attr:
            val_payload = parse_money(safe_get_payload_field(payload, "valor_mensal", "valor"))
            atual = getattr(contrato, val_attr, None)
            atual_f = 0.0
            try:
                if atual is not None:
                    atual_f = float(atual)
            except Exception:
                atual_f = 0.0
            if val_payload is not None and (atual is None or atual_f == 0.0):
                setattr(contrato, val_attr, val_payload)
                changed["valor"] += 1

        # data_envio (nomes flexíveis)
        data_attr = pick_attr(contrato, *DATA_ENVIO_ATTRS)
        if data_attr:
            dt = to_date_iso(safe_get_payload_field(payload, "data_mov_iso"))
            if dt and not getattr(contrato, data_attr, None):
                setattr(contrato, data_attr, dt)
                changed["data"] += 1

    # periodo_contratual (via cabeçalho)
    if hasattr(contrato, "periodo_contratual"):
        per = None
        if cab:
            per = getattr(cab, "prazo_contratual", None) or getattr(cab, "periodo_contratual", None)
        if per and not getattr(contrato, "periodo_contratual", None):
            try:
                contrato.periodo_contratual = int(per)
                changed["periodo"] += 1
            except Exception:
                pass

    return changed

# --------- Executor principal ---------
def fixup_lote(db, lote_id: int, incluir_retorno_troca: bool, verbose: bool = False) -> Dict[str, Any]:
    ensure_models()

    itens = db.query(MovimentacaoItem).filter(getattr(MovimentacaoItem, "lote_id") == lote_id).all()
    if not itens:
        return {"lote_id": lote_id, "contratos_atualizados": 0, "msg": "Nenhum item no lote."}

    total = 0
    updated = 0
    set_num = set_cli = set_periodo = set_valor = set_data = 0
    diagnostics: List[Dict[str, Any]] = []

    for it in itens:
        total += 1
        payload = getattr(it, "payload", {}) or {}
        mov_hash = payload.get("mov_hash")
        tp = (payload.get("tp_norm") or payload.get("tp_transacao") or "").upper()

        # Tipos que podem preencher valor/data
        permitir_valor_data = tp in ("ENVIO", "TROCA-ENVIO")
        # Incluir num/cli/período também para RETORNO/TROCA se a flag estiver ligada
        if tp not in ("ENVIO", "TROCA-ENVIO", "RETORNO", "TROCA") and tp != "":
            # tipos estranhos — registra e segue
            diagnostics.append({"linha_idx": getattr(it, "linha_idx", None),
                                "motivo": "ignorado_por_tipo_desconhecido", "tp": tp})
            continue
        if tp in ("RETORNO", "TROCA") and not incluir_retorno_troca:
            diagnostics.append({"linha_idx": getattr(it, "linha_idx", None),
                                "motivo": "ignorado_por_tipo", "tp": tp})
            continue

        # Dados do payload
        num_raw = safe_get_payload_field(payload, "contrato_num_norm", "contrato_num")
        ativo_raw = safe_get_payload_field(payload, "ativo_norm", "ativo")
        ativo = normalize_ativo(ativo_raw) if ativo_raw else None
        num = str(num_raw) if num_raw is not None else None

        contrato_ref = None

        # (A) via log
        if mov_hash and ContratoLog:
            contrato_ref = find_contrato_via_log(db, mov_hash)
            if contrato_ref and verbose:
                diagnostics.append({"linha_idx": getattr(it, "linha_idx", None),
                                    "resolucao": "via_log",
                                    "contrato_id": getattr(contrato_ref, "id", None)})

        # (B) via numero + ativo
        if not contrato_ref and num and ativo:
            contrato_ref = find_contrato_via_num_ativo(db, num, ativo)
            if contrato_ref and verbose:
                diagnostics.append({"linha_idx": getattr(it, "linha_idx", None),
                                    "resolucao": "via_num_ativo",
                                    "contrato_id": getattr(contrato_ref, "id", None)})

        # (C) via numero
        if not contrato_ref and num:
            contrato_ref = find_contrato_via_num(db, num)
            if contrato_ref and verbose:
                diagnostics.append({"linha_idx": getattr(it, "linha_idx", None),
                                    "resolucao": "via_num",
                                    "contrato_id": getattr(contrato_ref, "id", None)})

        # (D) via ativo
        if not contrato_ref and ativo:
            contrato_ref = find_contrato_via_ativo(db, ativo)
            if contrato_ref and verbose:
                diagnostics.append({"linha_idx": getattr(it, "linha_idx", None),
                                    "resolucao": "via_ativo",
                                    "contrato_id": getattr(contrato_ref, "id", None)})

        if not contrato_ref:
            diagnostics.append({
                "linha_idx": getattr(it, "linha_idx", None),
                "tp": tp,
                "contrato_num_payload": num_raw,
                "ativo_payload": ativo_raw,
                "ativo_norm": ativo,
                "tem_log_model": bool(ContratoLog),
                "mov_hash": mov_hash,
                "motivo": "nao_localizado"
            })
            continue

        # Cabeçalho (para período)
        cab = find_cabecalho_por_num(db, num) if num else None

        # Aplica enriquecimento
        changed = apply_enrichment(contrato_ref, cab, payload, permitir_valor_data)
        if any(changed.values()):
            updated += 1
            set_num     += changed["num"]
            set_cli     += changed["cli"]
            set_periodo += changed["periodo"]
            set_valor   += changed["valor"]
            set_data    += changed["data"]
        else:
            if verbose:
                diagnostics.append({"linha_idx": getattr(it, "linha_idx", None),
                                    "contrato_id": getattr(contrato_ref, "id", None),
                                    "motivo": "sem_alteracoes"})

    return {
        "lote_id": lote_id,
        "total_itens": total,
        "contratos_atualizados": updated,
        "set_num": set_num,
        "set_cli": set_cli,
        "set_periodo": set_periodo,
        "set_valor": set_valor,
        "set_data": set_data,
        "diagnostics": diagnostics,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lote", type=int, required=True, help="ID do lote já importado")
    ap.add_argument("--dry-run", action="store_true", help="Apenas simula; não grava no banco")
    ap.add_argument("--verbose", action="store_true", help="Inclui diagnósticos detalhados por linha")
    ap.add_argument("--incluir-retorno-troca", action="store_true",
                    help="Permite preencher nº/cliente/período também para linhas RETORNO/TROCA")
    args = ap.parse_args()

    os.makedirs(RUNTIME_DIR, exist_ok=True)

    with SessionLocal() as db:
        res = fixup_lote(db, args.lote, incluir_retorno_troca=args.incluir_retorno_troca, verbose=args.verbose)
        if args.dry_run:
            db.rollback()
        else:
            db.commit()

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(RUNTIME_DIR, f"fixup_lote_{args.lote}_{ts}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)

    print(json.dumps(res, ensure_ascii=False, indent=2))
    print(f"[ok] Resumo salvo em: {out}")

if __name__ == "__main__":
    main()
