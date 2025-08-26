# routers/ultima_importacao.py
# v2025-08-26.9
# - [PAGE] /importacoes agora injeta payload server-side no template (render SSR)
# - Mantém JSON→JSONL→DB, cálculo de totais e endpoints JSON
# - ok = qtd_itens; trocas por par (contrato, data, marcador)

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import json, os
from collections import defaultdict

__version__ = "2025.08.26.9"

router = APIRouter(prefix="/ultima-importacao", tags=["Dashboard"])
router_page = APIRouter(tags=["Importações"])
templates = Jinja2Templates(directory="templates")

# ---- DB opcional ----
try:
    from database import SessionLocal
except Exception:
    SessionLocal = None  # type: ignore
try:
    from models import ContratoLog
except Exception:
    ContratoLog = None  # type: ignore

# ---------------- Paths ----------------
def _candidates_json() -> List[Path]:
    cands: List[Path] = []
    env = os.environ.get("ULTIMA_IMPORTACAO_PATH")
    if env: cands.append(Path(env))
    try:
        from utils.runtime import path_ultima_importacao as _rt_json
        cands.append(_rt_json())
    except Exception:
        pass
    cwd = Path.cwd(); here = Path(__file__).resolve(); proj = here.parents[1]
    cands += [
        cwd / "runtime" / "ultima_importacao.json",
        cwd / "data" / "ultima_importacao.json",
        cwd / "ultima_importacao.json",
        proj / "runtime" / "ultima_importacao.json",
        proj / "data" / "ultima_importacao.json",
        proj / "ultima_importacao.json",
    ]
    out, seen = [], set()
    for p in cands:
        try: rp = p.resolve()
        except Exception: rp = p
        if rp not in seen:
            out.append(rp); seen.add(rp)
    return out

def _json_path() -> Path:
    for p in _candidates_json():
        if p.exists(): return p
    return (Path.cwd() / "runtime" / "ultima_importacao.json").resolve()

def _jsonl_path() -> Path:
    env = os.environ.get("IMPORTACOES_JSONL_PATH")
    if env: return Path(env).resolve()
    try:
        from utils.runtime import path_importacoes_jsonl as _rt_jsonl
        return _rt_jsonl().resolve()
    except Exception:
        pass
    return (Path.cwd() / "runtime" / "importacoes.jsonl").resolve()

# --------------- Helpers ---------------
def _normalize_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    base = {
        "arquivo": None, "linhas_total": 0, "inseridos": 0, "atualizados": 0,
        "trocas": 0, "retornos": 0, "erros": 0,
        "lote_id": d.get("lote_id") if isinstance(d, dict) else None,
        "status": d.get("status") if isinstance(d, dict) else None,
        "mensagem": d.get("mensagem") if isinstance(d, dict) else None,
        "processado_em": d.get("processado_em") if isinstance(d, dict) else None,
        "timestamp": d.get("timestamp") if isinstance(d, dict) else None,
    }
    if isinstance(d, dict):
        base.update(d)
    return base

def _extract_list(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, list): return obj
    if not isinstance(obj, dict): return []
    for key in ("itens", "items", "dados", "linhas", "detalhes", "movimentos", "registros"):
        v = obj.get(key)
        if isinstance(v, list): return v
    return []

def _read_json_with_meta() -> Dict[str, Any]:
    path = _json_path()
    exists = path.exists()
    meta = {
        "disponivel": bool(exists),
        "versao_router": __version__,
        "path": str(path),
        "updated_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(sep=" ", timespec="seconds") if exists else None,
    }
    if not exists:
        return {**meta, "mensagem": "Arquivo de última importação não encontrado.", "itens": [], "itens_origem": "nenhum"}

    try:
        text = path.read_text(encoding="utf-8")
        data = json.loads(text) if text.strip() else {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao ler JSON: {e}")

    resumo = _normalize_dict(data if isinstance(data, dict) else {})
    itens_raw = _extract_list(data)
    origem = "json" if itens_raw else "json(vazio)"
    return {**resumo, **meta, "itens": itens_raw, "itens_origem": origem}

def _read_jsonl(limit: int = 800) -> List[Dict[str, Any]]:
    p = _jsonl_path()
    itens: List[Dict[str, Any]] = []
    if p.exists():
        with p.open(encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln: continue
                try: itens.append(_normalize_dict(json.loads(ln)))
                except Exception: continue
    itens = list(reversed(itens))
    if limit > 0: itens = itens[: min(limit, 2000)]
    return itens

def _parse_dt_safe(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z","%Y-%m-%dT%H:%M:%S%z","%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S","%Y-%m-%dT%H:%M:%S.%f"):
        try: return datetime.strptime(s, fmt)
        except Exception: continue
    return None

def _hydrate_from_jsonl(meta: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
    eventos = _read_jsonl(limit=800)
    if not eventos: return [], "nenhum_jsonl"

    alvo = meta.get("lote_id")
    if alvo is not None:
        for ev in eventos:
            if str(ev.get("lote_id")) == str(alvo):
                lst = _extract_list(ev)
                if lst: return lst, f"jsonl(lote_id={alvo})"

    ts_meta = _parse_dt_safe(meta.get("timestamp")) or _parse_dt_safe(meta.get("processado_em"))
    if ts_meta:
        candidatos = []
        for ev in eventos:
            ts = _parse_dt_safe(ev.get("timestamp")) or _parse_dt_safe(ev.get("processado_em"))
            if not ts: continue
            same_day = ts.date() == ts_meta.date()
            close = abs((ts - ts_meta).total_seconds()) <= 7200
            if same_day or close:
                lst = _extract_list(ev)
                if lst: candidatos.append((abs((ts - ts_meta).total_seconds()), lst))
        if candidatos:
            candidatos.sort(key=lambda x: x[0])
            return candidatos[0][1], "jsonl(~timestamp)"

    for ev in eventos:
        lst = _extract_list(ev)
        if lst: return lst, "jsonl(ultimo_com_itens)"
    return [], "jsonl_sem_itens"

# ------------- DB fallback (ContratoLog) -------------
def _row_cols(model) -> set:
    try: return set(model.__table__.columns.keys())  # type: ignore
    except Exception: return set()

_ACTION_MAP = {
    "ENVIO_INSERIDO": "INSERIDO",
    "ENVIO_ATUALIZADO": "ATUALIZADO",
    "RETORNO_REMOVIDO": "REMOVIDO",
    "TROCA_REMOVIDO": "REMOVIDO(TROCA)",
    "TROCA_INSERIDO": "INSERIDO(TROCA)",
    "ENVIO_INSERIDO(LEGADO)": "INSERIDO",
    "RETORNO_REMOVIDO(LEGADO)": "REMOVIDO",
    "TROCA_REMOVIDO(LEGADO)": "REMOVIDO(TROCA)",
    "ENVIO_ATUALIZADO(LEGADO)": "ATUALIZADO",
}

def _fmt_dt(dtval) -> Optional[str]:
    if isinstance(dtval, datetime): return dtval.isoformat(sep=" ", timespec="seconds")
    return str(dtval) if dtval not in (None, "") else None

def _row_to_item_from_cols(row, cols: set) -> Dict[str, Any]:
    status_col = getattr(row, "status", None) if "status" in cols else None
    acao_col   = getattr(row, "acao", None) if "acao" in cols else None
    tipo_col   = getattr(row, "tp_transacao", None) if "tp_transacao" in cols else None

    status = (str(status_col).upper().strip() if status_col not in (None, "") else None)
    if not status and acao_col:
        status = _ACTION_MAP.get(str(acao_col).upper().strip(), None)

    tipo = str(tipo_col).upper().strip() if (tipo_col not in (None, "")) else None
    if not status:
        if tipo and ("RETORNO" in tipo or "REMOVIDO" in tipo): status = "REMOVIDO"
        elif tipo and "TROCA" in tipo: status = "INSERIDO(TROCA)"
        elif tipo and ("ENVIO" in tipo or "ATUALIZADO" in tipo): status = "INSERIDO"
        else: status = "—"

    contrato_id = getattr(row, "contrato_id", None) if "contrato_id" in cols else None
    cab_id      = getattr(row, "contrato_cabecalho_id", None) if "contrato_cabecalho_id" in cols else None
    if contrato_id is not None and cab_id is not None:
        contrato = f"{contrato_id}/{cab_id}"
    else:
        contrato = contrato_id or cab_id

    data_mov = getattr(row, "data_mov", None) if "data_mov" in cols else None
    if not data_mov and "data_modificacao" in cols:
        data_mov = getattr(row, "data_modificacao", None)
    data_mov = _fmt_dt(data_mov)

    ativo     = getattr(row, "ativo", None) if "ativo" in cols else None
    cod_cli   = getattr(row, "cod_cli", None) if "cod_cli" in cols else None
    desc      = getattr(row, "descricao", None) if "descricao" in cols else None
    msg       = getattr(row, "mensagem", None) if "mensagem" in cols else None
    mov_hash  = getattr(row, "mov_hash", None) if "mov_hash" in cols else None

    return {
        "status": status,
        "contrato": contrato,
        "item": mov_hash,
        "descricao": (desc or msg or ""),
        "serial": None,
        "cod_pro": None,
        "data_mov": data_mov,
        "tipo": (tipo or ("RETORNO" if "REMOVIDO" in status else "ENVIO")),
        "qtd": 1,
        "valor_mensal": None,
        "meses_rest": None,
        "cliente": None,
        "ativo": ativo,
        "cod_cliente": cod_cli,
        "obs": msg or "",
    }

def _hydrate_from_db(meta: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
    if SessionLocal is None or ContratoLog is None:
        return [], "db_indisponivel"
    cols = _row_cols(ContratoLog)
    if not cols: return [], "db_indisponivel"

    ts_meta = _parse_dt_safe(meta.get("timestamp")) or _parse_dt_safe(meta.get("processado_em"))
    start, end = None, None
    if ts_meta:
        start = ts_meta - timedelta(hours=24)
        end   = ts_meta + timedelta(hours=24)

    try:
        from sqlalchemy import desc
    except Exception:
        desc = lambda x: x  # type: ignore

    sess = SessionLocal()
    try:
        q = sess.query(ContratoLog)
        origem = "db(logs_recente)"
        if "data_modificacao" in cols and ts_meta:
            q = q.filter(getattr(ContratoLog, "data_modificacao") >= start,
                         getattr(ContratoLog, "data_modificacao") <= end)
            origem = "db(logs±24h)"
        if "id" in cols:
            q = q.order_by(desc(getattr(ContratoLog, "id")))
        elif "data_modificacao" in cols:
            q = q.order_by(desc(getattr(ContratoLog, "data_modificacao")))
        q = q.limit(500)

        itens = [_row_to_item_from_cols(r, cols) for r in q.all()]
        def _nonempty(it: Dict[str, Any]) -> bool:
            keys = ["contrato","descricao","data_mov","ativo","cod_cliente","status","tipo","item"]
            return any(it.get(k) not in (None,"","—") for k in keys)
        itens = [i for i in itens if _nonempty(i)]
        return itens, origem
    finally:
        try: sess.close()
        except Exception: pass

# ---------- Derivação de totais ----------
def _compute_totals(itens: List[Dict[str, Any]]) -> Dict[str, int]:
    envios = retornos = 0
    troca_env = defaultdict(int)
    troca_ret = defaultdict(int)

    for i in itens:
        tipo = (i.get("tipo") or "").upper()
        if "ENVIO" in tipo: envios += 1
        if "RETORNO" in tipo: retornos += 1

        if "TROCA" in tipo:
            c = str(i.get("contrato") or "")
            d = (i.get("data_mov") or "")[:10]
            marcador = (i.get("obs") or i.get("descricao") or i.get("ativo") or "")
            key = (c, d, str(marcador))
            if "ENVIO" in tipo:
                troca_env[key] += 1
            elif "RETORNO" in tipo:
                troca_ret[key] += 1

    trocas = 0
    keys = set(list(troca_env.keys()) + list(troca_ret.keys()))
    for k in keys:
        trocas += min(troca_env.get(k, 0), troca_ret.get(k, 0))

    return {
        "linhas_total": len(itens),
        "inseridos": envios,
        "retornos": retornos,
        "trocas": trocas,
        "atualizados": 0,
        "ok": len(itens),
    }

def _apply_totals(raw: Dict[str, Any]) -> Dict[str, Any]:
    itens = raw.get("itens") or []
    totals = _compute_totals(itens)

    raw["ok"] = totals["ok"]
    for k in ("linhas_total","inseridos","retornos","trocas","atualizados"):
        if not raw.get(k):
            raw[k] = totals[k]

    if not raw.get("arquivo") and raw.get("path"):
        try:
            raw["arquivo"] = Path(raw["path"]).name
        except Exception:
            raw["arquivo"] = None

    if not raw.get("contratos_afetados"):
        contratos = []
        for i in itens:
            c = i.get("contrato")
            if c not in (None, "", "—"):
                contratos.append(str(c))
        raw["contratos_afetados"] = sorted(list(set(contratos))) if contratos else []
    return raw

# ------------- composição -------------
def _load_raw_with_meta() -> Dict[str, Any]:
    raw = _read_json_with_meta()

    if not raw.get("itens"):
        itens_h, origem = _hydrate_from_jsonl(raw)
        if itens_h:
            raw["itens"] = itens_h
            raw["itens_origem"] = origem

    if not raw.get("itens"):
        itens_db, origem_db = _hydrate_from_db(raw)
        if itens_db:
            raw["itens"] = itens_db
            raw["itens_origem"] = origem_db

    raw = _apply_totals(raw)
    return raw

# ------------- API -------------
@router.get("", summary="Dados normalizados para 'Última Importação'")
def get_ultima_importacao() -> Dict[str, Any]:
    out = _load_raw_with_meta()
    out["qtd_itens"] = len(out.get("itens", []))
    return out

@router.get("/raw", summary="Conteúdo bruto do arquivo de última importação")
def get_raw() -> Dict[str, Any]:
    return _load_raw_with_meta()

@router.get("/historico", summary="Histórico das importações (JSONL)")
def get_historico(limit: int = Query(200, ge=1, le=1000)) -> Dict[str, Any]:
    itens = _read_jsonl(limit=limit)
    return {"versao_router": __version__, "path_jsonl": str(_jsonl_path()), "total": len(itens), "itens": itens}

@router.get("/debug", summary="Diagnóstico de descoberta de path")
def debug() -> Dict[str, Any]:
    path = _json_path()
    jsonl = _jsonl_path()
    sample = _read_jsonl(limit=5)
    db_ok = bool(SessionLocal and ContratoLog)
    cols = set()
    if db_ok:
        try:
            cols = set(ContratoLog.__table__.columns.keys())  # type: ignore
        except Exception:
            cols = set()
    return {
        "versao_router": __version__,
        "resolved_path": str(path),
        "exists": path.exists(),
        "candidates": [str(p) for p in _candidates_json()],
        "cwd": str(Path.cwd()),
        "jsonl_path": str(jsonl),
        "jsonl_exists": jsonl.exists(),
        "jsonl_sample_keys": [sorted(list((s or {}).keys())) for s in sample],
        "db_fallback_disponivel": db_ok,
        "db_cols": sorted(list(cols)) if cols else [],
    }

@router.get("/ping", summary="Healthcheck do router")
def ping() -> Dict[str, Any]:
    return {"pong": True, "versao_router": __version__, "json_path": str(_json_path()), "jsonl_path": str(_jsonl_path())}

# ------------- Página (SSR) e compat -------------
@router_page.get("/ultima_importacao", include_in_schema=False)
def compat_redirect():
    return RedirectResponse(url="/ultima-importacao", status_code=307)

@router_page.get("/importacoes", summary="Página: Importações")
def importacoes_page(request: Request):
    payload = _load_raw_with_meta()
    payload["qtd_itens"] = len(payload.get("itens", []))
    return templates.TemplateResponse(
        "importacoes.html",
        {"request": request, "payload": payload, "itens": payload.get("itens", [])},
    )

__all__ = ["router", "router_page", "__version__"]
