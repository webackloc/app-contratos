# main.py – Versão 3.9.17 (2025-08-11) — inclui admin_users e ajustes mínimos sem quebrar o que já funciona
import os
import csv
import io
import json
import hashlib
from uuid import uuid4
from datetime import datetime, date
from typing import Optional

from fastapi import FastAPI, Request, Depends, UploadFile, File, Form, Body, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from jinja2 import TemplateNotFound

from starlette.middleware.sessions import SessionMiddleware

from sqlalchemy import func
from sqlalchemy.orm import Session

from utils.auth_middleware import AuthRequiredMiddleware

from routers import admin_users as admin_users_router


# ── MODELOS ────────────────────────────────────────────────────────────────────
try:
    from models import Contrato, ContratoCabecalho
except Exception:
    Contrato = None
    ContratoCabecalho = None

# opcionais (podem não existir no seu schema; tratamos com fallback)
try:
    from models import ImportMovimentoRegistro
except Exception:
    ImportMovimentoRegistro = None

try:
    from models import ContratoLog
except Exception:
    ContratoLog = None

# ── DB / base (necessário para create_all e SessionLocal usado abaixo) ─────────
try:
    from database import SessionLocal, engine, Base
except Exception:
    SessionLocal = None
    engine = None
    Base = None

# ── Routers opcionais ─────────────────────────────────────────────────────────
try:
    from routers import auth as auth_router
except Exception:
    auth_router = None

try:
    from routers import dashboard
except Exception:
    dashboard = None

try:
    from routers import export as export_router
except Exception:
    export_router = None

try:
    from routers import debug_auth as debug_auth_router
    _HAS_DEBUG_AUTH = True
except Exception:
    _HAS_DEBUG_AUTH = False

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(version="3.9.17")

# diretórios/arquivos de runtime (logs/ultima importação)
RUNTIME_DIR = os.getenv("RUNTIME_DIR", os.path.join(os.getcwd(), "runtime"))
os.makedirs(RUNTIME_DIR, exist_ok=True)
LAST_IMPORT_FILE = os.path.join(RUNTIME_DIR, "ultima_importacao.json")
CABECALHO_LOG_FILE = os.path.join(RUNTIME_DIR, "cabecalhos.log")

# Whitelist de autenticação (NÃO inclua "/" aqui)
AUTH_WHITELIST = (
    "/login",
    "/logout",
    "/favicon.ico",
    "/healthz",
    "/api/health",
    "/static/",
)
if os.getenv("ENABLE_DEBUG_AUTH") == "1":
    AUTH_WHITELIST = AUTH_WHITELIST + ("/debug/",)

# Middlewares (ORDEM IMPORTA: Session primeiro, depois Auth)
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SECRET_KEY", "dev-change-me"),
    same_site="lax",
    https_only=False,
    session_cookie="appsession",
)
app.add_middleware(
    AuthRequiredMiddleware,
    whitelist=AUTH_WHITELIST,
)

# Cria tabelas se possível (safe no-op se já existirem)
try:
    if Base is not None and engine is not None:
        Base.metadata.create_all(bind=engine)
except Exception as _e:
    print("[WARN] create_all falhou no header:", _e)

# static / templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
app.state.templates = templates  # expõe p/ routers externos

print(
    "MIDDLEWARES ORDER:",
    [m.cls.__name__ for m in app.user_middleware],
    "(esperado: ['SessionMiddleware', 'AuthRequiredMiddleware'])",
)

# Routers opcionais (só inclui se existirem)
if auth_router and hasattr(auth_router, "router"):
    app.include_router(auth_router.router)
if _HAS_DEBUG_AUTH and debug_auth_router and hasattr(debug_auth_router, "router"):
    app.include_router(debug_auth_router.router)
if export_router and hasattr(export_router, "router"):
    app.include_router(export_router.router)
if dashboard and hasattr(dashboard, "router"):
    app.include_router(dashboard.router, prefix="/api")

app.include_router(admin_users_router.router)

# ── DB util ───────────────────────────────────────────────────────────────────
def get_db():
    if SessionLocal is None:
        raise RuntimeError("SessionLocal indisponível. Verifique o módulo database.")
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ── Helpers gerais ────────────────────────────────────────────────────────────
def parse_float(val):
    try:
        return float(str(val).replace("R$", "").replace(".", "").replace(",", ".").strip())
    except Exception:
        return 0.0

def parse_data(valor: str) -> date:
    valor = (valor or "").strip()
    if not valor:
        return date(1900, 1, 1)
    try:
        if "/" in valor:
            return datetime.strptime(valor, "%d/%m/%Y").date()
        return datetime.strptime(valor, "%Y-%m-%d").date()
    except Exception:
        return date(1900, 1, 1)

def meses_decorridos(inicio: date, fim: date) -> int:
    if inicio > fim:
        return 0
    anos = fim.year - inicio.year
    meses = fim.month - inicio.month
    total = anos * 12 + meses
    if fim.day < inicio.day:
        total -= 1
    return max(total, 0)

def calcular_valores_no_obj(contrato):
    vm = getattr(contrato, "valor_mensal", 0.0) or 0.0
    mr = getattr(contrato, "meses_restantes", 0) or 0
    contrato.valor_global_contrato = vm * mr
    contrato.valor_presente_contrato = vm * mr
    return contrato

def recomputa_meses_restantes_if_needed(contrato, prazo_contratual: Optional[int]):
    if (getattr(contrato, "meses_restantes", 0) or 0) > 0:
        return
    if prazo_contratual and getattr(contrato, "data_envio", None) and contrato.data_envio.year > 1900:
        passados = meses_decorridos(contrato.data_envio, date.today())
        contrato.meses_restantes = max(int(prazo_contratual) - passados, 0)

# === Descoberta dinâmica do campo de nº do contrato em um modelo (cabecalho/itens)
def get_contract_field(model) -> Optional[str]:
    try:
        cols = set(model.__table__.columns.keys())
    except Exception:
        return None
    for name in ("contrato_n", "contrato_num", "numero_contrato", "num_contrato", "n_contrato"):
        if name in cols:
            return name
    return None

# ---- LOG helper (compatível com diferentes esquemas de ContratoLog) ----
def add_contrato_log(db: Session, contrato_obj, acao: str, detalhes: dict):
    """
    Grava um log para o item de contrato (Contrato). Se o modelo ContratoLog
    não existir no schema, apenas ignora com aviso.
    """
    try:
        cols = set(ContratoLog.__table__.columns.keys())
    except Exception:
        print("[WARN] Modelo ContratoLog indisponível; log ignorado.")
        return

    payload_json = json.dumps(detalhes, ensure_ascii=False, default=str)
    kwargs = {}

    # coluna do vínculo
    vinc_id = getattr(contrato_obj, "id", None)
    if "contrato_id" in cols:
        kwargs["contrato_id"] = vinc_id
    elif "id_contrato" in cols:
        kwargs["id_contrato"] = vinc_id
    elif "contrato" in cols:
        kwargs["contrato"] = vinc_id

    # ação
    if "acao" in cols:
        kwargs["acao"] = acao
    elif "action" in cols:
        kwargs["action"] = acao

    # conteúdo
    if "detalhes" in cols:
        kwargs["detalhes"] = payload_json
    elif "mensagem" in cols:
        kwargs["mensagem"] = payload_json
    elif "descricao" in cols:
        kwargs["descricao"] = payload_json
    elif "payload" in cols:
        kwargs["payload"] = payload_json
    elif "conteudo" in cols:
        kwargs["conteudo"] = payload_json

    if "created_at" in cols and "created_at" not in kwargs:
        kwargs["created_at"] = datetime.utcnow()

    try:
        log = ContratoLog(**{k: v for k, v in kwargs.items() if k in cols})
        db.add(log)
        db.flush()
    except Exception as e:
        print("[WARN] Falha ao gravar log:", e)

# Helpers (import movimentos)
def im_normalize_header(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "_").replace("-", "_")

def im_parse_date(value: str):
    v = (value or "").strip()
    if not v:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(v, fmt).date().isoformat()
        except Exception:
            pass
    return None

def im_to_float(x):
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def im_to_int(x):
    try:
        return int(float(str(x).replace(",", ".").strip()))
    except Exception:
        return 0

def make_import_key(contrato_n: str, ativo: str, cod_cli: str, tipo: str, data_mov_iso: str) -> str:
    raw = f"{(contrato_n or '').strip()}|{(ativo or '').strip()}|{(cod_cli or '').strip()}|{(tipo or '').strip()}|{(data_mov_iso or '').strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

DEFAULT_REQUIRED = ["contrato", "item", "data_mov", "tipo", "qtd", "valor_mensal", "meses_restantes"]
OPTIONAL_FIELDS = ["cliente", "observacao", "ativo", "cod_cli", "serial", "cod_pro", "descricao_produto"]

# -------------------------------------------------------
# Heurística de busca para ENVIO/RETORNO
# -------------------------------------------------------
def _only_digits(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())

def _eq_relaxed(a: str, b: str) -> bool:
    da, db = _only_digits(a), _only_digits(b)
    if da or db:
        return da == db
    return str(a or "").strip().lower() == str(b or "").strip().lower()

def find_item_by_heuristics(db: Session, it_field: str, contrato_numero: str, r: dict):
    q = db.query(Contrato).filter(getattr(Contrato, it_field) == contrato_numero)

    ativo = (r.get("ativo") or "").strip()
    serial = (r.get("serial") or "").strip()
    cod_pro = (r.get("cod_pro") or "").strip()
    desc = (r.get("descricao_produto") or r.get("item") or "").strip()
    cod_cli = (r.get("cod_cli") or "").strip()

    if ativo:
        m = q.filter(Contrato.ativo == ativo).first()
        if m:
            return m, "contrato+ativo"
        candidatos = q.filter(Contrato.ativo.isnot(None)).all()
        for c in candidatos:
            if _eq_relaxed(c.ativo, ativo):
                return c, "contrato+ativo(relaxed)"

    if serial:
        m = q.filter(Contrato.serial == serial).first()
        if m:
            return m, "contrato+serial"

    if cod_pro:
        m = q.filter(Contrato.cod_pro == cod_pro).first()
        if m:
            return m, "contrato+cod_pro"

    if cod_cli and desc:
        m = q.filter(
            Contrato.cod_cli == cod_cli,
            func.lower(Contrato.descricao_produto) == desc.lower(),
        ).first()
        if m:
            return m, "contrato+cod_cli+descricao"

    if desc:
        m = q.filter(func.lower(Contrato.descricao_produto) == desc.lower()).first()
        if m:
            return m, "contrato+descricao"

    m = q.first()
    if m:
        return m, "fallback:primeiro_do_contrato"
    return None, "nao_encontrado"

def find_single_for_return(db: Session, it_field: str, contrato_numero: str, r: dict):
    q = db.query(Contrato).filter(getattr(Contrato, it_field) == contrato_numero)

    ativo = (r.get("ativo") or "").strip()
    serial = (r.get("serial") or "").strip()
    cod_pro = (r.get("cod_pro") or "").strip()
    desc = (r.get("descricao_produto") or r.get("item") or "").strip()
    cod_cli = (r.get("cod_cli") or "").strip()

    if ativo:
        if serial:
            m = (
                q.filter(Contrato.ativo == ativo, Contrato.serial == serial)
                .order_by(Contrato.id.desc())
                .first()
            )
            if m:
                return m, "contrato+ativo+serial"

        if cod_pro:
            m = (
                q.filter(Contrato.ativo == ativo, Contrato.cod_pro == cod_pro)
                .order_by(Contrato.id.desc())
                .first()
            )
            if m:
                return m, "contrato+ativo+cod_pro"

        if cod_cli and desc:
            m = (
                q.filter(
                    Contrato.ativo == ativo,
                    Contrato.cod_cli == cod_cli,
                    func.lower(Contrato.descricao_produto) == desc.lower(),
                )
                .order_by(Contrato.id.desc())
                .first()
            )
            if m:
                return m, "contrato+ativo+cod_cli+descricao"

        m = q.filter(Contrato.ativo == ativo).order_by(Contrato.id.desc()).first()
        if m:
            return m, "contrato+ativo"

        candidatos = q.filter(Contrato.ativo.isnot(None)).all()
        relaxed = [c for c in candidatos if _eq_relaxed(c.ativo, ativo)]
        if relaxed:
            if serial:
                f = [c for c in relaxed if (c.serial or "").strip() == serial]
                if f:
                    return sorted(f, key=lambda x: x.id, reverse=True)[0], "contrato+ativo(relaxed)+serial"
            if cod_pro:
                f = [c for c in relaxed if (c.cod_pro or "").strip() == cod_pro]
                if f:
                    return sorted(f, key=lambda x: x.id, reverse=True)[0], "contrato+ativo(relaxed)+cod_pro"
            if desc:
                f = [c for c in relaxed if (c.descricao_produto or "").lower() == desc.lower()]
                if f:
                    return sorted(f, key=lambda x: x.id, reverse=True)[0], "contrato+ativo(relaxed)+descricao"
            if cod_cli:
                f = [c for c in relaxed if (c.cod_cli or "").strip() == cod_cli]
                if f:
                    return sorted(f, key=lambda x: x.id, reverse=True)[0], "contrato+ativo(relaxed)+cod_cli"
            return sorted(relaxed, key=lambda x: x.id, reverse=True)[0], "contrato+ativo(relaxed)"

    unico, how = find_item_by_heuristics(db, it_field, contrato_numero, r)
    return unico, how

# ── ROTAS HTML ────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "ano": datetime.now().year})

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

# ── ENDPOINTS DE DADOS (dashboard simples) ───────────────────────────────────
@app.get("/relatorio/carteira")
def relatorio_carteira(db: Session = Depends(get_db)):
    contratos = db.query(Contrato).all()
    return {
        "total_contratos": len(contratos),
        "total_valor_mensal": sum((c.valor_mensal or 0.0) for c in contratos),
        "total_valor_global": sum((c.valor_global_contrato or 0.0) for c in contratos),
        "total_valor_presente": sum((c.valor_presente_contrato or 0.0) for c in contratos),
    }

@app.get("/dashboard_data")
def dashboard_data(db: Session = Depends(get_db)):
    contratos = db.query(Contrato).all()
    valor_mensal_por_mes = {}
    for c in contratos:
        if not c.data_envio:
            continue
        chave = f"{c.data_envio.year:04d}-{c.data_envio.month:02d}"
        valor_mensal_por_mes[chave] = valor_mensal_por_mes.get(chave, 0.0) + (c.valor_mensal or 0.0)
    meses_ordenados = sorted(valor_mensal_por_mes.keys())

    valor_global_por_cliente, quantidade_por_cliente, dist_mr = {}, {}, {}
    for c in contratos:
        cli = c.nome_cli or "—"
        quantidade_por_cliente[cli] = quantidade_por_cliente.get(cli, 0) + 1
        valor_global_por_cliente[cli] = valor_global_por_cliente.get(cli, 0.0) + (c.valor_global_contrato or 0.0)
        mr = int(c.meses_restantes or 0)
        dist_mr[mr] = dist_mr.get(mr, 0) + 1

    return {
        "valor_mensal_por_mes": {"labels": meses_ordenados, "data": [valor_mensal_por_mes[m] for m in meses_ordenados]},
        "valor_global_por_cliente": {"labels": list(valor_global_por_cliente.keys()), "data": list(valor_global_por_cliente.values())},
        "quantidade_por_cliente": {"labels": list(quantidade_por_cliente.keys()), "data": list(quantidade_por_cliente.values())},
        "distribuicao_meses_restantes": {"labels": list(dist_mr.keys()), "data": list(dist_mr.values())},
    }

# ── LISTAGEM & CADASTRO MANUAL ───────────────────────────────────────────────
@app.get("/contratos_html", response_class=HTMLResponse)
async def contratos_html(request: Request, db: Session = Depends(get_db)):
    contratos = db.query(Contrato).all()
    return templates.TemplateResponse("contratos.html", {"request": request, "contratos": contratos})

@app.get("/cadastrar", response_class=HTMLResponse)
async def show_form(request: Request, db: Session = Depends(get_db)):
    cabecalhos = db.query(ContratoCabecalho).all()
    return templates.TemplateResponse("cadastrar.html", {"request": request, "contratos": cabecalhos})

@app.post("/cadastrar")
async def cadastrar(
    request: Request,
    # nomes "oficiais"
    nome_cliente: Optional[str] = Form(None),
    cnpj: Optional[str] = Form(None),
    contrato_n: Optional[str] = Form(None),
    prazo_contratual: Optional[int] = Form(None),
    indice_reajuste: Optional[str] = Form(None),
    vendedor: Optional[str] = Form(None),
    # aliases compat. retro
    contrato_num: Optional[str] = Form(None),
    prazo: Optional[int] = Form(None),
    indice: Optional[str] = Form(None),
    # payload opcional JSON
    payload: Optional[dict] = Body(None),
    db: Session = Depends(get_db),
):
    if payload:
        nome_cliente = nome_cliente or payload.get("nome_cliente")
        cnpj = cnpj or payload.get("cnpj")
        contrato_n = contrato_n or payload.get("contrato_n") or payload.get("contrato_num") or payload.get("contrato")
        prazo_contratual = prazo_contratual or payload.get("prazo_contratual") or payload.get("prazo")
        indice_reajuste = indice_reajuste or payload.get("indice_reajuste") or payload.get("indice")
        vendedor = vendedor or payload.get("vendedor")

    contrato_n = contrato_n or contrato_num
    prazo_contratual = prazo_contratual or prazo
    indice_reajuste = indice_reajuste or indice

    missing = []
    if not nome_cliente:
        missing.append("nome_cliente")
    if not cnpj:
        missing.append("cnpj")
    if not contrato_n:
        missing.append("contrato_n/contrato_num")
    if not prazo_contratual:
        missing.append("prazo_contratual")
    if not indice_reajuste:
        missing.append("indice_reajuste")
    if not vendedor:
        missing.append("vendedor")

    if missing:
        cabecalhos = db.query(ContratoCabecalho).all()
        return templates.TemplateResponse(
            "cadastrar.html",
            {"request": request, "contratos": cabecalhos, "erro": f"Campos obrigatórios ausentes: {', '.join(missing)}"},
            status_code=400,
        )

    cab_field = get_contract_field(ContratoCabecalho)
    if not cab_field:
        return JSONResponse(
            {
                "ok": False,
                "mensagem": "Modelo ContratoCabecalho não possui coluna de número de contrato reconhecida (ex.: contrato_n/contrato_num).",
            },
            status_code=500,
        )

    kwargs = dict(
        nome_cliente=nome_cliente.strip(),
        cnpj=cnpj.strip(),
        prazo_contratual=int(prazo_contratual),
        indice_reajuste=str(indice_reajuste).strip(),
        vendedor=vendedor.strip(),
    )
    kwargs[cab_field] = str(contrato_n).strip()

    novo = ContratoCabecalho(
        **{k: v for k, v in kwargs.items() if k in ContratoCabecalho.__table__.columns.keys()}
    )
    db.add(novo)
    db.commit()

    # LOG de cadastro de cabeçalho (file-based, sem mudar schema)
    try:
        reg = {
            "ts": datetime.utcnow().isoformat(),
            "acao": "CABECALHO_CADASTRADO",
            "contrato": kwargs[cab_field],
            "nome_cliente": kwargs.get("nome_cliente"),
            "cnpj": kwargs.get("cnpj"),
            "prazo_contratual": kwargs.get("prazo_contratual"),
            "indice_reajuste": kwargs.get("indice_reajuste"),
            "vendedor": kwargs.get("vendedor"),
            "usuario": "web",
        }
        with open(CABECALHO_LOG_FILE, "a", encoding="utf-8") as fp:
            fp.write(json.dumps(reg, ensure_ascii=False) + "\n")
    except Exception as e:
        print("[WARN] Falha ao registrar log de cabeçalho:", e)

    return RedirectResponse("/cadastrar", status_code=302)

# ── IMPORTAÇÃO DE CONTRATOS (CSV) – legado ───────────────────────────────────
@app.get("/upload", response_class=HTMLResponse)
async def upload_form(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request})

@app.post("/upload")
async def upload_csv(request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)):
    contents = await file.read()
    decoded = contents.decode("utf-8").splitlines()
    reader = csv.DictReader(decoded)
    hoje = date.today()

    it_field = get_contract_field(Contrato) or "contrato_n"

    for row in reader:
        try:
            data_envio = parse_data(row.get("data de envio", ""))
            periodo_contratual = int(parse_float(row.get("periodo contratual", "0")))
            passados = meses_decorridos(data_envio, hoje)
            meses_restantes_calc = max(periodo_contratual - passados, 0)
            novo = Contrato(
                ativo=(row.get("ativo", "") or "").strip(),
                serial=(row.get("serial", "") or "").strip(),
                cod_pro=(row.get("cod pro", "") or "").strip(),
                descricao_produto=(row.get("descrição do produto", "") or "").strip(),
                cod_cli=(row.get("cod cli", "") or "").strip(),
                nome_cli=(row.get("nome cli", "") or "").strip(),
                data_envio=data_envio,
                valor_mensal=parse_float(row.get("valor mensal", "0")),
                periodo_contratual=periodo_contratual,
                meses_restantes=meses_restantes_calc,
            )
            setattr(novo, it_field, (row.get("contrato n", "") or "").strip())
            calcular_valores_no_obj(novo)
            db.add(novo)
        except Exception as e:
            print("Erro ao importar linha:", row, "Detalhe:", str(e))
            continue

    db.commit()
    return RedirectResponse("/", status_code=303)

# ── IMPORTAÇÃO DE MOVIMENTAÇÃO — UI ──────────────────────────────────────────
@app.get("/importar_movimentacao", response_class=HTMLResponse)
async def importar_movimentacao(request: Request):
    return templates.TemplateResponse("importar_movimentacao.html", {"request": request})

def _render_mapeamento_fallback(headers: list[str]) -> HTMLResponse:
    HEADERS_JSON = json.dumps(headers, ensure_ascii=False)
    html = """
<!DOCTYPE html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Mapeamento de Colunas (Preview/Importação)</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css">
</head>
<body class="container py-4">
  <h3>🧭 Mapeamento de Colunas (Preview / Pré-import / Importar)</h3>
  <form id="form-preview" class="mt-3" enctype="multipart/form-data" method="post">
    <div class="mb-3">
      <label class="form-label">Arquivo CSV</label>
      <input type="file" name="file" accept=".csv" required class="form-control">
      <div class="form-text">Reenvie o arquivo para cada operação (preview ou pré-import).</div>
    </div>
    <div id="maps" class="row g-3"></div>
    <input type="hidden" name="mapping" id="mapping-json">
    <div class="row g-3 mt-1">
      <div class="col-md-3">
        <label class="form-label">Delimitador</label>
        <select name="delimiter" class="form-select">
          <option value="auto" selected>Auto</option>
          <option value=";">;</option>
          <option value=",">,</option>
          <option value="|">|</option>
          <option value="\\t">TAB</option>
        </select>
      </div>
      <div class="col-md-3">
        <label class="form-label">Encoding</label>
        <select name="encoding" class="form-select">
          <option value="utf-8-sig" selected>utf-8-sig</option>
          <option value="latin1">latin1</option>
          <option value="utf-8">utf-8</option>
        </select>
      </div>
      <div class="col-md-3">
        <label class="form-label">Amostra</label>
        <input type="number" class="form-control" name="sample_rows" value="5" min="1" max="50">
      </div>
      <div class="col-md-3">
        <label class="form-label">Máx. linhas</label>
        <input type="number" class="form-control" name="max_rows" value="200000" min="1000" max="1000000">
      </div>
    </div>
    <div class="mt-3 d-flex gap-2">
      <button id="btn-preview"   type="button" class="btn btn-outline-secondary">Gerar preview</button>
      <button id="btn-preimport" type="button" class="btn btn-warning">Pré-importar (salvar lote)</button>
      <button id="btn-commit"    type="button" class="btn btn-primary" disabled>Importar agora</button>
      <a href="/dashboard" class="btn btn-light">Voltar</a>
    </div>
  </form>
  <hr>
  <h5>Resultado</h5>
  <pre id="resultado" class="border rounded p-3" style="max-height:420px; overflow:auto; background:#f8f9fa;">— gere o preview ou faça a pré-importação —</pre>
  <script>
    const HEADERS = __HEADERS_JSON__;
    const CAMPOS = [
      ["contrato","Nº do contrato (obrigatório)"],
      ["item","Item / código (obrigatório)"],
      ["descricao_produto","Descrição do produto (preferencial)"],
      ["serial","Serial (opcional)"],
      ["cod_pro","Código do produto (opcional)"],
      ["data_mov","Data do movimento (obrigatório)"],
      ["tipo","Tipo do movimento"],
      ["qtd","Quantidade"],
      ["valor_mensal","Valor mensal"],
      ["meses_restantes","Meses restantes"],
      ["cliente","Cliente"],
      ["observacao","Observação"],
      ["ativo","Ativo (opcional p/ anti-duplicação)"],
      ["cod_cli","Código do Cliente (opcional p/ anti-duplicação)"]
    ];
    const maps = document.getElementById("maps");
    CAMPOS.forEach(([key, label])=>{
      const col = document.createElement("div"); col.className="col-md-6";
      const lbl = document.createElement("label"); lbl.className="form-label"; lbl.textContent=label;
      const sel = document.createElement("select"); sel.className="form-select map-select"; sel.setAttribute("data-canon", key);
      const opt0 = document.createElement("option"); opt0.value=""; opt0.textContent="— não mapear —"; sel.appendChild(opt0);
      HEADERS.forEach(h=>{ const o=document.createElement("option"); o.value=h; o.textContent=h; sel.appendChild(o); });
      col.appendChild(lbl); col.appendChild(sel); maps.appendChild(col);
    });
    const res = document.getElementById("resultado");
    const btnPreview   = document.getElementById("btn-preview");
    const btnPreimport = document.getElementById("btn-preimport");
    const btnCommit    = document.getElementById("btn-commit");
    function formDataWithMapping() {
      const form = document.getElementById("form-preview");
      const mapping = {};
      document.querySelectorAll(".map-select").forEach(s=>{
        const canon = s.getAttribute("data-canon"); const val = (s.value||"").trim();
        if(val) mapping[canon]=val;
      });
      const fd = new FormData(form);
      fd.set("mapping", JSON.stringify(mapping));
      return fd;
    }
    async function post(url, fd){
      const r = await fetch(url, { method: "POST", body: fd });
      const t = await r.text();
      try { return JSON.parse(t); } catch { return t; }
    }
    btnPreview.onclick = async ()=>{
      res.textContent = "Processando preview...";
      const out = await post("/importar_movimentacao/preview", formDataWithMapping());
      res.textContent = typeof out === "string" ? out : JSON.stringify(out, null, 2);
    };
    btnPreimport.onclick = async ()=>{
      res.textContent = "Salvando lote (pré-importação)...";
      const out = await post("/importar_movimentacao/preimport", formDataWithMapping());
      res.textContent = typeof out === "string" ? out : JSON.stringify(out, null, 2);
      if (out && out.ok) btnCommit.removeAttribute("disabled");
    };
    btnCommit.onclick = async ()=>{
      res.textContent = "Executando importação (commit)...";
      const r = await fetch("/importar_movimentacao/commit", { method: "POST" });
      const t = await r.text();
      try { res.textContent = JSON.stringify(JSON.parse(t), null, 2); }
      catch { res.textContent = t; }
    };
  </script>
</body>
</html>
""".replace("__HEADERS_JSON__", HEADERS_JSON)
    return HTMLResponse(html)

# ── UPLOAD INICIAL (detecção de delimitador + cabeçalhos) ────────────────────
@app.post("/importar_movimentacao")
async def upload_movimentacao_csv(request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Envie um arquivo .csv")

    raw = await file.read()
    try:
        text = raw.decode("utf-8-sig", errors="replace")
    except Exception:
        text = raw.decode("latin1", errors="replace")

    try:
        delim = csv.Sniffer().sniff(text[:2000], delimiters=[",", ";", "|", "\t"]).delimiter
    except Exception:
        delim = ";"

    buf = io.StringIO(text)
    reader = csv.reader(buf, delimiter=delim)
    header_row = next(reader, [])
    headers = [str(h).strip() for h in header_row if str(h).strip()]
    if len(headers) <= 1 and header_row:
        brute = str(header_row[0])
        for sep in (";", ",", "|", "\t"):
            if sep in brute:
                headers = [h.strip() for h in brute.split(sep) if h.strip()]
                break

    temp_id = str(uuid4())
    temp_file = f"temp_{temp_id}.csv"
    with open(temp_file, "w", newline="", encoding="utf-8") as f:
        f.write(text)
    request.session["temp_file"] = temp_file

    try:
        return templates.TemplateResponse("mapeamento_colunas.html", {"request": request, "headers": headers})
    except TemplateNotFound:
        return _render_mapeamento_fallback(headers)

# ── PREVIEW (sem gravar) ─────────────────────────────────────────────────────
@app.post("/importar_movimentacao/preview")
async def importar_movimentacao_preview(
    request: Request,
    file: UploadFile = File(...),
    mapping: str = Form(default="{}"),
    delimiter: str = Form(default="auto"),
    encoding: str = Form(default="utf-8-sig"),
    sample_rows: int = Form(default=5),
    max_rows: int = Form(default=200000),
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Envie um arquivo .csv")

    raw = await file.read()
    text = raw.decode(encoding, errors="replace")

    if delimiter == "auto":
        try:
            use_delim = csv.Sniffer().sniff(text[:2000], delimiters=[",", ";", "|", "\t"]).delimiter
        except Exception:
            use_delim = ";"
    else:
        use_delim = {";": ";", ",": ",", "|": "|", "\\t": "\t"}.get(delimiter, delimiter)

    buf = io.StringIO(text)
    reader = csv.reader(buf, delimiter=use_delim)

    try:
        header = next(reader)
    except StopIteration:
        raise HTTPException(status_code=400, detail="CSV vazio")

    header_norm = [im_normalize_header(h) for h in header]
    header_map = {h: i for i, h in enumerate(header_norm)}

    try:
        mapping_dict = json.loads(mapping or "{}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"JSON inválido em 'mapping': {e}")

    auto_aliases = {
        "contrato": ["contrato", "contrato_n", "numero_contrato", "n_contrato", "contrato n"],
        "item": ["item", "produto", "descrição", "descricao", "descricao_item", "descrição do produto", "descricao_produto"],
        "descricao_produto": ["descrição do produto", "descricao do produto", "descricao_produto", "descrição_produto", "produto", "item"],
        "data_mov": ["data_mov", "data", "data movimento", "data_movimento", "data_envio"],
        "tipo": ["tipo", "tipo_mov", "movimento", "envio / retorno", "envio_retorno"],
        "qtd": ["qtd", "quantidade", "qtde"],
        "valor_mensal": ["valor_mensal", "valor mensal", "val cobrado", "vl_mensal", "mensalidade"],
        "meses_restantes": ["meses_restantes", "meses restantes", "meses", "m_restantes"],
        "cliente": ["cliente", "razao social", "razão social", "nome fantasia"],
        "observacao": ["observacao", "observação", "obs"],
        "ativo": ["ativo", "nr_ativo", "num_ativo"],
        "cod_cli": ["cod_cli", "codigo_cliente", "cod cli", "id_cliente"],
        "serial": ["serial", "n_serie", "numero_serie", "nº de série", "no_serie"],
        "cod_pro": ["cod_pro", "cod pro", "codigo_produto", "código do produto", "codigo do produto"],
    }

    def resolve_col(canon: str):
        if canon in mapping_dict:
            target = im_normalize_header(str(mapping_dict[canon]))
            return header_map.get(target)
        for alias in auto_aliases.get(canon, []):
            idx = header_map.get(im_normalize_header(alias))
            if idx is not None:
                return idx
        return header_map.get(im_normalize_header(canon))

    required_list = DEFAULT_REQUIRED[:]
    resolved_idx = {}
    for canon in set(required_list + OPTIONAL_FIELDS):
        idx = resolve_col(canon)
        if idx is not None:
            resolved_idx[canon] = idx

    missing_required = [c for c in required_list if c not in resolved_idx]
    if missing_required:
        return JSONResponse(
            {
                "ok": False,
                "mensagem": "Colunas obrigatórias ausentes no CSV.",
                "delimiter_detectado": use_delim,
                "header_original": header,
                "header_normalizado": header_norm,
                "faltando": missing_required,
                "dica": "Ajuste o mapeamento e tente novamente.",
            }
        )

    total = vazias = erros_linhas = 0
    MAX_ERRORS = 100
    amostras, contratos, clientes = [], set(), {}
    data_min = data_max = None
    soma_qtd = soma_valor_mensal = 0.0

    for row in reader:
        if total >= int(max_rows):
            break
        total += 1
        if not any(str(x).strip() for x in row):
            vazias += 1
            continue

        def get(c):
            idx = resolved_idx.get(c)
            return str(row[idx]).strip() if (idx is not None and idx < len(row)) else ""

        try:
            contrato = get("contrato")
            descricao = get("descricao_produto") or get("item")
            item = get("item")
            data_mov = im_parse_date(get("data_mov"))
            tipo = (get("tipo") or "").upper().strip()
            qtd = im_to_float(get("qtd"))
            v_mensal = im_to_float(get("valor_mensal"))
            meses_rest = im_to_int(get("meses_restantes"))
            cliente = get("cliente") if "cliente" in resolved_idx else ""
            ativo = get("ativo") if "ativo" in resolved_idx else ""
            cod_cli = get("cod_cli") if "cod_cli" in resolved_idx else ""
            serial = get("serial") if "serial" in resolved_idx else ""
            cod_pro = get("cod_pro") if "cod_pro" in resolved_idx else ""

            if contrato:
                contratos.add(contrato)
            if cliente:
                clientes[cliente] = clientes.get(cliente, 0) + 1
            if data_mov:
                if data_min is None or data_mov < data_min:
                    data_min = data_mov
                if data_max is None or data_mov > data_max:
                    data_max = data_mov

            soma_qtd += qtd
            soma_valor_mensal += v_mensal
            if len(amostras) < int(sample_rows):
                amostras.append(
                    {
                        "contrato": contrato,
                        "item": item,
                        "descricao_produto": descricao,
                        "serial": serial,
                        "cod_pro": cod_pro,
                        "data_mov": data_mov,
                        "tipo": tipo,
                        "qtd": qtd,
                        "valor_mensal": v_mensal,
                        "meses_restantes": meses_rest,
                        "cliente": cliente or None,
                        "ativo": ativo or None,
                        "cod_cli": cod_cli or None,
                    }
                )
        except Exception:
            erros_linhas += 1
            if erros_linhas <= MAX_ERRORS:
                pass

    top_clientes = sorted(clientes.items(), key=lambda x: (-x[1], x[0]))[:10]
    top_clientes = [{"cliente": k, "linhas": v} for k, v in top_clientes]

    return {
        "ok": True,
        "mensagem": "Preview gerado com sucesso. Nada foi gravado no banco.",
        "arquivo": file.filename,
        "delimiter_detectado": use_delim,
        "header_original": header,
        "header_normalizado": header_norm,
        "mapeamento_resolvido": {k: header[resolved_idx[k]] for k in resolved_idx},
        "obrigatorias_atendidas": [c for c in required_list],
        "faltando": [],
        "total_linhas_lidas": total,
        "linhas_vazias": vazias,
        "erros_linhas": erros_linhas,
        "estatisticas": {
            "contratos_unicos": len(contratos),
            "periodo_data_mov": {"de": data_min, "ate": data_max},
            "soma_qtd": soma_qtd,
            "soma_valor_mensal": soma_valor_mensal,
            "top_clientes_por_linhas": top_clientes,
        },
        "amostra": amostras,
    }

# ── PRÉ-IMPORTAÇÃO (salva lote .json) + checagens ────────────────────────────
@app.post("/importar_movimentacao/preimport")
async def importar_movimentacao_preimport(
    request: Request,
    file: UploadFile = File(...),
    mapping: str = Form(default="{}"),
    delimiter: str = Form(default="auto"),
    encoding: str = Form(default="utf-8-sig"),
    db: Session = Depends(get_db),
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Envie um arquivo .csv")

    raw = await file.read()
    text = raw.decode(encoding, errors="replace")

    if delimiter == "auto":
        try:
            use_delim = csv.Sniffer().sniff(text[:2000], delimiters=[",", ";", "|", "\t"]).delimiter
        except Exception:
            use_delim = ";"
    else:
        use_delim = {";": ";", ",": ",", "|": "|", "\\t": "\t"}.get(delimiter, delimiter)

    buf = io.StringIO(text)
    reader = csv.reader(buf, delimiter=use_delim)
    header = next(reader)
    header_norm = [im_normalize_header(h) for h in header]
    header_map = {h: i for i, h in enumerate(header_norm)}
    mapping_dict = json.loads(mapping or "{}")

    def get_idx(canon, aliases):
        if canon in mapping_dict:
            return header_map.get(im_normalize_header(str(mapping_dict[canon])))
        for alias in aliases:
            idx = header_map.get(im_normalize_header(alias))
            if idx is not None:
                return idx
        return header_map.get(im_normalize_header(canon))

    aliases = {
        "contrato": ["contrato", "contrato n", "numero_contrato", "contrato_n", "contrato_num"],
        "item": ["item", "produto", "descrição", "descricao", "descrição do produto", "descricao_produto"],
        "descricao_produto": ["descrição do produto", "descricao do produto", "descricao_produto", "descrição_produto", "produto", "item"],
        "data_mov": ["data movimento", "data_mov", "data", "data_envio"],
        "tipo": ["tipo", "envio / retorno", "movimento"],
        "qtd": ["quantidade", "qtd", "qtde"],
        "valor_mensal": ["valor mensal", "val cobrado", "mensalidade", "vl_mensal"],
        "meses_restantes": ["meses restantes", "meses", "m_restantes"],
        "cliente": ["cliente", "razão social", "razao social", "nome fantasia"],
        "observacao": ["observacao", "observação", "obs"],
        "ativo": ["ativo", "nr_ativo", "num_ativo"],
        "cod_cli": ["cod_cli", "codigo_cliente", "cod cli", "id_cliente"],
        "serial": ["serial", "n_serie", "numero_serie", "nº de série", "no_serie"],
        "cod_pro": ["cod_pro", "cod pro", "codigo_produto", "código do produto", "codigo do produto"],
    }
    idxs = {k: get_idx(k, v) for k, v in aliases.items()}

    missing = [k for k in ["contrato", "item", "data_mov", "tipo"] if idxs.get(k) is None]
    if missing:
        return {"ok": False, "mensagem": f"Colunas obrigatórias ausentes: {missing}"}

    cab_field = get_contract_field(ContratoCabecalho)
    rows = []
    for row in reader:
        if not any(str(x).strip() for x in row):
            continue

        def get(c):
            i = idxs.get(c)
            return str(row[i]).strip() if (i is not None and i < len(row)) else ""

        descricao = get("descricao_produto") or get("item")
        rows.append(
            {
                "contrato": get("contrato"),
                "item": get("item"),
                "descricao_produto": descricao,
                "serial": get("serial"),
                "cod_pro": get("cod_pro"),
                "data_mov": im_parse_date(get("data_mov")),
                "tipo": (get("tipo") or "").upper().strip(),
                "qtd": im_to_float(get("qtd")),
                "valor_mensal": im_to_float(get("valor_mensal")),
                "meses_restantes": im_to_int(get("meses_restantes")),
                "cliente": get("cliente"),
                "observacao": get("observacao"),
                "ativo": get("ativo"),
                "cod_cli": get("cod_cli"),
            }
        )

    it_field = get_contract_field(Contrato) or "contrato_n"

    existe, nao_existe, ja_importadas = 0, 0, 0
    sem_cab_count = 0
    sem_cab_contratos = set()

    for r in rows:
        contrato_numero = r.get("contrato") or ""
        if not contrato_numero or not (r.get("descricao_produto") or r.get("item")):
            nao_existe += 1
            continue

        # cabeçalho
        cab_ok = False
        if cab_field:
            cab_ok = (
                db.query(ContratoCabecalho)
                .filter(getattr(ContratoCabecalho, cab_field) == contrato_numero)
                .first()
                is not None
            )
        if not cab_ok:
            sem_cab_count += 1
            sem_cab_contratos.add(contrato_numero)

        match, _how = find_item_by_heuristics(db, it_field, contrato_numero=contrato_numero, r=r)
        if match:
            existe += 1
        else:
            nao_existe += 1

        if ImportMovimentoRegistro is not None:
            key_hash = make_import_key(
                contrato_numero, r.get("ativo", ""), r.get("cod_cli", ""), r.get("tipo", ""), r.get("data_mov", "")
            )
            if (
                db.query(ImportMovimentoRegistro)
                .filter(ImportMovimentoRegistro.unique_hash == key_hash)
                .first()
            ):
                ja_importadas += 1

    lote_id = str(uuid4())
    lote_path = os.path.join(RUNTIME_DIR, f"preimport_{lote_id}.json")
    with open(lote_path, "w", encoding="utf-8") as f:
        json.dump({"rows": rows}, f, ensure_ascii=False)
    request.session["mov_preimport_file"] = lote_path

    tipos = {}
    for r in rows:
        k = r["tipo"] or "—"
        tipos[k] = tipos.get(k, 0) + 1

    return {
        "ok": True,
        "mensagem": "Pré-importação concluída. Lote salvo no servidor.",
        "lote_id": lote_id,
        "totais": {
            "linhas": len(rows),
            "itens_existentes_na_base": existe,
            "itens_nao_encontrados": nao_existe,
            "por_tipo": tipos,
            "linhas_ja_importadas": ja_importadas,
            "cabecalhos_inexistentes": sem_cab_count,
        },
        "contratos_sem_cabecalho": sorted(list(sem_cab_contratos)),
        "proxima_etapa": "Cadastre os cabeçalhos faltantes e depois clique em 'Importar agora' para aplicar o lote.",
    }

# ── COMMIT (aplica lote) ─────────────────────────────────────────────────────
@app.post("/importar_movimentacao/commit")
async def importar_movimentacao_commit(request: Request, db: Session = Depends(get_db)):
    lote_path = request.session.get("mov_preimport_file")
    if not lote_path or not os.path.exists(lote_path):
        return JSONResponse(
            {"ok": False, "mensagem": "Nenhum lote pré-importado encontrado. Faça a pré-importação primeiro."},
            status_code=400,
        )

    with open(lote_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    rows = payload.get("rows", [])

    cab_field = get_contract_field(ContratoCabecalho)
    it_field = get_contract_field(Contrato)
    if not cab_field or not it_field:
        return JSONResponse(
            {"ok": False, "mensagem": "Modelos não possuem coluna de número de contrato reconhecida."}, status_code=500
        )

    inserted = updated = deleted = skipped = duplicated = 0
    cabecalho_inexistente = 0
    detalhes = []
    faltantes = set()

    try:
        for r in rows:
            contrato_numero = (r.get("contrato") or "").strip()
            desc = (r.get("descricao_produto") or r.get("item") or "").strip()
            tipo = (r.get("tipo") or "").upper().strip()
            data_mov_iso = r.get("data_mov") or ""
            ativo = (r.get("ativo") or "").strip()
            cod_cli = (r.get("cod_cli") or "").strip()
            serial = (r.get("serial") or "").strip()
            cod_pro = (r.get("cod_pro") or "").strip()

            if not contrato_numero or not desc:
                skipped += 1
                detalhes.append({"status": "IGNORADO", **r})
                continue

            cab = (
                db.query(ContratoCabecalho)
                .filter(getattr(ContratoCabecalho, cab_field) == contrato_numero)
                .first()
            )
            if not cab:
                cabecalho_inexistente += 1
                faltantes.add(contrato_numero)
                detalhes.append(
                    {
                        "status": "CABECALHO_INEXISTENTE",
                        "mensagem": f"Contrato {contrato_numero} não encontrado no cabeçalho. Cadastre-o e reimporte.",
                        **r,
                    }
                )
                continue
            prazo_cab = cab.prazo_contratual

            # hash do movimento (se modelo de import exists)
            ja_logado = False
            key_hash = make_import_key(contrato_numero, ativo, cod_cli, tipo, data_mov_iso)
            if ImportMovimentoRegistro is not None:
                ja_logado = (
                    db.query(ImportMovimentoRegistro)
                    .filter(ImportMovimentoRegistro.unique_hash == key_hash)
                    .first()
                    is not None
                )

            # RETORNO
            if tipo == "RETORNO":
                alvo, matched_by = find_single_for_return(db, it_field, contrato_numero, r)
                if alvo:
                    add_contrato_log(
                        db,
                        alvo,
                        "RETORNO_REMOVIDO",
                        {
                            "contrato": contrato_numero,
                            "ativo": ativo,
                            "serial": serial,
                            "cod_pro": cod_pro,
                            "cod_cli": cod_cli,
                            "data_mov": data_mov_iso,
                            "matched_by": matched_by,
                        },
                    )
                    db.delete(alvo)
                    deleted += 1
                    detalhes.append(
                        {"status": "REMOVIDO" + ("(retry-dup)" if ja_logado else ""), "matched_by": matched_by, "deleted_count": 1, **r}
                    )
                else:
                    if ja_logado:
                        detalhes.append({"status": "JA_REMOVIDO", "matched_by": "idempotent", "deleted_count": 0, **r})
                    else:
                        skipped += 1
                        detalhes.append({"status": "IGNORADO", "matched_by": "nao_encontrado", "deleted_count": 0, **r})

                if ImportMovimentoRegistro is not None and not ja_logado:
                    db.add(
                        ImportMovimentoRegistro(
                            contrato_n=contrato_numero,
                            ativo=ativo,
                            cod_cli=cod_cli,
                            tipo=tipo,
                            data_mov=(data_mov_iso or ""),
                            unique_hash=key_hash,
                        )
                    )
                continue

            # DUPLICADOS em ENVIO/TROCA
            if ja_logado and tipo in ("ENVIO", "TROCA"):
                duplicated += 1
                detalhes.append({"status": "DUPLICADO", **r})
                continue

            existente, matched_by = find_item_by_heuristics(db, it_field, contrato_numero=contrato_numero, r=r)

            # TROCA -> remove existente e trata como ENVIO
            if tipo == "TROCA":
                if existente:
                    add_contrato_log(
                        db,
                        existente,
                        "TROCA_REMOVIDO",
                        {
                            "contrato": contrato_numero,
                            "ativo": existente.ativo,
                            "serial": existente.serial,
                            "cod_pro": existente.cod_pro,
                            "cod_cli": existente.cod_cli,
                            "data_mov": data_mov_iso,
                            "matched_by": matched_by,
                        },
                    )
                    db.delete(existente)
                    deleted += 1
                    detalhes.append({"status": "REMOVIDO(TROCA)", "matched_by": matched_by, **r})
                existente = None
                tipo = "ENVIO"

            # ENVIO (insere/atualiza)
            if tipo == "ENVIO":
                if existente:
                    if serial:
                        existente.serial = serial
                    if cod_pro:
                        existente.cod_pro = cod_pro
                    if desc:
                        existente.descricao_produto = desc
                    if r.get("valor_mensal") is not None:
                        existente.valor_mensal = float(r.get("valor_mensal") or 0.0)
                    if r.get("meses_restantes") is not None:
                        existente.meses_restantes = int(r.get("meses_restantes") or 0)
                    if data_mov_iso:
                        existente.data_envio = parse_data(data_mov_iso)
                    if cod_cli:
                        existente.cod_cli = cod_cli
                    if ativo:
                        existente.ativo = ativo
                    if prazo_cab:
                        existente.periodo_contratual = int(prazo_cab)

                    recomputa_meses_restantes_if_needed(existente, prazo_cab)
                    calcular_valores_no_obj(existente)

                    add_contrato_log(
                        db,
                        existente,
                        "ENVIO_ATUALIZADO",
                        {
                            "contrato": contrato_numero,
                            "descricao": existente.descricao_produto,
                            "ativo": existente.ativo,
                            "serial": existente.serial,
                            "cod_pro": existente.cod_pro,
                            "cod_cli": existente.cod_cli,
                            "data_mov": data_mov_iso,
                            "valor_mensal": existente.valor_mensal,
                            "meses_restantes": existente.meses_restantes,
                            "periodo_contratual": existente.periodo_contratual,
                        },
                    )

                    updated += 1
                    detalhes.append({"status": "ATUALIZADO", "matched_by": matched_by, **r})
                else:
                    novo = Contrato(
                        ativo=ativo or "",
                        serial=serial or "",
                        cod_pro=cod_pro or "",
                        descricao_produto=desc,
                        cod_cli=cod_cli or "",
                        nome_cli=(r.get("cliente") or "").strip(),
                        data_envio=parse_data(data_mov_iso or ""),
                        valor_mensal=float(r.get("valor_mensal") or 0.0),
                        periodo_contratual=int(prazo_cab or (r.get("meses_restantes") or 0)),
                        meses_restantes=int(r.get("meses_restantes") or 0),
                    )
                    setattr(novo, it_field, contrato_numero)

                    recomputa_meses_restantes_if_needed(novo, prazo_cab)
                    calcular_valores_no_obj(novo)

                    db.add(novo)
                    db.flush()  # garante novo.id no log
                    add_contrato_log(
                        db,
                        novo,
                        "ENVIO_INSERIDO",
                        {
                            "contrato": contrato_numero,
                            "descricao": desc,
                            "ativo": ativo,
                            "serial": serial,
                            "cod_pro": cod_pro,
                            "cod_cli": cod_cli,
                            "data_mov": data_mov_iso,
                            "valor_mensal": novo.valor_mensal,
                            "meses_restantes": novo.meses_restantes,
                            "periodo_contratual": novo.periodo_contratual,
                        },
                    )

                    inserted += 1
                    detalhes.append({"status": "INSERIDO", "matched_by": matched_by, **r})

                if ImportMovimentoRegistro is not None:
                    db.add(
                        ImportMovimentoRegistro(
                            contrato_n=contrato_numero,
                            ativo=ativo,
                            cod_cli=cod_cli,
                            tipo=tipo,
                            data_mov=(data_mov_iso or ""),
                            unique_hash=key_hash,
                        )
                    )
                continue

            # tipo desconhecido
            skipped += 1
            detalhes.append({"status": "IGNORADO", **r})

        db.commit()
    except Exception as e:
        db.rollback()
        return JSONResponse({"ok": False, "mensagem": f"Falha ao importar: {str(e)}"}, status_code=500)
    finally:
        try:
            os.remove(lote_path)
        except Exception:
            pass
        request.session.pop("mov_preimport_file", None)

    ultima = {
        "executed_at": datetime.now().isoformat(),
        "resultado": {
            "inseridos": inserted,
            "atualizados": updated,
            "removidos": deleted,
            "ignorados": skipped,
            "duplicados": duplicated,
            "cabecalhos_inexistentes": cabecalho_inexistente,
        },
        "contratos_sem_cabecalho": sorted(list(faltantes)),
        "itens": detalhes,
    }
    try:
        with open(LAST_IMPORT_FILE, "w", encoding="utf-8") as f:
            json.dump(ultima, f, ensure_ascii=False, indent=2)
    except Exception as e:
        ultima["warning"] = f"Não foi possível salvar última importação: {e}"

    return {"ok": True, "mensagem": "Importação concluída.", **ultima}

# ── VISÕES: Última importação ────────────────────────────────────────────────
@app.get("/api/ultima_importacao")
async def api_ultima_importacao():
    if not os.path.exists(LAST_IMPORT_FILE):
        return JSONResponse({"ok": False, "mensagem": "Ainda não há dados de última importação."}, status_code=404)
    with open(LAST_IMPORT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data

@app.get("/ultima_importacao", response_class=HTMLResponse)
async def ultima_importacao(request: Request):
    data = None
    if os.path.exists(LAST_IMPORT_FILE):
        with open(LAST_IMPORT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    try:
        return templates.TemplateResponse("ultima_importacao.html", {"request": request, "dados": data})
    except TemplateNotFound:
        if not data:
            return HTMLResponse("<div style='padding:16px;font-family:system-ui'>Ainda não há dados de última importação.</div>")

        res = data.get("resultado", {})
        order = list(res.keys())
        color_map = {
            "inseridos": "bg-success",
            "atualizados": "bg-primary",
            "removidos": "bg-danger",
            "ignorados": "bg-secondary",
            "duplicados": "bg-dark",
            "cabecalhos_inexistentes": "bg-warning",
        }
        badges = "".join(
            f"<div class='col-auto'><span class='badge {color_map.get(k,'bg-info')}'>{k.replace('_',' ').title()}: {res.get(k,0)}</span></div>"
            for k in order
        )

        # evita f-string com aspas conflitantes
        missing_html = ""
        if data.get("contratos_sem_cabecalho"):
            missing_html = (
                "<div class='alert alert-warning p-2'>Contratos sem cabeçalho: "
                + ", ".join(data.get("contratos_sem_cabecalho", []))
                + "</div>"
            )

        head = """
        <table class="table table-sm table-striped">
        <thead><tr>
          <th>Status</th><th>Contrato</th><th>Item</th><th>Descrição</th><th>Serial</th><th>Cod. Pro</th>
          <th>Data mov.</th><th>Tipo</th><th>Qtd</th><th>Valor mensal</th><th>Meses rest.</th>
          <th>Cliente</th><th>Ativo</th><th>Cod. Cliente</th><th>Obs.</th>
        </tr></thead><tbody>
        """
        rows = []
        for it in data.get("itens", []):
            rows.append(
                "<tr>"
                f"<td>{it.get('status','')}</td>"
                f"<td>{it.get('contrato','')}</td>"
                f"<td>{it.get('item','')}</td>"
                f"<td>{it.get('descricao_produto','')}</td>"
                f"<td>{it.get('serial','')}</td>"
                f"<td>{it.get('cod_pro','')}</td>"
                f"<td>{it.get('data_mov','')}</td>"
                f"<td>{it.get('tipo','')}</td>"
                f"<td class='text-end'>{it.get('qtd','')}</td>"
                f"<td class='text-end'>{it.get('valor_mensal','')}</td>"
                f"<td class='text-end'>{it.get('meses_restantes','')}</td>"
                f"<td>{it.get('cliente','')}</td>"
                f"<td>{it.get('ativo','')}</td>"
                f"<td>{it.get('cod_cli','')}</td>"
                f"<td>{it.get('observacao','')}</td>"
                "</tr>"
            )
        html = f"""
        <!DOCTYPE html><html><head>
          <meta charset="utf-8"><title>Última Importação</title>
          <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
        </head><body class="container py-4">
          <h3>📦 Última Importação</h3>
          <div class="mb-2"><code>{data.get('executed_at','')}</code></div>
          <div class="row g-2 mb-3">{badges}</div>
          {missing_html}
          <div class="table-responsive">{head}{''.join(rows)}</tbody></table></div>
          <div class="mt-2 d-flex gap-2">
            <a class="btn btn-outline-secondary btn-sm" href="/api/ultima_importacao" target="_blank">Ver JSON</a>
          </div>
        </body></html>"""
        return HTMLResponse(html)

# ── Fluxo legado validar/confirmar — com logs ────────────────────────────────
@app.post("/validar_mapeamento", response_class=HTMLResponse)
async def validar_mapeamento(request: Request):
    form = await request.form()
    mapeamento = dict(form)
    temp_file = request.session.get("temp_file")

    preview = []
    if temp_file and os.path.exists(temp_file):
        with open(temp_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                preview.append({campo: row.get(origem, "") for campo, origem in mapeamento.items()})
    request.session["mapeamento"] = mapeamento
    return templates.TemplateResponse("validar_importacao.html", {"request": request, "preview": preview})

@app.post("/confirmar_importacao")
async def confirmar_importacao(request: Request, db: Session = Depends(get_db)):
    temp_file = request.session.get("temp_file")
    mapeamento = request.session.get("mapeamento")
    if not temp_file or not os.path.exists(temp_file) or not mapeamento:
        return RedirectResponse("/", status_code=303)

    cab_field = get_contract_field(ContratoCabecalho)
    it_field = get_contract_field(Contrato)

    with open(temp_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dados = {campo: row.get(origem, "") for campo, origem in mapeamento.items()}
            numero = (dados.get("contrato_n") or dados.get("contrato_num") or "").strip()
            tp = (dados.get("tp_transacao") or "").upper()

            if not cab_field or not it_field:
                continue
            cabecalho = (
                db.query(ContratoCabecalho)
                .filter(getattr(ContratoCabecalho, cab_field) == numero)
                .first()
            )
            if not cabecalho:
                continue

            if tp == "RETORNO":
                alvo = (
                    db.query(Contrato)
                    .filter(
                        getattr(Contrato, it_field) == numero,
                        Contrato.cod_cli == dados.get("cod_cli"),
                        Contrato.ativo == dados.get("ativo"),
                    )
                    .first()
                )
                if alvo:
                    add_contrato_log(
                        db,
                        alvo,
                        "RETORNO_REMOVIDO(LEGADO)",
                        {
                            "contrato": numero,
                            "ativo": alvo.ativo,
                            "serial": getattr(alvo, "serial", None),
                            "cod_pro": getattr(alvo, "cod_pro", None),
                            "cod_cli": getattr(alvo, "cod_cli", None),
                        },
                    )
                    db.delete(alvo)
            elif tp == "ENVIO":
                novo = Contrato(
                    **{k: v for k, v in dados.items() if k in Contrato.__table__.columns.keys()},
                    periodo_contratual=cabecalho.prazo_contratual,
                )
                setattr(novo, it_field, numero)
                if not getattr(novo, "meses_restantes", None) or int(getattr(novo, "meses_restantes") or 0) == 0:
                    recomputa_meses_restantes_if_needed(novo, cabecalho.prazo_contratual)
                calcular_valores_no_obj(novo)
                db.add(novo)
                db.flush()
                add_contrato_log(
                    db,
                    novo,
                    "ENVIO_INSERIDO(LEGADO)",
                    {
                        "contrato": numero,
                        "descricao": getattr(novo, "descricao_produto", None),
                        "ativo": getattr(novo, "ativo", None),
                        "serial": getattr(novo, "serial", None),
                        "cod_pro": getattr(novo, "cod_pro", None),
                        "cod_cli": getattr(novo, "cod_cli", None),
                        "valor_mensal": getattr(novo, "valor_mensal", None),
                        "meses_restantes": getattr(novo, "meses_restantes", None),
                        "periodo_contratual": getattr(novo, "periodo_contratual", None),
                    },
                )
            elif tp == "TROCA":
                existente = (
                    db.query(Contrato)
                    .filter(
                        getattr(Contrato, it_field) == numero,
                        Contrato.cod_cli == dados.get("cod_cli"),
                        Contrato.ativo == dados.get("ativo"),
                    )
                    .first()
                )
                if existente:
                    keep = existente.meses_restantes
                    add_contrato_log(
                        db,
                        existente,
                        "TROCA_REMOVIDO(LEGADO)",
                        {
                            "contrato": numero,
                            "ativo": existente.ativo,
                            "serial": getattr(existente, "serial", None),
                            "cod_pro": getattr(existente, "cod_pro", None),
                            "cod_cli": getattr(existente, "cod_cli", None),
                        },
                    )
                    db.delete(existente)
                    novo = Contrato(
                        **{k: v for k, v in dados.items() if k in Contrato.__table__.columns.keys()},
                        meses_restantes=keep,
                        periodo_contratual=cabecalho.prazo_contratual,
                    )
                    setattr(novo, it_field, numero)
                    calcular_valores_no_obj(novo)
                    db.add(novo)
                    db.flush()
                    add_contrato_log(
                        db,
                        novo,
                        "TROCA_INSERIDO(LEGADO)",
                        {
                            "contrato": numero,
                            "descricao": getattr(novo, "descricao_produto", None),
                            "ativo": getattr(novo, "ativo", None),
                            "serial": getattr(novo, "serial", None),
                            "cod_pro": getattr(novo, "cod_pro", None),
                            "cod_cli": getattr(novo, "cod_cli", None),
                            "valor_mensal": getattr(novo, "valor_mensal", None),
                            "meses_restantes": getattr(novo, "meses_restantes", None),
                            "periodo_contratual": getattr(novo, "periodo_contratual", None),
                        },
                    )
    db.commit()
    try:
        os.remove(temp_file)
    except Exception:
        pass
    return RedirectResponse("/", status_code=303)

# ── LOG DE CONTRATOS (por item) ──────────────────────────────────────────────
@app.get("/log_contrato/{id}", response_class=HTMLResponse)
async def log_contrato(id: int, request: Request, db: Session = Depends(get_db)):
    if ContratoLog is None:
        return HTMLResponse("<div style='padding:16px;font-family:system-ui'>Logs indisponíveis (ContratoLog não encontrado no schema).</div>")
    logs = db.query(ContratoLog).filter_by(contrato_id=id).all()
    return templates.TemplateResponse("log_contrato.html", {"request": request, "logs": logs})

# ── LOGS DE CABEÇALHO (arquivo) ──────────────────────────────────────────────
@app.get("/api/logs_cabecalhos")
async def api_logs_cabecalhos():
    out = []
    if os.path.exists(CABECALHO_LOG_FILE):
        with open(CABECALHO_LOG_FILE, "r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    pass
    return {"total": len(out), "registros": out[-200:]}

@app.get("/logs_cabecalhos", response_class=HTMLResponse)
async def logs_cabecalhos(request: Request):
    data = (await api_logs_cabecalhos())["registros"]
    try:
        return templates.TemplateResponse("logs_cabecalhos.html", {"request": request, "registros": data})
    except TemplateNotFound:
        rows = []
        for it in data:
            rows.append(
                "<tr>"
                f"<td><code>{it.get('ts','')}</code></td>"
                f"<td>{it.get('acao','')}</td>"
                f"<td>{it.get('contrato','')}</td>"
                f"<td>{it.get('nome_cliente','')}</td>"
                f"<td>{it.get('cnpj','')}</td>"
                f"<td class='text-end'>{it.get('prazo_contratual','')}</td>"
                f"<td>{it.get('indice_reajuste','')}</td>"
                f"<td>{it.get('vendedor','')}</td>"
                f"<td>{it.get('usuario','')}</td>"
                "</tr>"
            )
        html = f"""
        <!DOCTYPE html><html><head>
          <meta charset="utf-8"><title>Logs de Cabeçalho</title>
          <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
        </head><body class="container py-4">
          <h3>🧾 Logs de Cabeçalho</h3>
          <div class="table-responsive">
            <table class="table table-sm table-striped">
              <thead><tr>
                <th>Data</th><th>Ação</th><th>Contrato</th><th>Cliente</th><th>CNPJ</th>
                <th>Prazo</th><th>Índice</th><th>Vendedor</th><th>Usuário</th>
              </tr></thead>
              <tbody>{''.join(rows)}</tbody>
            </table>
          </div>
          <div class="mt-2"><a href="/api/logs_cabecalhos" target="_blank" class="btn btn-outline-secondary btn-sm">Ver JSON</a></div>
        </body></html>"""
        return HTMLResponse(html)

# ── Runtime safety: força a ordem correta de middlewares ─────────────────────
try:
    _ums = list(app.user_middleware)
    _sm = next((m for m in _ums if getattr(m.cls, "__name__", "") == "SessionMiddleware"), None)
    _am = next((m for m in _ums if getattr(m.cls, "__name__", "") == "AuthRequiredMiddleware"), None)
    if _sm and _am:
        app.user_middleware = [m for m in _ums if m not in (_sm, _am)] + [_sm, _am]
        app.middleware_stack = app.build_middleware_stack()
        print("Adjusted middleware order at runtime:", [m.cls.__name__ for m in app.user_middleware])
        print("MIDDLEWARES ORDER (após ajuste):", [m.cls.__name__ for m in app.user_middleware])
except Exception as _e:
    print("Middleware order fix skipped:", _e)

# ── Admin de usuários (novo) ─────────────────────────────────────────────────
try:
    from routers import admin_users
    app.include_router(admin_users.router, prefix="/admin", tags=["Usuários"])
except Exception as e:
    print("[WARN] Router admin_users indisponível:", e)
