# main.py – Versão 2.8.8 (08/08/2025)
# Correções:
# - Importação correta do campo "descricao" (CSV: "descrição do produto");
# - Gravação do campo "periodo_contratual" somente na importação de contratos;
# - Garantia de consistência entre dados do modelo e entrada;
# - Manutenção do controle de versão

from fastapi import FastAPI, Request, Form, UploadFile, File, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy.orm import Session
from datetime import datetime
from uuid import uuid4
import shutil
import os
import csv

from database import SessionLocal, engine
from models import Base, ContratoCabecalho, Contrato, ContratoLog

Base.metadata.create_all(bind=engine)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="secret")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ----------------------
# UTILITÁRIOS
# ----------------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def calcular_valores(contrato):
    contrato.valor_global_contrato = contrato.valor_mensal * contrato.meses_restantes
    contrato.valor_presente_contrato = contrato.valor_global_contrato * 0.9
    return contrato

def parse_float(val):
    try:
        return float(str(val).replace(",", ".").strip())
    except:
        return 0.0

def parse_data(valor):
    try:
        if "/" in valor:
            return datetime.strptime(valor.strip(), "%d/%m/%Y")
        return datetime.strptime(valor.strip(), "%Y-%m-%d")
    except:
        return datetime.strptime("1900-01-01", "%Y-%m-%d")

# ----------------------
# ROTAS HTML
# ----------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "ano": datetime.now().year})

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/dashboard_data")
async def dashboard_data(db: Session = Depends(get_db)):
    contratos = db.query(Contrato).all()
    por_cliente = {}
    for contrato in contratos:
        cliente = contrato.nome_cli
        por_cliente[cliente] = por_cliente.get(cliente, 0) + contrato.valor_global_contrato
    return {"por_cliente": por_cliente}

@app.get("/contratos_html", response_class=HTMLResponse)
async def contratos_html(request: Request, db: Session = Depends(get_db)):
    contratos = db.query(Contrato).all()
    return templates.TemplateResponse("contratos.html", {"request": request, "contratos": contratos})

# ----------------------
# CADASTRO CABEÇALHO MANUAL
# ----------------------

@app.get("/cadastrar", response_class=HTMLResponse)
async def show_form(request: Request, db: Session = Depends(get_db)):
    contratos = db.query(ContratoCabecalho).all()
    return templates.TemplateResponse("cadastrar.html", {"request": request, "contratos": contratos})

@app.post("/cadastrar")
async def cadastrar(
    request: Request,
    nome_cliente: str = Form(...),
    cnpj: str = Form(...),
    contrato_n: str = Form(...),
    prazo_contratual: int = Form(...),
    indice_reajuste: str = Form(...),
    vendedor: str = Form(...),
    db: Session = Depends(get_db)
):
    novo = ContratoCabecalho(
        nome_cliente=nome_cliente,
        cnpj=cnpj,
        contrato_n=contrato_n,
        prazo_contratual=prazo_contratual,
        indice_reajuste=indice_reajuste,
        vendedor=vendedor
    )
    db.add(novo)
    db.commit()
    return RedirectResponse("/cadastrar", status_code=302)

# ----------------------
# IMPORTAÇÃO DE CONTRATOS
# ----------------------

@app.get("/upload", response_class=HTMLResponse)
async def upload_form(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request})

@app.post("/upload")
async def upload_csv(request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)):
    contents = await file.read()
    decoded = contents.decode("utf-8").splitlines()
    reader = csv.DictReader(decoded)

    for row in reader:
        try:
            meses_restantes = int(parse_float(row.get("periodo contratual", "0")))
            contrato = Contrato(
                ativo=row.get("ativo", "").strip(),
                serial=row.get("serial", "").strip(),
                cod_pro=row.get("cod pro", "").strip(),
                descricao_produto=row.get("descrição do produto", "").strip(),  # <-- Correção
                cod_cli=row.get("cod cli", "").strip(),
                nome_cli=row.get("nome cli", "").strip(),
                data_envio=parse_data(row.get("data de envio", "")),
                contrato_n=row.get("contrato n", "").strip(),
                valor_mensal=parse_float(row.get("valor mensal", "0")),
                meses_restantes=meses_restantes,
                periodo_contratual=meses_restantes
            )
            calcular_valores(contrato)
            db.add(contrato)
        except Exception as e:
            print("Erro ao importar linha:", row)
            print("Detalhe:", str(e))
            continue
    db.commit()
    return RedirectResponse("/", status_code=303)

# ----------------------
# IMPORTAÇÃO DE MOVIMENTAÇÃO DE ITENS
# ----------------------

@app.get("/importar_movimentacao", response_class=HTMLResponse)
async def importar_movimentacao(request: Request):
    return templates.TemplateResponse("importar_movimentacao.html", {"request": request})

@app.post("/importar_movimentacao")
async def upload_movimentacao_csv(request: Request, file: UploadFile = File(...)):
    contents = await file.read()
    decoded = contents.decode("utf-8").splitlines()
    reader = csv.DictReader(decoded)
    headers = reader.fieldnames
    temp_id = str(uuid4())
    temp_file = f"temp_{temp_id}.csv"
    with open(temp_file, "w", newline='', encoding="utf-8") as f:
        f.writelines(decoded)
    request.session["temp_file"] = temp_file
    return templates.TemplateResponse("mapeamento_colunas.html", {"request": request, "headers": headers})

@app.post("/validar_mapeamento", response_class=HTMLResponse)
async def validar_mapeamento(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    mapeamento = dict(form)
    temp_file = request.session.get("temp_file")
    with open(temp_file, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        preview = []
        for row in reader:
            preview.append({campo: row.get(origem, "") for campo, origem in mapeamento.items()})
        request.session["mapeamento"] = mapeamento
    return templates.TemplateResponse("validar_importacao.html", {"request": request, "preview": preview})

@app.post("/confirmar_importacao")
async def confirmar_importacao(request: Request, db: Session = Depends(get_db)):
    temp_file = request.session.get("temp_file")
    mapeamento = request.session.get("mapeamento")
    with open(temp_file, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dados = {campo: row[mapeamento[campo]] for campo in mapeamento}
            cabecalho = db.query(ContratoCabecalho).filter_by(contrato_n=dados["contrato_n"]).first()
            if not cabecalho:
                continue
            tp = dados["tp_transacao"].upper()
            if tp == "RETORNO":
                db.query(Contrato).filter_by(
                    contrato_n=dados["contrato_n"],
                    cod_cli=dados["cod_cli"],
                    ativo=dados["ativo"]
                ).delete()
            elif tp == "ENVIO":
                novo = Contrato(**dados, cabecalho_id=cabecalho.id)
                calcular_valores(novo)
                db.add(novo)
            elif tp == "TROCA":
                existente = db.query(Contrato).filter_by(
                    contrato_n=dados["contrato_n"],
                    cod_cli=dados["cod_cli"],
                    ativo=dados["ativo"]
                ).first()
                if existente:
                    meses_restantes = existente.meses_restantes
                    db.delete(existente)
                    novo = Contrato(**dados, meses_restantes=meses_restantes, cabecalho_id=cabecalho.id)
                    calcular_valores(novo)
                    db.add(novo)
        db.commit()
    return RedirectResponse("/", status_code=303)

# ----------------------
# LOG DE CONTRATOS
# ----------------------

@app.get("/log_contrato/{id}", response_class=HTMLResponse)
async def log_contrato(id: int, request: Request, db: Session = Depends(get_db)):
    logs = db.query(ContratoLog).filter_by(contrato_id=id).all()
    return templates.TemplateResponse("log_contrato.html", {"request": request, "logs": logs})
