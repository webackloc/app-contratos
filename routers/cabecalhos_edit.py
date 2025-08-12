# routers/cabecalhos_edit.py
from fastapi import APIRouter, Depends, HTTPException, Request, Form
from fastapi.templating import Jinja2Templates
from starlette.responses import RedirectResponse
from sqlalchemy.orm import Session

from database import get_db
from models import ContratoCabecalho

router = APIRouter()
templates = Jinja2Templates(directory="templates")

def _pick_attr_name(model, *names):
    for n in names:
        if hasattr(model, n):
            return n
    return None

def _set_first_attr(obj, names, value):
    for n in names:
        if hasattr(obj, n):
            setattr(obj, n, value)
            return n
    return None

@router.get("/contratos/cabecalhos/{cab_id}/editar")
def get_editar_cabecalho(cab_id: int, request: Request, db: Session = Depends(get_db)):
    cab = db.query(ContratoCabecalho).get(cab_id)
    if not cab:
        raise HTTPException(404, detail="Cabeçalho não encontrado")
    return templates.TemplateResponse(
        "contrato_cabecalho_edit.html",
        {"request": request, "cab": cab}
    )

@router.post("/contratos/cabecalhos/{cab_id}/editar")
def post_editar_cabecalho(
    cab_id: int,
    request: Request,
    nome_cliente: str = Form(...),
    cnpj: str = Form(...),
    prazo_contratual: int = Form(...),
    indice_reajuste: str = Form(""),
    vendedor: str = Form(""),
    contrato_num: str = Form(None),  # opcional editar nº
    db: Session = Depends(get_db),
):
    cab = db.query(ContratoCabecalho).get(cab_id)
    if not cab:
        raise HTTPException(404, detail="Cabeçalho não encontrado")

    # Atribuições diretas (ajuste nomes conforme seu modelo)
    # Nome do Cliente
    _set_first_attr(cab, ["nome_cliente", "nome_cli", "cliente_nome"], nome_cliente.strip())

    # CNPJ
    cab.cnpj = cnpj.strip() if hasattr(cab, "cnpj") else getattr(cab, "cnpj", None)

    # Prazo contratual (meses)
    if hasattr(cab, "prazo_contratual"):
        cab.prazo_contratual = int(prazo_contratual)
    elif hasattr(cab, "periodo_contratual"):
        cab.periodo_contratual = int(prazo_contratual)

    # Índice de reajuste (guarda como string ou float conforme seu modelo)
    if hasattr(cab, "indice_reajuste"):
        cab.indice_reajuste = indice_reajuste

    # Vendedor
    if hasattr(cab, "vendedor"):
        cab.vendedor = vendedor.strip()

    # Número do contrato (se quiser permitir edição)
    if contrato_num is not None:
        _set_first_attr(cab, ["contrato_n", "contrato_num", "numero", "numero_contrato"], contrato_num.strip())

    db.commit()

    # Volta para a tela anterior (lista/cadastro) com sinalização de sucesso
    back = request.query_params.get("next") or request.headers.get("referer") or "/contratos"
    return RedirectResponse(url=f"{back}?ok=1&edit=1", status_code=303)

