# routers/cabecalhos_edit.py
# v2 (2025-08-21): incluir edição/persistência de cod_cli no cabeçalho;
#                  setters tolerantes a nomes alternativos; manter comportamento anterior.
# v1: versão original de edição de cabeçalho.

from fastapi import APIRouter, Depends, HTTPException, Request, Form
from fastapi.templating import Jinja2Templates
from starlette.responses import RedirectResponse
from sqlalchemy.orm import Session

from database import get_db
from models import ContratoCabecalho

router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _set_first_attr(obj, names, value):
    """Atribui o primeiro atributo existente em `names` no objeto `obj` e retorna o nome usado."""
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
    # O template acessa `cab` e já exibirá `cod_cli` se o campo existir
    return templates.TemplateResponse(
        "contrato_cabecalho_edit.html",
        {"request": request, "cab": cab},
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
    contrato_num: str | None = Form(None),   # opcional editar nº
    cod_cli: str = Form(...),                # <-- novo: código do cliente
    db: Session = Depends(get_db),
):
    cab = db.query(ContratoCabecalho).get(cab_id)
    if not cab:
        raise HTTPException(404, detail="Cabeçalho não encontrado")

    # Nome do Cliente (aceita variações de nome no modelo)
    _set_first_attr(cab, ["nome_cliente", "nome_cli", "cliente_nome"], nome_cliente.strip())

    # CNPJ (já existia no modelo)
    if hasattr(cab, "cnpj"):
        cab.cnpj = cnpj.strip()

    # Prazo contratual (meses)
    if hasattr(cab, "prazo_contratual"):
        cab.prazo_contratual = int(prazo_contratual)
    elif hasattr(cab, "periodo_contratual"):
        # fallback para modelos antigos
        cab.periodo_contratual = int(prazo_contratual)

    # Índice de reajuste
    if hasattr(cab, "indice_reajuste"):
        cab.indice_reajuste = (indice_reajuste or "").strip()

    # Vendedor
    if hasattr(cab, "vendedor"):
        cab.vendedor = (vendedor or "").strip()

    # Número do contrato (opcional)
    if contrato_num is not None:
        _set_first_attr(
            cab,
            ["contrato_n", "contrato_num", "numero", "numero_contrato"],
            contrato_num.strip(),
        )

    # NOVO: Código do Cliente
    _set_first_attr(
        cab,
        ["cod_cli", "codigo_cliente", "client_code"],
        (cod_cli or "").strip(),
    )

    db.commit()

    # Volta para a tela anterior (lista/cadastro) com sinalização de sucesso
    back = request.query_params.get("next") or request.headers.get("referer") or "/contratos"
    return RedirectResponse(url=f"{back}?ok=1&edit=1", status_code=303)
