# routers/contratos_sync.py
# ------------------------------------------------------------
# Sincroniza período contratual a partir do cabeçalho e
# recalcula meses_restantes, valor_global_contrato e
# valor_presente_contrato.
#
# Endpoints:
# - POST /contratos/sincronizar/{contrato_num}  -> um contrato
# - POST /contratos/sincronizar                 -> TODOS os contratos
# ------------------------------------------------------------

from fastapi import APIRouter, Depends, HTTPException, Request
from starlette.responses import RedirectResponse
from sqlalchemy.orm import Session

from database import get_db
from models import Contrato, ContratoCabecalho
from utils.recalculo_contratos import (
    calc_meses_restantes, calc_valor_global, calc_valor_presente
)

router = APIRouter()


def _pick_attr(model, *names):
    """
    Retorna (nome, atributo) do primeiro nome existente no model, senão (None, None).
    Permite tolerância a variações de nomes de colunas, ex.: contrato_n vs contrato_num.
    """
    for n in names:
        attr = getattr(model, n, None)
        if attr is not None:
            return n, attr
    return None, None


@router.post("/sincronizar/{contrato_num}")
def sincronizar_contrato(contrato_num: str, request: Request, db: Session = Depends(get_db)):
    """
    Sincroniza e recalcula apenas um contrato (identificado por contrato_num).
    """
    # Cabeçalho: aceita contrato_n, contrato_num, numero, numero_contrato
    _, cab_num_col = _pick_attr(ContratoCabecalho, "contrato_n", "contrato_num", "numero", "numero_contrato")
    if cab_num_col is None:
        raise HTTPException(500, detail="ContratoCabecalho sem coluna de número (tente: contrato_n/contrato_num/numero).")

    cab = db.query(ContratoCabecalho).filter(cab_num_col == contrato_num).first()
    if not cab:
        raise HTTPException(404, detail=f"Cabeçalho do contrato {contrato_num} não encontrado.")

    # Pega prazo (tenta 'prazo_contratual' e cai para 'periodo_contratual')
    prazo = getattr(cab, "prazo_contratual", None)
    if prazo is None:
        prazo = getattr(cab, "periodo_contratual", None)

    indice_anual = getattr(cab, "indice_reajuste", None)

    # Itens: aceita contrato_n, contrato_num, numero, numero_contrato
    _, item_num_col = _pick_attr(Contrato, "contrato_n", "contrato_num", "numero", "numero_contrato")
    if item_num_col is None:
        raise HTTPException(500, detail="Contrato (itens) sem coluna de número (tente: contrato_n/contrato_num/numero).")

    itens = db.query(Contrato).filter(item_num_col == contrato_num).all()
    if not itens:
        url = request.headers.get("referer") or "/contratos"
        return RedirectResponse(url=f"{url}?ok=0&msg=sem_itens", status_code=303)

    atualizados = 0
    for it in itens:
        # Se existir 'periodo_contratual' no item e houver prazo no cabeçalho, copie
        if hasattr(it, "periodo_contratual") and prazo is not None:
            it.periodo_contratual = prazo

        # período para cálculo
        periodo = getattr(it, "periodo_contratual", None) or prazo or 0

        # data de início: tenta várias opções
        data_inicio = getattr(it, "data_envio", None) or getattr(it, "data_inicio", None) or getattr(it, "data", None)

        # valor mensal
        valor_mensal = getattr(it, "valor_mensal", 0.0)

        # recalcular
        it.meses_restantes = calc_meses_restantes(data_inicio, int(periodo or 0))
        it.valor_global_contrato = calc_valor_global(valor_mensal, int(periodo or 0))
        it.valor_presente_contrato = calc_valor_presente(valor_mensal, it.meses_restantes, indice_anual)

        atualizados += 1

    db.commit()

    url = request.headers.get("referer") or f"/contratos/{contrato_num}"
    return RedirectResponse(url=f"{url}?ok=1&n={atualizados}", status_code=303)


@router.post("/sincronizar")
def sincronizar_todos(request: Request, db: Session = Depends(get_db)):
    """
    Sincroniza e recalcula TODOS os contratos em lote.
    - Copia período do cabeçalho (se existir no item) e recalcula campos derivados.
    - Continua mesmo se algum item falhar (contabiliza erros).
    """
    # Descobrir colunas de chave/atributos de cabeçalho e item
    cab_num_name, cab_num_col = _pick_attr(ContratoCabecalho, "contrato_n", "contrato_num", "numero", "numero_contrato")
    if cab_num_col is None:
        raise HTTPException(500, detail="ContratoCabecalho sem coluna de número (tente: contrato_n/contrato_num/numero).")

    prazo_name = "prazo_contratual" if hasattr(ContratoCabecalho, "prazo_contratual") else (
                 "periodo_contratual" if hasattr(ContratoCabecalho, "periodo_contratual") else None)
    indice_name = "indice_reajuste" if hasattr(ContratoCabecalho, "indice_reajuste") else None

    item_num_name, item_num_col = _pick_attr(Contrato, "contrato_n", "contrato_num", "numero", "numero_contrato")
    if item_num_col is None:
        raise HTTPException(500, detail="Contrato (itens) sem coluna de número (tente: contrato_n/contrato_num/numero).")

    # Mapa de cabeçalhos: numero (str) -> (prazo, indice)
    header_map = {}
    for cab in db.query(ContratoCabecalho).all():
        num = getattr(cab, cab_num_name)
        key = str(num) if num is not None else None
        if key is None:
            continue
        prazo = getattr(cab, prazo_name, None) if prazo_name else None
        indice = getattr(cab, indice_name, None) if indice_name else None
        header_map[key] = (prazo, indice)

    # Percorrer itens em lote e recalcular
    atualizados = 0
    pulados_sem_cab = 0
    erros = 0

    batch_size = 500
    q = db.query(Contrato)

    # yield_per reduz uso de memória e melhora throughput em coleções grandes
    for idx, it in enumerate(q.yield_per(batch_size), start=1):
        try:
            num_val = getattr(it, item_num_name)
            dados = header_map.get(str(num_val))
            if not dados:
                pulados_sem_cab += 1
                continue

            prazo, indice_anual = dados

            # copiar período para item (se existir a coluna)
            if hasattr(it, "periodo_contratual") and prazo is not None:
                it.periodo_contratual = prazo

            periodo = getattr(it, "periodo_contratual", None) or prazo or 0
            data_inicio = getattr(it, "data_envio", None) or getattr(it, "data_inicio", None) or getattr(it, "data", None)
            valor_mensal = getattr(it, "valor_mensal", 0.0)

            it.meses_restantes = calc_meses_restantes(data_inicio, int(periodo or 0))
            it.valor_global_contrato = calc_valor_global(valor_mensal, int(periodo or 0))
            it.valor_presente_contrato = calc_valor_presente(valor_mensal, it.meses_restantes, indice_anual)

            atualizados += 1

            # commits parciais para não segurar a transação por muito tempo
            if idx % batch_size == 0:
                db.flush()
                db.commit()
        except Exception:
            # contabiliza erro e segue para o próximo item
            erros += 1

    db.commit()

    url = request.headers.get("referer") or "/contratos"
    # Exibe contadores na UI (alert no template)
    return RedirectResponse(
        url=f"{url}?ok=1&n={atualizados}&skip={pulados_sem_cab}&err={erros}",
        status_code=303
    )
