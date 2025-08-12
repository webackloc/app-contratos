# routers/export.py — v1.0.0
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from sqlalchemy.orm import Session
from io import StringIO, BytesIO
import csv

from database import get_db
from models import Contrato, ContratoCabecalho

router = APIRouter()

@router.get("/export/contratos.csv")
def export_contratos_csv(db: Session = Depends(get_db)):
    output = StringIO()
    writer = csv.writer(output, lineterminator="\n")

    writer.writerow([
        "id", "cliente", "cnpj", "ativo", "serial", "descricao_produto",
        "valor_mensal", "meses_restantes", "valor_global_contrato", "valor_presente_contrato",
    ])

    q = (
        db.query(
            Contrato.id,
            Contrato.nome_cli,
            ContratoCabecalho.cnpj,
            Contrato.ativo,
            Contrato.serial,
            Contrato.descricao_produto,
            Contrato.valor_mensal,
            Contrato.meses_restantes,
            Contrato.valor_global_contrato,
            Contrato.valor_presente_contrato,
        )
        .outerjoin(ContratoCabecalho, Contrato.cabecalho_id == ContratoCabecalho.id)
    )

    for row in q:
        writer.writerow([
            row[0], row[1] or "", row[2] or "", row[3] or "", row[4] or "",
            row[5] or "", row[6] or 0.0, row[7] or 0, row[8] or 0.0, row[9] or 0.0
        ])

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=contratos.csv"}
    )

@router.get("/export/resumo.xlsx")
def export_resumo_xlsx(db: Session = Depends(get_db)):
    try:
        import pandas as pd
    except Exception:
        return JSONResponse(
            status_code=400,
            content={"error": "pandas/openpyxl não instalados. Use /export/contratos.csv ou instale dependências."},
        )

    contratos = (
        db.query(
            Contrato.id,
            Contrato.nome_cli,
            ContratoCabecalho.cnpj,
            Contrato.ativo,
            Contrato.serial,
            Contrato.descricao_produto,
            Contrato.valor_mensal,
            Contrato.meses_restantes,
            Contrato.valor_global_contrato,
            Contrato.valor_presente_contrato,
        )
        .outerjoin(ContratoCabecalho, Contrato.cabecalho_id == ContratoCabecalho.id)
        .all()
    )

    rows = []
    for c in contratos:
        rows.append({
            "ID": c[0],
            "Cliente": c[1] or "",
            "CNPJ": c[2] or "",
            "Ativo": c[3] or "",
            "Serial": c[4] or "",
            "Descrição": c[5] or "",
            "Valor Mensal": float(c[6] or 0.0),
            "Meses Restantes": int(c[7] or 0),
            "Valor Global": float(c[8] or 0.0),
            "Valor Presente": float(c[9] or 0.0),
        })

    import pandas as pd
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, sheet_name="Contratos")
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=resumo_contratos.xlsx"}
    )
