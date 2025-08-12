# schemas.py
from pydantic import BaseModel
from datetime import date
from typing import Optional

class ContratoOut(BaseModel):
    id: int
    ativo: Optional[str]
    serial: Optional[str]
    cod_pro: Optional[str]
    descricao_produto: Optional[str]
    cod_cli: Optional[str]
    nome_cli: Optional[str]
    data_envio: Optional[date]
    contrato_n: Optional[str]
    valor_mensal: Optional[float]
    periodo_contratual: Optional[int]
    meses_restantes: Optional[int]
    valor_global_contrato: Optional[float]
    valor_presente_contrato: Optional[float]

    class Config:
        orm_mode = True
