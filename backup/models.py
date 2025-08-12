# models.py
# Versão: 1.5.4
# Data: 08/08/2025
# Alterações:
# - Adicionado campo 'periodo_contratual' ao modelo Contrato
# - Correções finais para compatibilidade total com CSV

from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from database import Base
from datetime import datetime

# Modelo de contratos importados via CSV (itens do contrato)
class Contrato(Base):
    __tablename__ = "contratos"

    id = Column(Integer, primary_key=True, index=True)
    ativo = Column(String, nullable=False)
    serial = Column(String, nullable=False)
    cod_pro = Column(String, nullable=False)
    descricao_produto = Column(String, nullable=False)
    cod_cli = Column(String, nullable=False)
    nome_cli = Column(String, nullable=False)
    data_envio = Column(Date, nullable=False)
    contrato_n = Column(String, nullable=False)
    valor_mensal = Column(Float, nullable=False)

    periodo_contratual = Column(Integer, nullable=True)  # <-- Adicionado
    meses_restantes = Column(Integer, nullable=False)
    valor_global_contrato = Column(Float, nullable=False)
    valor_presente_contrato = Column(Float, nullable=False)

    tp_transacao = Column(String, nullable=True)

    cabecalho_id = Column(Integer, ForeignKey("contratos_cabecalho.id"), nullable=True)
    cabecalho = relationship("ContratoCabecalho", back_populates="itens")

    logs = relationship("ContratoLog", back_populates="contrato", cascade="all, delete-orphan")


# Modelo para o cabeçalho do contrato
class ContratoCabecalho(Base):
    __tablename__ = "contratos_cabecalho"

    id = Column(Integer, primary_key=True, index=True)
    nome_cliente = Column(String, nullable=False)
    cnpj = Column(String, nullable=False)
    contrato_num = Column(String, nullable=False)
    prazo_contratual = Column(Integer, nullable=False)
    indice_reajuste = Column(String, nullable=False)
    vendedor = Column(String, nullable=False)

    itens = relationship("Contrato", back_populates="cabecalho", cascade="all, delete-orphan")


# Modelo de histórico de alterações por contrato
class ContratoLog(Base):
    __tablename__ = "contratos_logs"

    id = Column(Integer, primary_key=True, index=True)
    contrato_id = Column(Integer, ForeignKey("contratos.id"), nullable=False)
    data_modificacao = Column(DateTime, default=datetime.utcnow)
    acao = Column(String, nullable=False)
    descricao = Column(String, nullable=False)

    contrato = relationship("Contrato", back_populates="logs")
