# models.py
# =====================================================================
# App Contratos - Modelos SQLAlchemy
# Versão: 1.7.3
# Data: 22/08/2025
# Alterações nesta versão:
# - ContratoCabecalho: mantém campo 'cod_cli' e garante índice nomeado
#   'ix_contratos_cabecalho_cod_cli' (alinhado à migração criada).
# - Índices explicitados para preservar o que já existe no banco
#   (ex.: ix_contratos_id, ix_contratos_logs_mov_hash).
# - Sem mudanças de esquema não relacionadas.
# Compatível com SQLite (dev) e PostgreSQL/SQL Server (prod).
# =====================================================================

from __future__ import annotations
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy import (
    Column, Integer, String, Float, Date, DateTime, ForeignKey,
    CheckConstraint, Index
)
from sqlalchemy.orm import relationship

from database import Base

# JSON: compat SA 1.x/2.x + fallback
try:
    JSON = sa.JSON  # SA 1.4+/2.x
except AttributeError:
    JSON = sa.Text   # fallback neutro (SQLite trata como TEXT)


# =========================
# Itens do contrato (linhas)
# =========================
class Contrato(Base):
    __tablename__ = "contratos"

    id = Column(Integer, primary_key=True, index=True)

    # Identificação do item
    ativo = Column(String, nullable=False)
    serial = Column(String, nullable=True)          # opcional; duplicidade tratada em regra
    cod_pro = Column(String, nullable=False)
    descricao_produto = Column(String, nullable=False)

    # Cliente
    cod_cli = Column(String, nullable=False)
    nome_cli = Column(String, nullable=False)

    # Datas do ciclo
    data_envio = Column(Date, nullable=False)
    data_troca = Column(Date, nullable=True)        # usado em TROCA
    data_retorno = Column(Date, nullable=True)      # preenchido em RETORNO

    # Legado (compat com CSVs antigos)
    contrato_n = Column(String, nullable=True)      # oficial em ContratoCabecalho.contrato_num

    # Valores
    valor_mensal = Column(Float, nullable=True)
    periodo_contratual = Column(Integer, nullable=True)
    meses_restantes = Column(Integer, nullable=True)
    valor_global_contrato = Column(Float, nullable=True)
    valor_presente_contrato = Column(Float, nullable=True)

    # Movimentação / auditoria
    tp_transacao = Column(String, nullable=True)    # última transação aplicada
    status = Column(String(16), nullable=True)      # 'ATIVO' | 'RETORNADO'
    mov_hash = Column(String(128), nullable=True)   # idempotência por hash

    # Relacionamentos
    cabecalho_id = Column(Integer, ForeignKey("contratos_cabecalho.id"), nullable=True)
    cabecalho = relationship("ContratoCabecalho", back_populates="itens", lazy="joined")

    logs = relationship("ContratoLog", back_populates="contrato", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint("status in ('ATIVO','RETORNADO')", name="ck_contratos_status"),
        Index("ix_contratos_mov_hash", "mov_hash"),
        Index("ix_contratos_cab_cli_ativo_status", "cabecalho_id", "cod_cli", "ativo", "status"),
    )


# ==================================
# Cabeçalho do contrato (com cod_cli)
# ==================================
class ContratoCabecalho(Base):
    __tablename__ = "contratos_cabecalho"

    id = Column(Integer, primary_key=True, index=True)

    # Código do cliente (para digitação/pesquisa)
    # Observação: índice nomeado explicitamente abaixo para casar com a migração.
    cod_cli = Column(String, nullable=True)

    # Dados do cliente e contrato
    nome_cliente = Column(String, nullable=False)
    cnpj = Column(String, nullable=False)
    contrato_num = Column(String, nullable=False)   # número oficial
    prazo_contratual = Column(Integer, nullable=False)
    indice_reajuste = Column(String, nullable=False)
    vendedor = Column(String, nullable=False)

    # Relacionamento com itens
    itens = relationship("Contrato", back_populates="cabecalho", cascade="all, delete-orphan")


# ==========================================
# Histórico / logs de movimentação e mudanças
# ==========================================
class ContratoLog(Base):
    __tablename__ = "contratos_logs"

    id = Column(Integer, primary_key=True, index=True)

    # Referências (nem sempre teremos contrato_id em erros de validação)
    contrato_id = Column(Integer, ForeignKey("contratos.id"), nullable=True)
    contrato_cabecalho_id = Column(Integer, ForeignKey("contratos_cabecalho.id"), nullable=True)

    # Metadados do evento
    data_modificacao = Column(DateTime, default=datetime.utcnow)

    # Campos clássicos (compat)
    acao = Column(String, nullable=True)        # "IMPORT", "ENVIO", "RETORNO", "TROCA"
    descricao = Column(String, nullable=True)

    # Auditoria estruturada
    cod_cli = Column(String, nullable=True)
    ativo = Column(String, nullable=True)
    tp_transacao = Column(String, nullable=True)    # ENVIO | RETORNO | TROCA
    data_mov = Column(Date, nullable=True)
    mov_hash = Column(String(128), nullable=True)   # idempotência
    status = Column(String(16), nullable=True)      # OK | ERRO
    mensagem = Column(String, nullable=True)

    # Relacionamento
    contrato = relationship("Contrato", back_populates="logs")


# ================================
# Índices explícitos já existentes
# ================================
# Mantém nomes que já existem no banco para evitar "drop/create" desnecessário
Index("ix_contratos_id", Contrato.id)
Index("ix_contratos_logs_id", ContratoLog.id)
Index("ix_contratos_logs_mov_hash", ContratoLog.mov_hash)
Index("ix_contratos_cabecalho_cod_cli", ContratoCabecalho.cod_cli)


# =====================================================
# Pré-importação de movimentações (lotes e itens)
# =====================================================
class MovimentacaoLote(Base):
    __tablename__ = "movimentacao_lotes"

    id = Column(Integer, primary_key=True, index=True)
    criado_em = Column(DateTime, default=datetime.utcnow, nullable=False)
    status = Column(String(32), default="ABERTO", nullable=False)  # ABERTO | PREVIEW | COMMIT | ERRO
    arquivo = Column(String, nullable=True)                        # nome do arquivo importado (opcional)
    total_itens = Column(Integer, nullable=True)
    processado_em = Column(DateTime, nullable=True)

    itens = relationship("MovimentacaoItem", back_populates="lote", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_mov_lotes_status", "status"),
        Index("ix_mov_lotes_criado_em", "criado_em"),
    )


class MovimentacaoItem(Base):
    __tablename__ = "movimentacao_itens"

    id = Column(Integer, primary_key=True, index=True)
    lote_id = Column(Integer, ForeignKey("movimentacao_lotes.id"), nullable=False)

    linha_idx = Column(Integer, nullable=False)     # posição no arquivo
    payload = Column(JSON, nullable=True)           # dados mapeados (contrato_num, cod_cli, ativo, tp_transacao, data_mov etc.)
    erro_msg = Column(String, nullable=True)        # erro por linha (se houver)
    criado_em = Column(DateTime, default=datetime.utcnow, nullable=False)

    lote = relationship("MovimentacaoLote", back_populates="itens")

    __table_args__ = (
        Index("ix_mov_itens_lote_idx", "lote_id", "linha_idx"),
    )


# Índices explícitos (IDs) — preservam nomes existentes
Index("ix_movimentacao_lotes_id", MovimentacaoLote.id)
Index("ix_movimentacao_itens_id", MovimentacaoItem.id)
