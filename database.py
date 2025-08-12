"""
Módulo: Database
Versão: 1.2.0
Data: 2025-08-11
Autor: Leonardo Muller

Descrição:
    Configuração do banco de dados (SQLAlchemy) e sessão para uso com FastAPI.
    Expõe:
        - engine
        - SessionLocal
        - Base
        - get_db(): dependência para injetar sessão nos endpoints

Histórico de Alterações:
    1.2.0 - 2025-08-11
        • Adicionada a função get_db() para uso como dependência no FastAPI.
        • Definido expire_on_commit=False para evitar objetos expirados após commit.
        • Mantido SQLite local (contratos.db) com check_same_thread=False (Windows/uvicorn reload).
    1.1.0 - 2025-08-06
        • Ajustes de compatibilidade com SQLAlchemy e organização.
    1.0.0 - 2025-08-06
        • Criação do módulo inicial (engine, SessionLocal, Base).
"""

from __future__ import annotations
import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# URL de conexão (env > default). Mantém seu padrão anterior: ./contratos.db
SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./contratos.db").strip()

# Parâmetros específicos para SQLite (necessário no Windows/uvicorn com reload)
connect_args = {"check_same_thread": False} if SQLALCHEMY_DATABASE_URL.startswith("sqlite") else {}

# Engine
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args=connect_args,
    future=True,
    echo=False,  # mude para True se quiser logs SQL no console
)

# Session factory
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,  # evita expirar objetos após commit (facilita no FastAPI)
    future=True,
)

# Base ORM para seus modelos
Base = declarative_base()

def get_db() -> Generator:
    """
    Dependência para injetar sessão nos endpoints FastAPI.
    Uso:
        from fastapi import Depends
        from sqlalchemy.orm import Session

        @app.get("/algo")
        def handler(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Opcional (dev): criar todas as tabelas com Base.metadata
def create_all() -> None:
    Base.metadata.create_all(bind=engine)
