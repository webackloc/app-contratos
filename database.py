# database.py
# -----------------------------------------------------------------------------
# Versão: 2.0.0 (2025-08-21)
# Mudanças principais:
# - Mantém compatibilidade com PRODUÇÃO via variável de ambiente (DATABASE_URL / APP_DB_URL / SQLALCHEMY_DATABASE_URL),
#   normalizando "postgres://" -> "postgresql://".
# - Em DESENVOLVIMENTO (sem env var), faz fallback para **SQLite local** em caminho ABSOLUTO, usando
#   o arquivo **contratos.db** na pasta do projeto (evita erro "unable to open database file" no Windows).
# - Pool conservador para Postgres (NullPool + pre_ping) e `check_same_thread=False` para SQLite.
# - Expõe `Base = declarative_base()` para uso nos models.
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import NullPool

# Declarative Base para os modelos (ex.: `from database import Base`)
Base = declarative_base()


def _normalize_db_url(url: str | None) -> str | None:
    """Normaliza esquemas antigos do Postgres ("postgres://") para "postgresql://".
    Não altera URLs vazias ou SQLite.
    """
    if not url:
        return url
    url = url.strip()
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


# 1) Lê a URL de ambiente (produção) ou cai para SQLite local (dev)
DATABASE_URL = (
    os.getenv("APP_DB_URL")
    or os.getenv("DATABASE_URL")
    or os.getenv("SQLALCHEMY_DATABASE_URL")
)
DATABASE_URL = _normalize_db_url(DATABASE_URL)

if not DATABASE_URL:
    # Fallback de DEV: arquivo contratos.db na pasta do projeto, com caminho ABSOLUTO
    db_path = (Path(__file__).resolve().parent / "contratos.db").resolve()
    DATABASE_URL = f"sqlite+pysqlite:///{db_path.as_posix()}"

# 2) Parâmetros específicos por driver
connect_args: dict = {}
engine_kwargs: dict = {"future": True}

if DATABASE_URL.startswith("sqlite"):
    # SQLite precisa desse parâmetro quando usado em apps web (threads)
    connect_args = {"check_same_thread": False}
else:
    # Em Postgres/serviços gerenciados, evite pool agressivo e habilite pre_ping
    engine_kwargs["poolclass"] = NullPool
    engine_kwargs["pool_pre_ping"] = True

# 3) Cria o engine
engine = create_engine(
    DATABASE_URL,
    connect_args=connect_args,
    **engine_kwargs,
)

# 4) Session factory e dependency para FastAPI
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Dependency de sessão para FastAPI: abre/fecha a conexão por request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
