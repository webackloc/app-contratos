# database.py
# -----------------------------------------------------------------------------
# Criação do engine e sessão do SQLAlchemy com suporte a:
# - DATABASE_URL via variável de ambiente (produção)
# - Fallback para SQLite local (desenvolvimento)
# - Normalização de "postgres://" -> "postgresql://"
# - Pool conservador (NullPool) e pre_ping no Postgres (evita conexões mortas)
# - EXPÕE Base = declarative_base() para uso pelos models
# -----------------------------------------------------------------------------

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import NullPool

# Declarative Base para os modelos (ex.: from database import Base)
Base = declarative_base()


def _normalize_db_url(url: str) -> str:
    """Normaliza esquemas antigos do Postgres ('postgres://') para 'postgresql://'.
    Não altera URLs vazias ou SQLite.
    """
    if not url:
        return url
    # Render/Heroku costumam fornecer 'postgres://'
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


# 1) Lê a URL do ambiente (produção) ou cai para SQLite (dev)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DATABASE_URL = _normalize_db_url(DATABASE_URL)

if not DATABASE_URL:
    # Ajuste o caminho do seu arquivo SQLite local, se necessário
    DATABASE_URL = "sqlite:///./data/app.sqlite"

# 2) Parâmetros específicos por driver
connect_args = {}
engine_kwargs = {}

if DATABASE_URL.startswith("sqlite"):
    # SQLite precisa desse parâmetro quando usado em apps web (threads)
    connect_args = {"check_same_thread": False}
else:
    # Em Postgres na nuvem, é melhor evitar um pool agressivo e habilitar pre_ping
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
