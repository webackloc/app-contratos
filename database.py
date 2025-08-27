# database.py
# -----------------------------------------------------------------------------
# Versão: 2.1.0 (2025-08-27)
# Mudanças:
# - Força o uso do driver psycopg2 para Postgres (evita psycopg v3).
# - Adiciona sslmode=require automaticamente para hosts do Render.
# - Habilita pre_ping e keepalives para reduzir "the connection is closed".
# - Mantém fallback para SQLite local em desenvolvimento.
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import NullPool

# Declarative Base para os modelos (ex.: `from database import Base`)
Base = declarative_base()


def _normalize_db_url(url: str | None) -> str | None:
    """Normaliza 'postgres://' -> 'postgresql://'.
    Não altera URLs vazias ou SQLite."""
    if not url:
        return url
    url = url.strip()
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def _force_psycopg2_and_ssl(url: str) -> str:
    """
    Se a URL for Postgres, força o dialecto 'postgresql+psycopg2://'
    e acrescenta 'sslmode=require' quando o host é do Render e
    o parâmetro não está presente.
    """
    parsed = urlparse(url)

    # Só toca se for postgres
    if parsed.scheme.startswith("postgresql"):
        # força driver psycopg2
        scheme = "postgresql+psycopg2"

        # reconstrói netloc/params sem mexer em user:pass@host:port
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))

        host_is_render = isinstance(parsed.hostname, str) and "render.com" in parsed.hostname
        if host_is_render and "sslmode" not in {k.lower(): v for k, v in query.items()}:
            query["sslmode"] = "require"

        new = parsed._replace(
            scheme=scheme,
            query=urlencode(query),
        )
        return urlunparse(new)

    return url


# 1) Lê a URL de ambiente (produção) ou cai para SQLite local (dev)
DATABASE_URL = (
    os.getenv("APP_DB_URL")
    or os.getenv("DATABASE_URL")
    or os.getenv("SQLALCHEMY_DATABASE_URL")
)
DATABASE_URL = _normalize_db_url(DATABASE_URL)

if not DATABASE_URL:
    # Fallback de DEV: arquivo contratos.db na pasta do projeto, caminho ABSOLUTO
    db_path = (Path(__file__).resolve().parent / "contratos.db").resolve()
    DATABASE_URL = f"sqlite+pysqlite:///{db_path.as_posix()}"
else:
    # Produção: força psycopg2 e sslmode=require p/ Render
    DATABASE_URL = _force_psycopg2_and_ssl(DATABASE_URL)

# 2) Parâmetros específicos por driver
connect_args: dict = {}
engine_kwargs: dict = {"future": True}

if DATABASE_URL.startswith("sqlite"):
    # SQLite precisa desse parâmetro quando usado em apps web (threads)
    connect_args = {"check_same_thread": False}
else:
    # Postgres em serviços gerenciados (Render):
    #  - NullPool: sem conexões quentes entre requests (robusto p/ ambientes serverless)
    #  - pre_ping: valida conexão antes de usar (evita "connection is closed")
    #  - keepalives: reduz quedas de conexão durante streams/yield_per
    engine_kwargs["poolclass"] = NullPool
    engine_kwargs["pool_pre_ping"] = True
    # Opções libpq (psycopg2) de keepalive:
    connect_args.update({
        # habilita keepalive
        "keepalives": 1,
        # segundos de inatividade antes de enviar keepalive
        "keepalives_idle": 30,
        # intervalo entre pacotes de keepalive
        "keepalives_interval": 10,
        # tentativas antes de considerar a conexão morta
        "keepalives_count": 5,
        # opcional: etiqueta a conexão
        "application_name": os.getenv("RENDER_SERVICE_NAME", "app-contratos"),
    })

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
