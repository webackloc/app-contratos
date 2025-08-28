# database.py
# -----------------------------------------------------------------------------
# Versão: 2.2.0 (2025-08-28)
# Mudanças desta versão (compatível com a 2.1.0):
# - Mantém tudo que já funciona (fallback SQLite, Base, SessionLocal, get_db).
# - Torna o driver **configurável por env** (APP_DB_DRIVER=psycopg | psycopg2).
#   * Padrão continua psycopg2 (igual 2.1.0) para não quebrar produção.
# - SSL "require" para hosts da Render permanece; pode forçar via FORCE_DB_SSL=1.
# - Pool configurável: APP_DB_POOL=null (padrão, robusto no Render) ou queue.
#   * Se queue: suporta APP_DB_POOL_SIZE, APP_DB_MAX_OVERFLOW, APP_DB_POOL_RECYCLE.
# - Mantém pre_ping e keepalives (tanto para psycopg2 quanto psycopg 3).
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import NullPool, QueuePool

# Declarative Base para os modelos (ex.: `from database import Base`)
Base = declarative_base()


# ------------------------- helpers URL/driver -------------------------

def _normalize_db_url(url: str | None) -> str | None:
    """Normaliza 'postgres://' -> 'postgresql://'. Não altera SQLite."""
    if not url:
        return url
    url = url.strip()
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def _apply_driver_and_ssl(url: str, prefer_driver: str | None = None) -> str:
    """Força driver desejado (psycopg2 ou psycopg) e aplica sslmode=require
    para hosts *.render.com (ou quando FORCE_DB_SSL=1)."""
    parsed = urlparse(url)

    if not parsed.scheme.startswith("postgresql"):
        return url  # não é Postgres

    # Driver preferido (padrão = psycopg2, como na 2.1.0)
    drv = (prefer_driver or os.getenv("APP_DB_DRIVER") or "psycopg2").strip().lower()
    if drv not in {"psycopg2", "psycopg"}:
        drv = "psycopg2"

    scheme = f"postgresql+{drv}"

    # Query params (preserva existentes)
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))

    # sslmode=require quando host for da Render ou explicitamente forçado
    host_is_render = isinstance(parsed.hostname, str) and "render.com" in parsed.hostname
    force_ssl = os.getenv("FORCE_DB_SSL", "0").strip() in {"1", "true", "True"}
    if (host_is_render or force_ssl) and "sslmode" not in {k.lower(): v for k, v in q.items()}:
        q["sslmode"] = "require"

    new = parsed._replace(scheme=scheme, query=urlencode(q))
    return urlunparse(new)


# ------------------------- build DATABASE_URL -------------------------

DATABASE_URL = (
    os.getenv("APP_DB_URL")
    or os.getenv("DATABASE_URL")
    or os.getenv("SQLALCHEMY_DATABASE_URL")
)
DATABASE_URL = _normalize_db_url(DATABASE_URL)

if not DATABASE_URL:
    # Fallback DEV: arquivo contratos.db na raiz do projeto
    db_path = (Path(__file__).resolve().parent / "contratos.db").resolve()
    DATABASE_URL = f"sqlite+pysqlite:///{db_path.as_posix()}"
else:
    # Produção: driver configurável + SSL coerente
    DATABASE_URL = _apply_driver_and_ssl(DATABASE_URL)


# ------------------------- engine args -------------------------

connect_args: dict = {}
engine_kwargs: dict = {"future": True, "pool_pre_ping": True}

if DATABASE_URL.startswith("sqlite"):
    # Necessário para apps web (threads)
    connect_args = {"check_same_thread": False}
else:
    # Postgres (psycopg2 ou psycopg 3)
    # Pool: por padrão NullPool (robusto no Render). Permite queue via env.
    pool_mode = (os.getenv("APP_DB_POOL") or "null").strip().lower()
    if pool_mode == "queue":
        # Pool com reciclagem (bom se você tem workers estáveis)
        pool_size = int(os.getenv("APP_DB_POOL_SIZE", "5"))
        max_overflow = int(os.getenv("APP_DB_MAX_OVERFLOW", "10"))
        recycle = int(os.getenv("APP_DB_POOL_RECYCLE", "1800"))  # 30min
        engine_kwargs.update({
            "poolclass": QueuePool,
            "pool_size": pool_size,
            "max_overflow": max_overflow,
            "pool_recycle": recycle,
            "pool_use_lifo": True,
        })
    else:
        # Padrão: NullPool (sem conexões quentes entre requests)
        engine_kwargs.update({"poolclass": NullPool})

    # Keepalives (libpq) — funciona para psycopg2 e psycopg 3
    connect_args.update({
        "keepalives": 1,              # habilita keepalive
        "keepalives_idle": 30,        # segundos até o primeiro keepalive
        "keepalives_interval": 10,    # intervalo entre pacotes
        "keepalives_count": 5,        # tentativas antes de cair
        "application_name": os.getenv("RENDER_SERVICE_NAME", "app-contratos"),
    })


# ------------------------- engine & session -------------------------

engine = create_engine(
    DATABASE_URL,
    connect_args=connect_args,
    **engine_kwargs,
)

# Mantido como na 2.1.0 (autocommit/autoflush). Se precisar, podemos
# expor expire_on_commit=False em uma versão futura.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Dependency de sessão para FastAPI: abre/fecha por request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ------------------------- util opcional (debug) -------------------------

def _redact_url(u: str) -> str:
    try:
        p = urlparse(u)
        if p.password:
            netloc = (p.username or "") + ":***@" + (p.hostname or "")
            if p.port:
                netloc += f":{p.port}"
            p = p._replace(netloc=netloc)
        return urlunparse(p)
    except Exception:
        return u


def engine_info() -> dict:
    """Retorna informações úteis para logs/inspeção sem vazar segredos."""
    return {
        "url": _redact_url(DATABASE_URL),
        "pool": engine.pool.__class__.__name__,
        "driver": urlparse(DATABASE_URL).scheme,  # ex.: postgresql+psycopg2
        "pre_ping": engine_kwargs.get("pool_pre_ping", False),
    }
