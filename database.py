# database.py
# -----------------------------------------------------------------------------
# Versão: 2.2.1 (2025-08-28)
# Mudanças vs 2.2.0:
# - Autodetecção de driver: se APP_DB_DRIVER não estiver definido, tenta usar
#   psycopg (v3) se instalado; caso contrário, cai para psycopg2. Evita
#   ModuleNotFoundError quando requirements só tem psycopg.
# - Mantém NullPool por padrão (bom para Render) e todas as opções anteriores.
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import NullPool, QueuePool

Base = declarative_base()


# ------------------------- helpers URL/driver -------------------------

def _normalize_db_url(url: str | None) -> str | None:
    if not url:
        return url
    url = url.strip()
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def _preferred_driver_from_runtime() -> str:
    """Retorna driver preferido.
    Ordem:
      1) APP_DB_DRIVER, se setada (psycopg|psycopg2)
      2) se 'psycopg' (v3) estiver instalado, usa psycopg
      3) fallback: psycopg2
    """
    env = (os.getenv("APP_DB_DRIVER") or "").strip().lower()
    if env in {"psycopg", "psycopg2"}:
        return env
    try:
        import psycopg  # noqa: F401
        return "psycopg"
    except Exception:
        return "psycopg2"


def _apply_driver_and_ssl(url: str, prefer_driver: str | None = None) -> str:
    parsed = urlparse(url)
    if not parsed.scheme.startswith("postgresql"):
        return url  # não é Postgres

    drv = (prefer_driver or _preferred_driver_from_runtime()).strip().lower()
    if drv not in {"psycopg", "psycopg2"}:
        drv = "psycopg2"

    scheme = f"postgresql+{drv}"
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))

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
    db_path = (Path(__file__).resolve().parent / "contratos.db").resolve()
    DATABASE_URL = f"sqlite+pysqlite:///{db_path.as_posix()}"
else:
    DATABASE_URL = _apply_driver_and_ssl(DATABASE_URL)


# ------------------------- engine args -------------------------

connect_args: dict = {}
engine_kwargs: dict = {"future": True, "pool_pre_ping": True}

if DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}
else:
    pool_mode = (os.getenv("APP_DB_POOL") or "null").strip().lower()
    if pool_mode == "queue":
        pool_size = int(os.getenv("APP_DB_POOL_SIZE", "5"))
        max_overflow = int(os.getenv("APP_DB_MAX_OVERFLOW", "10"))
        recycle = int(os.getenv("APP_DB_POOL_RECYCLE", "1800"))
        engine_kwargs.update({
            "poolclass": QueuePool,
            "pool_size": pool_size,
            "max_overflow": max_overflow,
            "pool_recycle": recycle,
            "pool_use_lifo": True,
        })
    else:
        engine_kwargs.update({"poolclass": NullPool})

    connect_args.update({
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "application_name": os.getenv("RENDER_SERVICE_NAME", "app-contratos"),
    })


# ------------------------- engine & session -------------------------

engine = create_engine(
    DATABASE_URL,
    connect_args=connect_args,
    **engine_kwargs,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
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
    return {
        "url": _redact_url(DATABASE_URL),
        "pool": engine.pool.__class__.__name__,
        "driver": urlparse(DATABASE_URL).scheme,
        "pre_ping": engine_kwargs.get("pool_pre_ping", False),
    }
