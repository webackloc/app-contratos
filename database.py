# database.py
# -----------------------------------------------------------------------------
# Versão: 2.3.0 (2025-09-02)
# Mudanças vs 2.2.1:
# - [FIX] SessionLocal agora usa expire_on_commit=False por padrão para evitar
#   DetachedInstanceError ao acessar atributos após commit.
# - [NEW] db_session(): context manager para abrir/fechar sessão c/ commit/rollback.
# - [NEW] db_transaction(db): context manager para bloco transacional coeso.
# - [NEW] ensure_attached(db, obj): garante que um ORM possivelmente detached
#   seja "re-anexado" à sessão via merge(load=False).
# - [IMP] engine_info() inclui expire_on_commit.
# - Mantém autodetecção de driver, NullPool/QueuePool, SSL e demais configs.
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from typing import Generator, Optional

from sqlalchemy import create_engine, inspect as sa_inspect
from sqlalchemy.orm import sessionmaker, declarative_base, Session
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
    force_ssl = os.getenv("FORCE_DB_SSL", "0").strip().lower() in {"1", "true"}
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

    # Parâmetros de keepalive (libpq) funcionam para psycopg/psycopg2
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

# Permite override por env, mas por padrão DESATIVA expiração pós-commit
_expire_default = os.getenv("APP_DB_EXPIRE_ON_COMMIT", "false").strip().lower() in {"1", "true", "yes"}
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    expire_on_commit=_expire_default,  # <- chave para evitar DetachedInstanceError
)


def get_db() -> Generator[Session, None, None]:
    """Dependência FastAPI padrão: abre sessão por request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ------------------------- context helpers (recomendado p/ importação) -------------------------

@contextmanager
def db_session(commit: bool = True) -> Generator[Session, None, None]:
    """
    Abre uma sessão, faz commit (ou rollback se der erro) e fecha.
    Uso típico em jobs/lotes (ex.: importação):
        with db_session() as db:
            ... (operações) ...
    """
    db = SessionLocal()
    try:
        yield db
        if commit:
            db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@contextmanager
def db_transaction(db: Session) -> Generator[Session, None, None]:
    """
    Inicia um bloco transacional dentro de uma sessão existente.
    Útil para garantir que um conjunto de operações seja aplicado atômicamente:
        with db_session() as db:
            with db_transaction(db):
                ... (ENVIO/TROCA/RETORNO em sequência) ...
    """
    # O context manager do SQLAlchemy já cuida de commit/rollback
    with db.begin():
        yield db


def ensure_attached(db: Session, obj):
    """
    Garante que 'obj' (um ORM) esteja ligado à sessão 'db'.
    Se estiver 'detached', faz merge(load=False) para re-anexar sem recarregar do banco.
    Retorna o objeto anexado.
    """
    if obj is None:
        return None
    try:
        state = sa_inspect(obj)
    except Exception:
        # Se não for um objeto ORM, retorna como está
        return obj
    if state.detached:
        # evita refresh automático; perfeito para casos onde só precisamos do id/relacionamentos já carregados
        return db.merge(obj, load=False)
    return obj


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
        "expire_on_commit": _expire_default,
    }
