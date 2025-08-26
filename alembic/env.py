# alembic/env.py
# Versão: 1.3.0 (2025-08-21)
# Alterações desta versão:
# - Passa a usar a MESMA URL do app: tenta importar de `database.SQLALCHEMY_DATABASE_URL`.
#   Fallback: APP_DB_URL/DATABASE_URL/SQLALCHEMY_DATABASE_URL; por fim, alembic.ini.
# - Protege contra placeholder "driver://..." (causava NoSuchModuleError).
# - Garante a existência do diretório do arquivo SQLite antes de conectar.
# - Mantém autogenerate (target_metadata=models.Base, compare_type=True).

from __future__ import annotations

import os
import sys
from pathlib import Path
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import make_url

# Alembic config
config = context.config

# Logging do Alembic (opcional; respeita alembic.ini)
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# --- Path do projeto para importar models/Base e database ---
BASE_DIR = Path(__file__).resolve().parent.parent  # raiz do projeto (.. de /alembic)
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Importa modelos e metadata do projeto (ajuste o caminho se necessário)
from models import Base  # noqa: E402

# Metadata alvo para autogenerate
target_metadata = Base.metadata

# --------------------------- URL do Banco ---------------------------

def resolve_database_url() -> str:
    """Resolve a URL do banco com a mesma fonte do app.
    Ordem: database.SQLALCHEMY_DATABASE_URL -> APP_DB_URL/DATABASE_URL/SQLALCHEMY_DATABASE_URL -> alembic.ini
    """
    # 1) do app
    url = None
    try:
        from database import SQLALCHEMY_DATABASE_URL as APP_URL  # ajuste se seu módulo/constante tiver outro nome
        url = APP_URL
    except Exception:
        url = None

    # 2) ambiente
    url = url or os.getenv("APP_DB_URL") or os.getenv("DATABASE_URL") or os.getenv("SQLALCHEMY_DATABASE_URL")

    # 3) ini
    if not url:
        url = config.get_main_option("sqlalchemy.url")

    # valida placeholder
    if not url or url.strip().lower().startswith("driver://"):
        raise RuntimeError(
            f"Database URL inválida para Alembic: {url!r}. "
            "Use database.SQLALCHEMY_DATABASE_URL, APP_DB_URL/DATABASE_URL ou ajuste sqlalchemy.url no alembic.ini."
        )
    return url


def ensure_sqlite_dir(db_url: str) -> None:
    """Se a URL for SQLite com caminho de arquivo, cria o diretório pai se necessário."""
    try:
        u = make_url(db_url)
        if u.drivername.startswith("sqlite"):
            db_path = u.database or ""
            if db_path and db_path not in {":memory:", ":memory"}:
                p = Path(db_path)
                # Se veio relativo, torna relativo à raiz do projeto
                if not p.is_absolute():
                    p = (BASE_DIR / p).resolve()
                p.parent.mkdir(parents=True, exist_ok=True)
                # reescreve URL com caminho absoluto padronizado (POSIX) para evitar \ em Windows
                abs_url = f"sqlite+pysqlite:///{p.as_posix()}"
                config.set_main_option("sqlalchemy.url", abs_url)
    except Exception:
        # não falha se não conseguir parsear; segue com a URL original
        pass


# ----------------------- Configuração de contexto -------------------

def configure_context_offline(url: str) -> None:
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        include_schemas=True,
    )


def configure_context_online() -> None:
    url = resolve_database_url()
    # garante diretório para SQLite e normaliza a URL se necessário
    ensure_sqlite_dir(url)
    # mantém a URL efetiva no config (engine_from_config lê daqui)
    url_effective = config.get_main_option("sqlalchemy.url") or url
    config.set_main_option("sqlalchemy.url", url_effective)

    print(f"[alembic] usando sqlalchemy.url = {url_effective}")

    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_schemas=True,
        )
        with context.begin_transaction():
            context.run_migrations()


# ------------------------------- Entradas --------------------------

def run_migrations_offline() -> None:
    url = resolve_database_url()
    ensure_sqlite_dir(url)
    url_effective = config.get_main_option("sqlalchemy.url") or url
    configure_context_offline(url_effective)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configure_context_online()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
