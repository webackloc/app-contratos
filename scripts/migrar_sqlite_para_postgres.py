# scripts/migrar_sqlite_para_postgres.py
# -----------------------------------------------------------------------------
# Migra TODOS os dados do SQLite local para um banco PostgreSQL.
#
# Como usar (PowerShell, na raiz do projeto):
#   $env:SQLITE_URL="sqlite:///./data/app.sqlite"           # ajuste se seu arquivo for outro
#   $env:POSTGRES_URL="postgresql://USER:SENHA@HOST:5432/NOME_DB"
#   python scripts/migrar_sqlite_para_postgres.py
#
# Observações:
# - O script cria o schema no Postgres usando seus models (Base.metadata.create_all).
# - Copia linha a linha (em lotes) preservando as colunas simples.
# - Se você tiver MAIS tabelas (ex.: Usuario, Log, etc.), inclua no array TABLES.
# - Se suas PKs forem autoincrement no Postgres, pode ser necessário ajustar sequences depois.
# -----------------------------------------------------------------------------

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from typing import List, Type
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Ajuste estes imports ao seu projeto:
# - Base: declarative_base
# - ContratoCabecalho e Contrato: seus modelos principais
from models import Base, Contrato, ContratoCabecalho  # ADICIONE OUTROS MODELOS SE PRECISAR


def _normalize_db_url(url: str) -> str:
    """Normaliza 'postgres://' -> 'postgresql://'. Mantém outras URLs."""
    if not url:
        return url
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


# 1) Fontes (envs)
SQLITE_URL = os.getenv("SQLITE_URL", "sqlite:///./data/app.sqlite").strip()
POSTGRES_URL = _normalize_db_url(os.getenv("POSTGRES_URL", "").strip())

if not POSTGRES_URL:
    raise SystemExit(
        "Defina POSTGRES_URL no ambiente (ex.: postgresql://USER:SENHA@HOST:5432/NOME_DB)."
    )

# 2) Engines
src_connect_args = {"check_same_thread": False} if SQLITE_URL.startswith("sqlite") else {}
src_engine = create_engine(SQLITE_URL, connect_args=src_connect_args)
dst_engine = create_engine(POSTGRES_URL, pool_pre_ping=True)

# 3) Sessions
SrcSession = sessionmaker(bind=src_engine, autocommit=False, autoflush=False)
DstSession = sessionmaker(bind=dst_engine, autocommit=False, autoflush=False)

# 4) Liste aqui as tabelas a migrar (ordem importa se houver FKs)
TABLES: List[Type] = [
    ContratoCabecalho,
    Contrato,
    # Ex.: Usuario, LogMovimentacao, etc...
]


def copy_table(src_sess, dst_sess, model, batch_commit=1000, fetch_batch=500) -> int:
    """
    Copia todos os registros de uma tabela do SQLite para o Postgres.
    - batch_commit: a cada N inserts faz commit;
    - fetch_batch: usa yield_per para reduzir memória.
    """
    table_name = getattr(model, "__tablename__", model.__table__.name)
    print(f"[...] Copiando tabela: {table_name}")
    total = 0

    query = src_sess.query(model).yield_per(fetch_batch)
    for i, row in enumerate(query, start=1):
        # Copia colunas simples (ignora relationships)
        data = {col.name: getattr(row, col.name) for col in model.__table__.columns}
        dst_sess.add(model(**data))
        total += 1

        if i % batch_commit == 0:
            try:
                dst_sess.commit()
            except IntegrityError as e:
                dst_sess.rollback()
                print(f"[WARN] conflito de integridade ao commitar {table_name}: {e}")

    # commit final
    try:
        dst_sess.commit()
    except IntegrityError as e:
        dst_sess.rollback()
        print(f"[WARN] conflito de integridade no commit final de {table_name}: {e}")

    print(f"[OK ] {table_name}: {total} registro(s)")
    return total


def main():
    t0 = time.time()
    print("= MIGRAÇÃO: SQLite -> PostgreSQL ================================")
    print(f"SQLITE_URL   = {SQLITE_URL}")
    print(f"POSTGRES_URL = {POSTGRES_URL.split('@')[-1]}  (credenciais ocultas)")
    print("Criando schema no destino (se necessário)...")
    Base.metadata.create_all(bind=dst_engine)

    with SrcSession() as s_src, DstSession() as s_dst:
        grand_total = 0
        for model in TABLES:
            grand_total += copy_table(s_src, s_dst, model)

    dt = time.time() - t0
    print(f"\nConcluído. Total inserido: {grand_total} registro(s) em {dt:0.1f}s.")
    print("=================================================================")
    print("Se suas PKs são autoincrement no Postgres e você inseriu IDs manualmente,")
    print("talvez seja preciso ajustar sequences (posso te passar um script se quiser).")


if __name__ == "__main__":
    main()
