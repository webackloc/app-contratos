# tools/clean_db.py
# Limpa dados de todas as tabelas EXCETO as de usuários/roles.
# Mantém o schema. Seguro para SQLite/SQLAlchemy.

from contextlib import suppress
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Ajuste estes imports conforme o seu projeto:
from database import engine  # deve expor um Engine do SQLAlchemy
import models                # tabelas de contratos/logs/etc
import auth_models           # tabelas de usuários/roles/etc


# >>>>>> CONFIGURÁVEIS <<<<<<
# Coloque aqui os nomes EXATOS das tabelas que NÃO devem ser apagadas.
# ajuste conforme os nomes reais no seu banco:
TABLES_TO_KEEP = {
    "users",
    "roles",
    "user_roles",
    "alembic_version",  # se você usa Alembic, mantenha a versão
}
DRY_RUN = False  # True = só mostra o que faria; False = executa
# >>>>>> /CONFIGURÁVEIS <<<<<<


def list_tables(md):
    return [t.name for t in md.sorted_tables]  # já vem em ordem por dependências


def main(engine: Engine):
    metadatas = []
    with suppress(Exception):
        metadatas.append(models.Base.metadata)
    with suppress(Exception):
        metadatas.append(auth_models.Base.metadata)

    if not metadatas:
        raise RuntimeError(
            "Não encontrei metadados. Ajuste os imports de models/auth_models/Base."
        )

    # Mostra panorama antes de executar
    print("\n== Tabelas detectadas ==")
    for md in metadatas:
        for t in md.sorted_tables:
            keep = " (KEEP)" if t.name in TABLES_TO_KEEP else ""
            print(f"- {t.name}{keep}")

    if DRY_RUN:
        print("\nDRY_RUN=True: nada será apagado. Ajuste DRY_RUN=False para executar.")
        return

    backend = engine.url.get_backend_name()

    with engine.begin() as conn:
        # Para SQLite, deletar em ordem reversa já respeita chaves.
        # (Opcional) garantir FK ON:
        if backend == "sqlite":
            conn.exec_driver_sql("PRAGMA foreign_keys=ON")

        # Apagar em ordem reversa (respeita FKs)
        for md in metadatas:
            for table in reversed(md.sorted_tables):
                if table.name in TABLES_TO_KEEP:
                    continue
                print(f"DELETE FROM {table.name}")
                conn.execute(table.delete())

    # (Opcional) VACUUM para reduzir o arquivo SQLite
    if backend == "sqlite":
        with engine.begin() as conn:
            print("VACUUM (SQLite)…")
            conn.exec_driver_sql("VACUUM")

    print("\nOK: dados limpos; schema preservado; tabelas de usuários/roles mantidas.")


if __name__ == "__main__":
    main(engine)

