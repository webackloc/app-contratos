"""add status/mov_hash/data_retorno em contratos; mov_hash em contratos_logs; índices
Compat: SQLite (batch), Postgres e SQL Server
v2 (2025-08-14): SQLite-safe + guards de idempotência
"""
from alembic import op
import sqlalchemy as sa


# IDs
revision = "20250814_mov_cols_and_indexes"
down_revision = "20250814_baseline"
branch_labels = None
depends_on = None


# ---------- helpers ----------
def _has_column(insp, table, col):
    try:
        return any(c["name"] == col for c in insp.get_columns(table))
    except Exception:
        return False


def _has_index(insp, table, name):
    try:
        return any(ix.get("name") == name for ix in insp.get_indexes(table))
    except Exception:
        return False


def upgrade():
    bind = op.get_bind()
    dialect = bind.engine.dialect.name
    insp = sa.inspect(bind)

    # -------- contratos: colunas + CHECK(status) --------
    status_exists = _has_column(insp, "contratos", "status")
    mov_hash_exists = _has_column(insp, "contratos", "mov_hash")
    data_ret_exists = _has_column(insp, "contratos", "data_retorno")

    if dialect == "sqlite":
        # No SQLite, constraints exigem recriar tabela (batch mode)
        with op.batch_alter_table("contratos", recreate="always") as batch:
            if not status_exists:
                batch.add_column(sa.Column("status", sa.String(length=16), nullable=True))
            if not mov_hash_exists:
                batch.add_column(sa.Column("mov_hash", sa.String(length=128), nullable=True))
            if not data_ret_exists:
                batch.add_column(sa.Column("data_retorno", sa.Date(), nullable=True))
            # recria a tabela já com o CHECK
            # (mesmo se já existia a coluna, o batch monta o schema final com a constraint)
            batch.create_check_constraint(
                "ck_contratos_status",
                "status IN ('ATIVO','RETORNADO')",
            )
    else:
        if not status_exists:
            op.add_column("contratos", sa.Column("status", sa.String(length=16), nullable=True))
        if not mov_hash_exists:
            op.add_column("contratos", sa.Column("mov_hash", sa.String(length=128), nullable=True))
        if not data_ret_exists:
            op.add_column("contratos", sa.Column("data_retorno", sa.Date(), nullable=True))
        # em PG/SQL Server o ALTER ... ADD CONSTRAINT funciona
        try:
            op.create_check_constraint(
                "ck_contratos_status",
                "contratos",
                "status IN ('ATIVO','RETORNADO')",
            )
        except Exception:
            # se já existir de uma tentativa anterior, ignore
            pass

    # -------- contratos_logs: coluna mov_hash --------
    cl_mov_hash_exists = _has_column(insp, "contratos_logs", "mov_hash")
    if dialect == "sqlite":
        with op.batch_alter_table("contratos_logs", recreate="auto") as batch:
            if not cl_mov_hash_exists:
                batch.add_column(sa.Column("mov_hash", sa.String(length=128), nullable=True))
    else:
        if not cl_mov_hash_exists:
            op.add_column("contratos_logs", sa.Column("mov_hash", sa.String(length=128), nullable=True))

    # -------- índices (cria só se não existirem) --------
    if not _has_index(insp, "contratos", "ix_contratos_mov_hash"):
        op.create_index("ix_contratos_mov_hash", "contratos", ["mov_hash"], unique=False)

    if not _has_index(insp, "contratos", "ix_contratos_cab_cli_ativo_status"):
        op.create_index(
            "ix_contratos_cab_cli_ativo_status",
            "contratos",
            ["cabecalho_id", "cod_cli", "ativo", "status"],
            unique=False,
        )

    if not _has_index(insp, "contratos_logs", "ix_contratos_logs_mov_hash"):
        op.create_index("ix_contratos_logs_mov_hash", "contratos_logs", ["mov_hash"], unique=False)

    # -------- backfill --------
    # Só roda se colunas necessárias existirem (após batch, existirão)
    if _has_column(insp, "contratos", "status") and _has_column(insp, "contratos", "data_retorno"):
        op.execute(
            """
            UPDATE contratos
               SET status = CASE WHEN data_retorno IS NULL THEN 'ATIVO' ELSE 'RETORNADO' END
             WHERE status IS NULL
            """
        )


def downgrade():
    bind = op.get_bind()
    dialect = bind.engine.dialect.name
    insp = sa.inspect(bind)

    # drop índices se existirem
    if _has_index(insp, "contratos_logs", "ix_contratos_logs_mov_hash"):
        op.drop_index("ix_contratos_logs_mov_hash", table_name="contratos_logs")
    if _has_index(insp, "contratos", "ix_contratos_cab_cli_ativo_status"):
        op.drop_index("ix_contratos_cab_cli_ativo_status", table_name="contratos")
    if _has_index(insp, "contratos", "ix_contratos_mov_hash"):
        op.drop_index("ix_contratos_mov_hash", table_name="contratos")

    # contratos_logs: remover mov_hash
    if dialect == "sqlite":
        with op.batch_alter_table("contratos_logs", recreate="always") as batch:
            if _has_column(insp, "contratos_logs", "mov_hash"):
                batch.drop_column("mov_hash")
    else:
        if _has_column(insp, "contratos_logs", "mov_hash"):
            op.drop_column("contratos_logs", "mov_hash")

    # contratos: remover CHECK + colunas
    if dialect == "sqlite":
        # recria a tabela sem o CHECK e sem as colunas adicionadas
        with op.batch_alter_table("contratos", recreate="always") as batch:
            if _has_column(insp, "contratos", "data_retorno"):
                batch.drop_column("data_retorno")
            if _has_column(insp, "contratos", "mov_hash"):
                batch.drop_column("mov_hash")
            if _has_column(insp, "contratos", "status"):
                batch.drop_column("status")
        # no batch, ao não recriar o CHECK, ele é removido automaticamente
    else:
        try:
            op.drop_constraint("ck_contratos_status", "contratos", type_="check")
        except Exception:
            pass
        if _has_column(insp, "contratos", "data_retorno"):
            op.drop_column("contratos", "data_retorno")
        if _has_column(insp, "contratos", "mov_hash"):
            op.drop_column("contratos", "mov_hash")
        if _has_column(insp, "contratos", "status"):
            op.drop_column("contratos", "status")
