"""add cod_cli em contratos_cabecalho

Compatível com SQLite (batch), Postgres e SQL Server.
v2 (2025-08-22): ignora drops indevidos (users/valor_presente) e faz guards idempotentes.
"""

from alembic import op
import sqlalchemy as sa

# Revisões Alembic
revision = "1f3cebbed642"
down_revision = "dd855b561022"  # última que você aplicou antes desta
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # --- contratos_cabecalho: adicionar coluna cod_cli (se não existir) ---
    cols = {c["name"] for c in insp.get_columns("contratos_cabecalho")}
    if "cod_cli" not in cols:
        # Em SQLite, add_column é suportado; índices criamos depois
        op.add_column("contratos_cabecalho", sa.Column("cod_cli", sa.String(), nullable=True))

    # --- índice em cod_cli (se não existir) ---
    idxs = {ix["name"] for ix in insp.get_indexes("contratos_cabecalho")}
    if "ix_contratos_cabecalho_cod_cli" not in idxs:
        op.create_index("ix_contratos_cabecalho_cod_cli", "contratos_cabecalho", ["cod_cli"])

    # Importante: NÃO apagar nada aqui (users, índices de users, nem contratos.valor_presente)
    # Mesmo que o autogenerate tenha sugerido, estamos mantendo por compatibilidade/legado.


def downgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # Remover índice (se existir)
    idxs = {ix["name"] for ix in insp.get_indexes("contratos_cabecalho")}
    if "ix_contratos_cabecalho_cod_cli" in idxs:
        op.drop_index("ix_contratos_cabecalho_cod_cli", table_name="contratos_cabecalho")

    # Remover coluna (se existir) — em SQLite precisa batch
    cols = {c["name"] for c in insp.get_columns("contratos_cabecalho")}
    if "cod_cli" in cols:
        with op.batch_alter_table("contratos_cabecalho") as batch_op:
            batch_op.drop_column("cod_cli")
