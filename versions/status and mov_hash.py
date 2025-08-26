"""add status and mov_hash to Contrato/ContratoLog; indexes; backfill"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20250814_mov_status_hash"
down_revision = "<COLOQUE_A_REVISAO_ANTERIOR_AQUI>"
branch_labels = None
depends_on = None

def upgrade():
    # Contrato
    op.add_column("contratos", sa.Column("status", sa.String(length=16), nullable=True))
    op.add_column("contratos", sa.Column("mov_hash", sa.String(length=128), nullable=True))
    op.create_index("ix_contratos_mov_hash", "contratos", ["mov_hash"], unique=False)
    op.create_index(
        "ix_contratos_cab_cli_ativo_status",
        "contratos",
        ["contrato_cabecalho_id", "cod_cli", "ativo", "status"],
        unique=False,
    )

    # ContratoLog
    op.add_column("contrato_logs", sa.Column("mov_hash", sa.String(length=128), nullable=True))
    op.create_index("ix_contrato_logs_mov_hash", "contrato_logs", ["mov_hash"], unique=False)

    # Backfill de status: ATIVO se sem data_retorno; RETORNADO caso contrário
    op.execute(
        """
        UPDATE contratos
           SET status = CASE WHEN data_retorno IS NULL THEN 'ATIVO' ELSE 'RETORNADO' END
        """
    )

    # Constraint simples para status
    op.create_check_constraint(
        "ck_contratos_status",
        "contratos",
        "status in ('ATIVO','RETORNADO')"
    )

def downgrade():
    op.drop_constraint("ck_contratos_status", "contratos", type_="check")
    op.drop_index("ix_contrato_logs_mov_hash", table_name="contrato_logs")
    op.drop_column("contrato_logs", "mov_hash")
    op.drop_index("ix_contratos_cab_cli_ativo_status", table_name="contratos")
    op.drop_index("ix_contratos_mov_hash", table_name="contratos")
    op.drop_column("contratos", "mov_hash")
    op.drop_column("contratos", "status")
