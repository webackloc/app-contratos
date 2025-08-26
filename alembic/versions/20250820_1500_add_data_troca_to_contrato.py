"""add data_troca to contratos

Revision ID: 20250820_1500
Revises: "20250814_mov_cols_and_indexes"
Create Date: 2025-08-20 15:00:00
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20250820_1500"
down_revision = "20250814_mov_cols_and_indexes"

branch_labels = None
depends_on = None


def upgrade():
    op.add_column("contratos", sa.Column("data_troca", sa.Date(), nullable=True))


def downgrade():
    op.drop_column("contratos", "data_troca")
