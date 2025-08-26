"""add data_troca to contrato

Revision ID: 5e7e2dde172f
Revises: 0d177c1a2de8
Create Date: 2025-08-20 15:53:35.001183

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5e7e2dde172f'
down_revision: Union[str, Sequence[str], None] = '0d177c1a2de8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
