"""merge heads

Revision ID: 0d177c1a2de8
Revises: dd855b561022, 20250820_1500
Create Date: 2025-08-20 15:48:09.371066

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0d177c1a2de8'
down_revision: Union[str, Sequence[str], None] = ('dd855b561022', '20250820_1500')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
