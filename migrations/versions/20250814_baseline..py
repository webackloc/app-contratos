# alembic/versions/20250814_baseline.py
# Baseline manual: cria todas as tabelas definidas em models.Base
from __future__ import annotations

from alembic import op
import os, sys

# IDs da revisão
revision = "20250814_baseline"
down_revision = None
branch_labels = None
depends_on = None

# Ajusta sys.path para importar models a partir da raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from models import Base  # Base.metadata contém todas as tabelas

def upgrade():
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)

def downgrade():
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
