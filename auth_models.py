# auth_models.py — v1.1.2 (12/08/2025)
from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, inspect, text
from sqlalchemy.engine import Engine
from models import Base
from database import engine

# --------------------------
# Modelo
# --------------------------
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(120), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    role = Column(String(50), default="user", nullable=False)
    email = Column(String(255), nullable=True)  # <— NOVO
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    @property
    def is_admin(self) -> bool:
        return (self.role or "user").lower() == "admin"


def _sqlite_add_column_if_missing(engine: Engine, table: str, column: str, ddl: str) -> None:
    """Adiciona coluna no SQLite se não existir (simples e suficiente p/ dev)."""
    try:
        insp = inspect(engine)
        cols = [c["name"] for c in insp.get_columns(table)]
        if column not in cols:
            with engine.begin() as conn:
                conn.execute(text(f'ALTER TABLE {table} ADD COLUMN {ddl}'))
                print(f"[auth_models] Coluna '{column}' adicionada em '{table}'.")
    except Exception as e:
        print(f"[auth_models] Aviso: não foi possível checar/adicionar coluna '{column}': {e}")

def _sqlite_fix_role_default(engine: Engine) -> None:
    """Garante que role vazia/nula vire 'user'."""
    try:
        with engine.begin() as conn:
            conn.execute(text("UPDATE users SET role='user' WHERE role IS NULL OR role=''"))
    except Exception as e:
        print(f"[auth_models] Aviso: não foi possível normalizar 'role': {e}")

# cria a tabela ao importar o módulo (se não existir)
Base.metadata.create_all(bind=engine)

# migração leve para SQLite (opcional/seguro em dev)
try:
    if engine.dialect.name == "sqlite":
        _sqlite_add_column_if_missing(engine, "users", "email", "email VARCHAR(255)")
        _sqlite_fix_role_default(engine)
except Exception as e:
    print(f"[auth_models] Migração leve falhou (pode ignorar em prod): {e}")
