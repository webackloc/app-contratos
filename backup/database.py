# database.py
# Versão 1.0.0 - 2025-08-06
# Finalidade: Configuração da conexão com o banco de dados e sessão do SQLAlchemy

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# URL de conexão com banco SQLite local
SQLALCHEMY_DATABASE_URL = "sqlite:///./contratos.db"

# Criação do engine para conexão
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}  # Necessário para SQLite em modo single thread
)

# Criação da sessão local do SQLAlchemy
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base declarativa para os modelos ORM
Base = declarative_base()
