# Deploy na Web + Migração de Dados

Guia rápido para colocar o app FastAPI no ar (Render) com **PostgreSQL gerenciado** e levar **os dados locais** para o banco em produção.

---

## 1) Preparar o projeto

- `requirements.txt` deve incluir um driver de Postgres:
  - `psycopg[binary]` (psycopg3) **ou** `psycopg2-binary`.
- `database.py` lendo a URL do banco via `DATABASE_URL` (com fallback para SQLite local).
- Commit/push no GitHub: branch `main`.

---

## 2) Criar o banco no Render

1. Painel Render → **New → PostgreSQL**.
2. Copie a `DATABASE_URL` (formato `postgresql://USER:SENHA@HOST:5432/DB`).

---

## 3) Migrar dados do SQLite → Postgres

No seu computador (na raiz do projeto):

```powershell
# Ajuste o caminho do seu arquivo SQLite, se for outro
$env:SQLITE_URL="sqlite:///./app.sqlite"
$env:POSTGRES_URL="postgresql://USER:SENHA@HOST:5432/contratos_DB"

python scripts/migrar_sqlite_para_postgres.py
