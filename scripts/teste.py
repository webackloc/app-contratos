# scripts/teste_login_db.py
import os
from sqlalchemy import create_engine, text
from passlib.context import CryptContext

def main():
    url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
    print("URL definida?", bool(url))
    print("URL prefixo ok?", (url or "").startswith("postgresql"))
    if not url:
        print("ERRO: faltou DATABASE_URL/POSTGRES_URL")
        return

    engine = create_engine(url, pool_pre_ping=True)
    ctx = CryptContext(schemes=["bcrypt", "pbkdf2_sha256", "sha256_crypt"], deprecated="auto")

    with engine.connect() as c:
        print("select 1 ->", c.execute(text("select 1")).scalar())

        row = c.execute(text("""
            select id, username, is_active, role,
                   coalesce(password_hash, hashed_password, password) as pwd,
                   created_at, updated_at
              from users
             where trim(lower(username)) = :u
        """), {"u": "admin"}).mappings().first()

        print("Row admin =", dict(row) if row else None)
        if not row:
            print("NÃO encontrei o usuário 'admin' nesta base.")
            return

        h = row["pwd"]
        if not h:
            print("Usuário existe mas SEM senha salva (coluna nula).")
            return

        try:
            print("Esquema do hash:", ctx.identify(h))
            print("Senha confere? (NovaSenha@2025!) ->", ctx.verify("NovaSenha@2025!", h))
        except Exception as ex:
            print("Falha ao verificar hash:", type(ex).__name__, ex)

if __name__ == "__main__":
    main()
