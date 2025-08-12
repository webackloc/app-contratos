python - <<'PY'
import os
from sqlalchemy import create_engine, text
from passlib.context import CryptContext

u = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
e = create_engine(u, pool_pre_ping=True)
ctx = CryptContext(schemes=["bcrypt","pbkdf2_sha256","sha256_crypt"], deprecated="auto")

with e.connect() as c:
    row = c.execute(text("""
        select id, username, is_active, role,
               coalesce(password_hash, hashed_password, password) as pwd,
               created_at, updated_at
          from users
         where username = :u
    """), {"u":"admin"}).mappings().first()
    print("Row admin =", row)

    if row and row["pwd"]:
        print("Esquema:", ctx.identify(row["pwd"]))
        print("Senha confere?:", ctx.verify("NovaSenha@2025!", row["pwd"]))
PY
