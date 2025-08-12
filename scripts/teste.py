# scripts/teste_login_db.py
import os
from sqlalchemy import create_engine, text
from passlib.context import CryptContext

def get_user_columns(conn, table="users"):
    cols = conn.execute(text("""
        select column_name
          from information_schema.columns
         where table_name = :t
           and table_schema = current_schema()
    """), {"t": table}).scalars().all()
    return set(c.lower() for c in cols)

def main():
    url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
    print("URL definida?", bool(url))
    print("URL prefixo ok?", (url or "").startswith("postgresql"))
    if not url:
        print("ERRO: faltou DATABASE_URL/POSTGRES_URL"); return

    engine = create_engine(url, pool_pre_ping=True)
    ctx = CryptContext(schemes=["bcrypt", "pbkdf2_sha256", "sha256_crypt"], deprecated="auto")

    with engine.connect() as c:
        print("select 1 ->", c.execute(text("select 1")).scalar())

        cols = get_user_columns(c)
        print("Colunas em users:", sorted(cols))

        def has(col): return col.lower() in cols

        select_parts = []
        # campos básicos (se não tiver, retorna NULL pra não quebrar)
        for col in ("id", "username"):
            select_parts.append(col if has(col) else f"NULL as {col}")
        select_parts.append("is_active" if has("is_active") else "NULL as is_active")
        select_parts.append("role" if has("role") else "NULL as role")

        # coluna de senha (usa a que existir)
        if has("password_hash"):
            pwd_expr = "password_hash"
        elif has("hashed_password"):
            pwd_expr = "hashed_password"
        elif has("password"):
            pwd_expr = "password"
        else:
            pwd_expr = "NULL"
        select_parts.append(f"{pwd_expr} as pwd")

        # datas (opcionais)
        select_parts.append("created_at" if has("created_at") else "NULL as created_at")
        select_parts.append("updated_at" if has("updated_at") else "NULL as updated_at")

        sql = f"""
            select {', '.join(select_parts)}
              from users
             where trim(lower(username)) = :u
        """
        row = c.execute(text(sql), {"u": "admin"}).mappings().first()

        # também mostra todos os "admin" (se houver duplicados)
        all_admin = c.execute(text("""
            select id, username, is_active, coalesce(role, '') as role
              from users
             where trim(lower(username))='admin'
             order by id
        """)).mappings().all()
        print(f"Total 'admin' encontrados: {len(all_admin)} -> IDs: {[r['id'] for r in all_admin]}")

        print("Row admin =", dict(row) if row else None)
        if not row:
            print("NÃO encontrei o usuário 'admin'."); return

        h = row["pwd"]
        if not h:
            print("Usuário existe, mas SEM hash de senha salvo (coluna nula)."); return

        # identificar e testar a senha
        try:
            print("Esquema do hash:", ctx.identify(h))
        except Exception as ex:
            print("Aviso ao identificar hash:", type(ex).__name__, ex)

        for senha in ("NovaSenha@2025!", "admin", "123456", "senha"):
            try:
                ok = ctx.verify(senha, h)
            except Exception:
                ok = False
            print(f"Teste senha '{senha}':", ok)

if __name__ == "__main__":
    main()
