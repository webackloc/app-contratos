import os
import sys
import argparse
from sqlalchemy import create_engine, text

parser = argparse.ArgumentParser(description="Resetar/definir senha de um usuário (tabela 'users').")
parser.add_argument("--username", "-u", default=os.getenv("ADMIN_USERNAME", "admin"),
                    help="username a resetar (default: env ADMIN_USERNAME ou 'admin')")
parser.add_argument("--new-password", "-p", dest="new_password",
                    default=os.getenv("ADMIN_NEW_PASSWORD", "NovaSenha@2025!"),
                    help="nova senha (default: env ADMIN_NEW_PASSWORD ou 'NovaSenha@2025!')")
args = parser.parse_args()

DB_URL = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
if not DB_URL:
    print("ERRO: defina DATABASE_URL (ou POSTGRES_URL) nas variáveis de ambiente.")
    sys.exit(1)

# hash da senha
try:
    from passlib.hash import bcrypt
except Exception:
    print("ERRO: 'passlib[bcrypt]' não instalado. Garanta no requirements.txt: passlib[bcrypt]>=1.7.4 e bcrypt==3.2.2")
    sys.exit(1)

def main():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        # pegar colunas reais da tabela
        cols = [r[0] for r in conn.execute(text(
            "select column_name from information_schema.columns "
            "where table_schema='public' and table_name='users'"
        ))]

        # identificar coluna de senha
        passcol = next((c for c in ("password_hash","hashed_password","password") if c in cols), None)
        if not passcol:
            print("ERRO: não achei coluna de senha na tabela 'users'. Colunas:", cols)
            sys.exit(1)

        # se existir, vamos também atualizar updated_at no update
        has_updated = "updated_at" in cols
        has_created = "created_at" in cols

        h = bcrypt.hash(args.new_password)

        # 1) tentar UPDATE
        if has_updated:
            upd_sql = text(f"""
                update users
                   set {passcol}=:h,
                       updated_at = now()
                 where username=:u
            """)
        else:
            upd_sql = text(f"update users set {passcol}=:h where username=:u")

        upd = conn.execute(upd_sql, {"h": h, "u": args.username})

        if upd.rowcount > 0:
            print(f"Senha atualizada: {args.username} (coluna de senha: {passcol})")
            return

        # 2) se não atualizou, INSERT novo usuário
        # campos comuns que vamos tentar inserir se existirem
        values = {"username": args.username, passcol: h}
        if "is_active" in cols: values["is_active"] = True
        if "is_admin"  in cols: values["is_admin"]  = True
        if "role"      in cols: values["role"]      = "admin"
        if "email"     in cols: values.setdefault("email", f"{args.username}@local")
        if "name"      in cols: values.setdefault("name", args.username.title())

        insert_cols = ["username", passcol]
        if "is_active" in values: insert_cols.append("is_active")
        if "is_admin"  in values: insert_cols.append("is_admin")
        if "role"      in values: insert_cols.append("role")
        if "email"     in values: insert_cols.append("email")
        if "name"      in values: insert_cols.append("name")

        # timestamps no lado do servidor:
        # se a coluna existir, vamos pôr "now()" direto no SQL (sem parâmetro)
        if has_created: insert_cols.append("created_at")
        if has_updated: insert_cols.append("updated_at")

        # montar a parte dos VALUES
        value_parts = []
        params = {}
        for c in insert_cols:
            if c in ("created_at", "updated_at"):
                value_parts.append("now()")
            else:
                value_parts.append(f":{c}")
                params[c] = values[c]

        ins_sql = text(f"""
            insert into users ({", ".join(insert_cols)})
            values ({", ".join(value_parts)})
        """)

        conn.execute(ins_sql, params)
        print(f"Usuário criado: {args.username} (coluna de senha: {passcol})")

        print("Credenciais:")
        print(" - username:", args.username)
        print(" - senha   :", args.new_password)

if __name__ == "__main__":
    main()
