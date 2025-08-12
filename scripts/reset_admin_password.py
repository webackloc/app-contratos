import os
import sys
import argparse
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

# --- argumentos ---
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
except Exception as ex:
    print("ERRO: 'passlib[bcrypt]' não instalado. Adicione 'passlib[bcrypt]>=1.7.4' ao requirements.txt.")
    sys.exit(1)

def main():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        # colunas existentes
        cols = [r[0] for r in conn.execute(text(
            "select column_name from information_schema.columns "
            "where table_schema='public' and table_name='users'"
        ))]

        # coluna da senha
        passcol = next((c for c in ("password_hash","hashed_password","password") if c in cols), None)
        if not passcol:
            print("ERRO: não achei coluna de senha na tabela 'users'. Colunas:", cols)
            sys.exit(1)

        # colunas NOT NULL sem default
        meta = conn.execute(text("""
            select column_name, is_nullable, column_default
            from information_schema.columns
            where table_schema='public' and table_name='users'
        """)).all()
        required = {name for (name, nullable, default) in meta
                    if nullable == 'NO' and default is None}

        h = bcrypt.hash(args.new_password)

        # tenta atualizar usuário existente
        upd = conn.execute(
            text(f"update users set {passcol}=:h where username=:u"),
            {"h": h, "u": args.username},
        )
        if upd.rowcount == 0:
            # criar usuário novo
            values = {"username": args.username, passcol: h}

            # campos comuns
            if "is_active" in cols: values["is_active"] = True
            if "is_admin"  in cols: values["is_admin"]  = True
            if "role"      in cols: values["role"]      = "admin"

            # preencher NOT NULL típicos
            if "email" in required and "email" not in values:
                values["email"] = f"{args.username}@local"
            if "name" in required and "name" not in values:
                values["name"] = args.username.title()

            now = datetime.now(timezone.utc)
            if "created_at" in required and "created_at" not in values:
                values["created_at"] = now
            if "updated_at" in required and "updated_at" not in values:
                values["updated_at"] = now

            insert_cols = list(values.keys())
            placeholders = ", ".join([f":{c}" for c in insert_cols])
            conn.execute(
                text(f"insert into users ({', '.join(insert_cols)}) values ({placeholders})"),
                values,
            )
            print(f"Usuário criado: {args.username} (coluna de senha: {passcol})")
        else:
            print(f"Senha atualizada: {args.username} (coluna de senha: {passcol})")

    print("Login agora com:")
    print(" - username:", args.username)
    print(" - senha   :", args.new_password)

if __name__ == "__main__":
    main()
