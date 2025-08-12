python - <<'PY'
import os, sys
from sqlalchemy import create_engine, text

# ---- ajuste aqui se quiser ----
USER = "admin"              # username a resetar
NEW  = "NovaSenha@2025!"    # senha provisória forte
# -------------------------------

# tenta usar passlib (bcrypt). Se não houver, falha.
try:
    from passlib.hash import bcrypt
except Exception as ex:
    print("Faltou 'passlib[bcrypt]' no requirements.txt:", ex)
    sys.exit(1)

e = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)

with e.begin() as c:
    # identifica a coluna de senha
    cols = [r[0] for r in c.execute(text(
        "select column_name from information_schema.columns "
        "where table_schema='public' and table_name='users'"
    ))]
    for passcol in ("password_hash", "hashed_password", "password"):
        if passcol in cols:
            break
    else:
        print("Não achei coluna de senha na tabela users. Colunas:", cols)
        sys.exit(1)

    h = bcrypt.hash(NEW)

    # tenta atualizar o usuário
    r = c.execute(
        text(f"update users set {passcol}=:h where username=:u"),
        {"h": h, "u": USER},
    )
    if r.rowcount == 0:
        # se não existe, cria com flags padrões se houverem
        values = {"username": USER, passcol: h}
        extras_cols = []
        if "is_admin" in cols:
            extras_cols.append("is_admin"); values["is_admin"] = True
        if "is_active" in cols:
            extras_cols.append("is_active"); values["is_active"] = True
        insert_cols = ["username", passcol] + extras_cols
        placeholders = ", ".join([f":{c}" for c in insert_cols])
        c.execute(text(f"insert into users ({', '.join(insert_cols)}) values ({placeholders})"), values)
        print(f"Usuário criado: {USER} (coluna de senha: {passcol})")
    else:
        print(f"Senha atualizada para usuário: {USER} (coluna de senha: {passcol})")

print("Use esta senha provisória para login agora:", NEW)
PY
