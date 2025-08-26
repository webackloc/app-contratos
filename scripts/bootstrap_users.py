# scripts/bootstrap_users.py
# v5 (2025-08-22): preenche created_at (NOT NULL sem default); não atualiza created_at em ON CONFLICT;
#                  mantém compat. com esquema existente; logs detalhados.
import os
import sqlite3
import urllib.parse as up
from typing import Dict, Any
from datetime import datetime

ADMIN_USER = "admin"
# bcrypt da senha: Admin@123
ADMIN_HASH = "$2b$12$H/4irdXPjDPvaMyagFn1zeuioPmSyxsTKokSP0Dz4YPH5nwVsI9iy"
ADMIN_EMAIL = "admin@local"
ADMIN_ROLE = "admin"
ADMIN_NAME = "Administrador"

def db_path_from_url(url: str) -> str:
    if not url or ":///" not in url or not url.startswith("sqlite"):
        raise SystemExit(
            "Defina DATABASE_URL para SQLite, ex.: "
            "sqlite:///C:/Users/SEU_USER/Documentos/app-contratos/contratos.db"
        )
    return up.unquote(url.split(":///")[1])

def table_exists(cur, name: str) -> bool:
    return bool(cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone())

def table_info(cur, table: str):
    # (cid, name, type, notnull, dflt_value, pk)
    return cur.execute(f'PRAGMA table_info("{table}")').fetchall()

def columns_map(cur, table: str) -> Dict[str, Dict[str, Any]]:
    info = table_info(cur, table)
    return {r[1]: {"type": (r[2] or ""), "notnull": int(r[3] or 0), "default": r[4], "pk": int(r[5] or 0)} for r in info}

def ensure_users_table(cur):
    if not table_exists(cur, "users"):
        cur.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username VARCHAR(120) NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT 1,
                role VARCHAR(50) NOT NULL DEFAULT 'user',
                email VARCHAR(255),
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                hashed_password VARCHAR,
                is_superuser INTEGER NOT NULL DEFAULT 0,
                full_name VARCHAR
            )
        """)

def ensure_indexes(cur):
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_users_username ON users(username)")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_users_id ON users(id)")

def build_admin_values(cols: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    vals: Dict[str, Any] = {}
    if "username" in cols:         vals["username"] = ADMIN_USER
    if "hashed_password" in cols:  vals["hashed_password"] = ADMIN_HASH
    if "password_hash" in cols:    vals["password_hash"] = ADMIN_HASH
    if "is_active" in cols:        vals["is_active"] = 1
    if "is_superuser" in cols:     vals["is_superuser"] = 1
    if "role" in cols:             vals["role"] = ADMIN_ROLE
    if "full_name" in cols:        vals["full_name"] = ADMIN_NAME
    if "email" in cols:            vals["email"] = ADMIN_EMAIL
    # created_at pode ser NOT NULL sem default no seu schema atual
    if "created_at" in cols:       vals["created_at"] = now_str
    return vals

def upsert_admin(cur):
    cols = columns_map(cur, "users")
    vals = build_admin_values(cols)

    # garantir defaults para NOT NULL sem default (além do created_at já tratado)
    for name, meta in cols.items():
        if meta["pk"] or name == "id":
            continue
        if meta["notnull"] and name not in vals and meta["default"] is None:
            t = (meta["type"] or "").upper()
            if name in ("password_hash", "hashed_password"):
                vals[name] = ADMIN_HASH
            elif name in ("is_active", "is_superuser"):
                vals[name] = 1
            elif name == "role":
                vals[name] = ADMIN_ROLE
            elif name == "email":
                vals[name] = ADMIN_EMAIL
            elif name == "full_name":
                vals[name] = ADMIN_NAME
            elif name == "created_at":
                from datetime import datetime
                vals[name] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            else:
                if "CHAR" in t or "TEXT" in t or "CLOB" in t or "VARCHAR" in t:
                    vals[name] = ""
                elif "INT" in t:
                    vals[name] = 0
                elif "DATE" in t or "TIME" in t:
                    # se for NOT NULL sem default, colocar timestamp
                    vals[name] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                else:
                    vals[name] = None

    if "username" not in vals:
        raise SystemExit("Esquema de 'users' não possui coluna 'username'.")

    col_list = list(vals.keys())
    placeholders = ",".join("?" * len(col_list))

    # não altere created_at em updates
    set_cols = [c for c in col_list if c not in ("username", "created_at")]
    set_clause = ", ".join(f"{c}=excluded.{c}" for c in set_cols) if set_cols else ""

    sql_upsert = (
        f"INSERT INTO users ({','.join(col_list)}) VALUES ({placeholders}) "
        f"ON CONFLICT(username) DO UPDATE SET {set_clause}" if set_clause else
        f"INSERT OR IGNORE INTO users ({','.join(col_list)}) VALUES ({placeholders})"
    )

    try:
        cur.execute(sql_upsert, [vals[c] for c in col_list])
    except sqlite3.OperationalError:
        # Fallback: UPDATE (sem mexer em created_at) -> se 0 linhas, INSERT IGNORE
        if set_cols:
            cur.execute(
                f"UPDATE users SET {', '.join(f'{c}=?' for c in set_cols)} WHERE username=?",
                [vals[c] for c in set_cols] + [vals["username"]],
            )
        if cur.rowcount == 0:
            cur.execute(
                f"INSERT OR IGNORE INTO users ({','.join(col_list)}) VALUES ({placeholders})",
                [vals[c] for c in col_list],
            )

def main():
    url = os.environ.get("DATABASE_URL")
    dbfile = db_path_from_url(url)
    print(f"DATABASE_URL = {url}")
    print(f"DB file = {dbfile} | existe? {os.path.exists(dbfile)}")

    con = sqlite3.connect(dbfile)
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys=ON")

    ensure_users_table(cur)
    ensure_indexes(cur)

    print("\n[users - colunas] PRAGMA table_info")
    print(table_info(cur, "users"))

    before = cur.execute("SELECT id, username, is_superuser, is_active, role, created_at FROM users ORDER BY id").fetchall()
    print("\n[users antes]")
    print(before)

    upsert_admin(cur)
    con.commit()

    after = cur.execute("SELECT id, username, is_superuser, is_active, role, created_at FROM users ORDER BY id").fetchall()
    print("\n[users depois]")
    print(after)

    print("\nOK: usuário 'admin' pronto. Login: admin / Admin@123")
    con.close()

if __name__ == "__main__":
    main()
