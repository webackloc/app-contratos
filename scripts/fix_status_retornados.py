# scripts/fix_status_retornados.py
# v1 (2025-08-22): Marca como RETORNADO todo contrato com data_retorno preenchida
#                  e status nulo/diferente de RETORNADO.

import os, sqlite3, urllib.parse as up

db_url = os.environ.get("DATABASE_URL")
if not db_url or "sqlite" not in db_url:
    raise SystemExit("DATABASE_URL ausente ou não-SQLite.")

db_path = up.unquote(db_url.split(":///")[1])

con = sqlite3.connect(db_path)
cur = con.cursor()

print("DB =", db_path)
cur.execute("""
    SELECT COUNT(*) FROM contratos
    WHERE data_retorno IS NOT NULL
      AND (status IS NULL OR status <> 'RETORNADO')
""")
pend = cur.fetchone()[0]
print("pendentes_para_marcar =", pend)

if pend:
    cur.execute("""
        UPDATE contratos
           SET status = 'RETORNADO'
         WHERE data_retorno IS NOT NULL
           AND (status IS NULL OR status <> 'RETORNADO')
    """)
    con.commit()

cur.execute("""
    SELECT id, ativo, status, data_retorno
      FROM contratos
     WHERE data_retorno IS NOT NULL
     ORDER BY id DESC
     LIMIT 10
""")
amostra = cur.fetchall()
print("amostra_pos_update =", amostra)

con.close()
print("OK.")
