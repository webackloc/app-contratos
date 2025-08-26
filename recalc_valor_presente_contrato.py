# recalc_valor_presente_contrato.py
import os
import sqlite3

DB = "contratos.db"
if not os.path.exists(DB):
    DB = os.path.join("data", "app.sqlite")

con = sqlite3.connect(DB)
cur = con.cursor()

# Garante a coluna alvo (NÃO cria se já existir)
cur.execute("""
PRAGMA table_info(contratos)
""")
cols = {row[1] for row in cur.fetchall()}
if "valor_presente_contrato" not in cols:
    raise SystemExit("Coluna 'valor_presente_contrato' não existe na tabela contratos. "
                     "Pare aqui e me avise.")

# Recalcula com base no valor_mensal * meses_restantes
cur.execute("""
UPDATE contratos
SET valor_presente_contrato =
    ROUND(COALESCE(valor_mensal,0) * COALESCE(meses_restantes,0), 2)
""")

con.commit()
con.close()
print("OK: valor_presente_contrato recalculado.")
