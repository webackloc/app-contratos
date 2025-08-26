# recalc_valor_presente_v2.py
# v1.1 — cria a coluna valor_presente se faltar e recalcula com base em valor_mensal * meses_restantes

import os, sys, sqlite3

def get_db_path() -> str:
    here = os.getcwd()
    cand = os.path.join(here, "contratos.db")
    if os.path.exists(cand):
        return cand
    cand2 = os.path.join(here, "data", "app.sqlite")
    if os.path.exists(cand2):
        return cand2
    raise SystemExit("Base não encontrada (contratos.db ou data/app.sqlite).")

db = get_db_path()
con = sqlite3.connect(db)
cur = con.cursor()

# Descobre as colunas atuais
cols = [r[1] for r in cur.execute("PRAGMA table_info(contratos)")]

# Valida colunas necessárias para o cálculo
if "valor_mensal" not in cols:
    con.close()
    sys.exit("ERRO: coluna 'valor_mensal' não existe na tabela contratos.")
if "meses_restantes" not in cols:
    con.close()
    sys.exit("ERRO: coluna 'meses_restantes' não existe na tabela contratos.")

# Cria a coluna valor_presente se faltar
if "valor_presente" not in cols:
    print("Criando coluna 'valor_presente' em contratos…")
    cur.execute("ALTER TABLE contratos ADD COLUMN valor_presente REAL")
    con.commit()

# Recalcula (fallback sem desconto)
cur.execute("""
UPDATE contratos
SET valor_presente = ROUND(COALESCE(valor_mensal,0) * COALESCE(meses_restantes,0), 2)
""")
print("linhas afetadas:", cur.rowcount)

con.commit()
con.close()
print("OK — valor_presente recalculado em:", db)
