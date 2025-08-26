# recalc_valor_presente.py
# v1.0 (hotfix) — recalcula valor_presente com base em valor_mensal * meses_restantes

import os, sqlite3

def get_db_path():
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

# aplica para todos; se preferir restringir só onde está 0/NULL, acrescente WHERE (valor_presente IS NULL OR valor_presente=0)
cur.execute("""
UPDATE contratos
SET valor_presente = ROUND(COALESCE(valor_mensal,0) * COALESCE(meses_restantes,0), 2)
""")
print("linhas afetadas:", cur.rowcount)

con.commit()
con.close()
print("OK — valor_presente recalculado em:", db)
