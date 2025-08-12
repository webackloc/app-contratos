# scripts/ajustar_sequences_postgres.py
# Ajusta sequences/identities para PKs após import "manual" de IDs.
# Usa POSTGRES_URL do ambiente (mesma que você já testou).

import os
from sqlalchemy import create_engine, text

POSTGRES_URL = os.getenv("POSTGRES_URL")
if not POSTGRES_URL:
    raise SystemExit("Defina POSTGRES_URL (postgresql://... ou postgresql+psycopg://...)")

engine = create_engine(POSTGRES_URL, pool_pre_ping=True)

# Liste aqui as tabelas e a coluna PK (se for diferente de 'id', ajuste)
ALVOS = [
    ("public", "contratos_cabecalho", "id"),
    ("public", "contratos", "id"),
]

def ajustar(conn, schema, tabela, pk):
    fq = f"{schema}.{tabela}"
    # tenta sequência do tipo SERIAL
    seq = conn.execute(text("SELECT pg_get_serial_sequence(:tbl, :col)"),
                       {"tbl": fq, "col": pk}).scalar()
    max_id = conn.execute(text(f"SELECT COALESCE(MAX({pk}), 0) FROM {fq}")).scalar() or 0
    proximo = max_id + 1 if max_id > 0 else 1
    if seq:
        # SERIAL -> usa setval
        conn.execute(text("SELECT setval(:seq, :val, :is_called)"),
                     {"seq": seq, "val": proximo, "is_called": False})
        print(f"[OK] {fq}: sequence {seq} ajustada para iniciar em {proximo}")
    else:
        # IDENTITY -> faz RESTART
        conn.execute(text(f"ALTER TABLE {fq} ALTER COLUMN {pk} RESTART WITH {proximo}"))
        print(f"[OK] {fq}: IDENTITY reiniciada para {proximo}")

def main():
    with engine.begin() as conn:
        for (schema, tabela, pk) in ALVOS:
            ajustar(conn, schema, tabela, pk)

if __name__ == "__main__":
    main()
