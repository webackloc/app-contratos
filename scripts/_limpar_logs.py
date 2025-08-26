$py = @'
import sys
from database import engine
from sqlalchemy import inspect, text

def main():
    if len(sys.argv) < 2:
        raise SystemExit("Uso: python _limpar_logs.py <ativo>")

    ativo = sys.argv[1]
    insp = inspect(engine)

    # Confere a tabela e colunas
    tables = insp.get_table_names()
    if "contratos_logs" not in tables:
        raise SystemExit(f"Tabela 'contratos_logs' não existe. Tabelas: {tables}")

    cols = [c["name"] for c in insp.get_columns("contratos_logs")]
    print("cols:", cols)

    # Coluna provável onde guarda o ativo
    col = None
    for candidate in ("ativo", "ativo_norm", "asset", "item", "id_ativo", "payload"):
        if candidate in cols:
            col = candidate
            break
    print("col_escolhida:", col)

    with engine.begin() as conn:
        if col == "payload":
            # Payload em texto/JSON: tenta pelos dois campos mais comuns
            where = "payload LIKE :p1 OR payload LIKE :p2"
            params = {
                "p1": f'%"ativo":"{ativo}"%',
                "p2": f'%"ativo_norm":"{ativo}"%',
            }
            before = conn.execute(text(f"SELECT COUNT(*) FROM contratos_logs WHERE {where}"), params).scalar() or 0
            print("Antes:", before)
            deleted = conn.execute(text(f"DELETE FROM contratos_logs WHERE {where}"), params).rowcount
            after = conn.execute(text(f"SELECT COUNT(*) FROM contratos_logs WHERE {where}"), params).scalar() or 0
            print("Removidos:", deleted, "Depois:", after)
        else:
            if not col:
                raise SystemExit("Nenhuma coluna de ativo encontrada em contratos_logs")
            q = text(f"SELECT COUNT(*) FROM contratos_logs WHERE {col}=:a")
            d = text(f"DELETE FROM contratos_logs WHERE {col}=:a")
            before = conn.execute(q, {"a": ativo}).scalar() or 0
            print("Antes:", before)
            deleted = conn.execute(d, {"a": ativo}).rowcount
            after = conn.execute(q, {"a": ativo}).scalar() or 0
            print("Removidos:", deleted, "Depois:", after)

if __name__ == "__main__":
    main()
'@

Set-Content -Path .\_limpar_logs.py -Value $py -Encoding UTF8
