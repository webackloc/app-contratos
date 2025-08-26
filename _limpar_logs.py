import sys
from database import engine
from sqlalchemy import inspect, text

def main():
    if len(sys.argv) < 2:
        print("Uso: python _limpar_logs.py <ativo> [<hash> ...]")
        return

    ativo = sys.argv[1]
    extra_hashes = sys.argv[2:]

    insp = inspect(engine)
    if "contratos_logs" not in insp.get_table_names():
        print("Tabela 'contratos_logs' não encontrada.")
        return

    cols = [c["name"] for c in insp.get_columns("contratos_logs")]
    print("cols:", cols)

    total_deleted = 0
    with engine.begin() as conn:
        # Tenta por colunas explícitas de ativo
        for col in ("ativo", "ativo_norm", "asset", "item", "id_ativo"):
            if col in cols:
                q = text(f"SELECT COUNT(*) FROM contratos_logs WHERE {col}=:a")
                d = text(f"DELETE FROM contratos_logs WHERE {col}=:a")
                before = conn.execute(q, {"a": ativo}).scalar() or 0
                deleted = conn.execute(d, {"a": ativo}).rowcount
                after = conn.execute(q, {"a": ativo}).scalar() or 0
                print(f"{col}: Antes={before} Removidos={deleted} Depois={after}")
                total_deleted += deleted

        # Fallback: procurar no payload (se armazenado como texto/JSON)
        if "payload" in cols:
            where = "payload LIKE :p1 OR payload LIKE :p2"
            params = {
                "p1": f'%\"ativo\":\"{ativo}\"%',
                "p2": f'%\"ativo_norm\":\"{ativo}\"%',
            }
            before = conn.execute(text(f"SELECT COUNT(*) FROM contratos_logs WHERE {where}"), params).scalar() or 0
            deleted = conn.execute(text(f"DELETE FROM contratos_logs WHERE {where}"), params).rowcount
            after = conn.execute(text(f"SELECT COUNT(*) FROM contratos_logs WHERE {where}"), params).scalar() or 0
            print(f"payload: Antes={before} Removidos={deleted} Depois={after}")
            total_deleted += deleted

        # Opcional: apagar hashes específicos passados como argumentos extras
        if extra_hashes and "mov_hash" in cols:
            for h in extra_hashes:
                b = conn.execute(text("SELECT COUNT(*) FROM contratos_logs WHERE mov_hash=:h"), {"h": h}).scalar() or 0
                d = conn.execute(text("DELETE FROM contratos_logs WHERE mov_hash=:h"), {"h": h}).rowcount
                a = conn.execute(text("SELECT COUNT(*) FROM contratos_logs WHERE mov_hash=:h"), {"h": h}).scalar() or 0
                print(f"mov_hash={h[:8]}...: Antes={b} Removidos={d} Depois={a}")
                total_deleted += d

    print("TOTAL removidos:", total_deleted)

if __name__ == "__main__":
    main()
