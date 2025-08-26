import os
import sys
import sqlite3

def localizar_db():
    candidatos = ["contratos.db", os.path.join("data", "app.sqlite")]
    for p in candidatos:
        if os.path.exists(p):
            return p
    return None

def main():
    db = localizar_db()
    if not db:
        print("ERRO: nenhum banco encontrado (procurei contratos.db e data/app.sqlite).")
        sys.exit(2)

    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Descobre qual coluna guarda o nº do contrato no cabeçalho
    cols = [r[1] for r in cur.execute("PRAGMA table_info(contratos_cabecalho)")]
    cab_num = "contrato_num" if "contrato_num" in cols else ("contrato_n" if "contrato_n" in cols else None)

    print(f"DB: {db} | coluna no cabeçalho: {cab_num}")
    if not cab_num:
        print("ERRO: não achei a coluna do número de contrato no cabeçalho (esperado contrato_num ou contrato_n).")
        sys.exit(3)

    q = f"""
    SELECT i.id, i.ativo, i.contrato_n, i.cod_cli, i.nome_cli
    FROM contratos i
    LEFT JOIN contratos_cabecalho c
      ON TRIM(COALESCE(c.{cab_num}, '')) = TRIM(COALESCE(i.contrato_n, ''))
    WHERE c.id IS NULL
    ORDER BY i.id
    """
    rows = list(cur.execute(q))

    print(f"órfãos: {len(rows)}")
    if rows:
        print(f"{'ID':>6} {'Ativo':>10} {'Contrato':>12} {'CodCli':>8}  Nome")
        print("-" * 80)
        for r in rows:
            id_ = r["id"]
            ativo = "" if r["ativo"] is None else str(r["ativo"])
            contrato = "" if r["contrato_n"] is None else str(r["contrato_n"])
            cod_cli = "" if r["cod_cli"] is None else str(r["cod_cli"])
            nome = "" if r["nome_cli"] is None else str(r["nome_cli"])
            print(f"{id_:6} {ativo:>10} {contrato:>12} {cod_cli:>8}  {nome}")

    con.close()

if __name__ == "__main__":
    main()
