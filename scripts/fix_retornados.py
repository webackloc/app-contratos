# v1 (2025-08-22): normaliza status conforme data_retorno; trata nulos como ATIVO
import os, sqlite3, urllib.parse as up

def get_db_path():
    url = os.environ.get("DATABASE_URL")
    if not url or "sqlite" not in url:
        raise SystemExit("DATABASE_URL não aponta para SQLite.")
    return up.unquote(url.split(":///")[1])

def main():
    db = get_db_path()
    con = sqlite3.connect(db)
    cur = con.cursor()

    # Antes
    total = cur.execute("SELECT COUNT(*) FROM contratos").fetchone()[0]
    by_status = cur.execute("SELECT COALESCE(status,'(NULL)'), COUNT(*) FROM contratos GROUP BY status").fetchall()
    print("[ANTES] total:", total, "| por status:", by_status)

    # 1) Quem tem data_retorno => RETORNADO
    cur.execute("""
        UPDATE contratos
           SET status = 'RETORNADO'
         WHERE data_retorno IS NOT NULL
           AND (status IS NULL OR status <> 'RETORNADO')
    """)
    print("Atualizados para RETORNADO (data_retorno IS NOT NULL):", cur.rowcount)

    # 2) Quem segue com status nulo => ATIVO
    cur.execute("""
        UPDATE contratos
           SET status = 'ATIVO'
         WHERE status IS NULL
    """)
    print("Atualizados para ATIVO (status NULL):", cur.rowcount)

    con.commit()

    # Depois
    total = cur.execute("SELECT COUNT(*) FROM contratos").fetchone()[0]
    by_status = cur.execute("SELECT status, COUNT(*) FROM contratos GROUP BY status").fetchall()
    print("[DEPOIS] total:", total, "| por status:", by_status)

    con.close()

if __name__ == "__main__":
    main()
