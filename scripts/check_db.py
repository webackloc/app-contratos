# scripts/check_db.py
# v3 (2025-08-22): inclui inspeção de contratos_cabecalho (colunas, índices e DDL),
# mantém checagens anteriores. Compatível com Windows/PowerShell.
import os
import sqlite3
import urllib.parse as up
from pathlib import Path

def _resolve_sqlite_path(db_url: str) -> str:
    """
    Aceita URLs do tipo sqlite:///C:/.../arquivo.db (ou sqlite:////C:/...).
    Retorna o caminho de arquivo decodificado (sem %20).
    """
    if not db_url:
        return ""
    # pega tudo depois do primeiro ':///'
    if ":///" in db_url:
        tail = db_url.split(":///")[1]
    else:
        # fallback: remove prefixo até o primeiro ':'
        tail = db_url.split(":", 1)[-1].lstrip("/")
    return up.unquote(tail)

def _print(title: str, value):
    print(title)
    print("->", value)
    print()

def main():
    url = os.environ.get("DATABASE_URL", "")
    print(f"DATABASE_URL = {url}")
    db_path = _resolve_sqlite_path(url)
    print(f"Arquivo .db = {db_path} | existe? {Path(db_path).exists()}\n")

    if not db_path:
        print("ERRO: DATABASE_URL vazio ou inválido. Defina a variável e rode novamente.")
        return

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # listas gerais
    tabelas = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    _print("[tabelas]", tabelas)

    # --- contratos (mantém do script anterior) ---
    print("--- Tabela: contratos")
    _print("  [colunas] PRAGMA table_info", cur.execute('PRAGMA table_info("contratos")').fetchall())
    idx = cur.execute('PRAGMA index_list("contratos")').fetchall()
    print("  [índices] PRAGMA index_list")
    print("  ->", idx)
    for ix in idx:
        ixname = ix[1]
        print(f"     - {ixname}:", cur.execute(f'PRAGMA index_info("{ixname}")').fetchall())
    print()

    # --- contratos_logs (mantém do script anterior) ---
    print("--- Tabela: contratos_logs")
    _print("  [colunas] PRAGMA table_info", cur.execute('PRAGMA table_info("contratos_logs")').fetchall())
    idx = cur.execute('PRAGMA index_list("contratos_logs")').fetchall()
    print("  [índices] PRAGMA index_list")
    print("  ->", idx)
    for ix in idx:
        ixname = ix[1]
        print(f"     - {ixname}:", cur.execute(f'PRAGMA index_info("{ixname}")').fetchall())
    print()

    # --- movimentacao_lotes (mantém) ---
    print("--- Tabela: movimentacao_lotes")
    _print("  [colunas] PRAGMA table_info", cur.execute('PRAGMA table_info("movimentacao_lotes")').fetchall())
    idx = cur.execute('PRAGMA index_list("movimentacao_lotes")').fetchall()
    print("  [índices] PRAGMA index_list")
    print("  ->", idx)
    for ix in idx:
        ixname = ix[1]
        print(f"     - {ixname}:", cur.execute(f'PRAGMA index_info("{ixname}")').fetchall())
    print()

    # --- movimentacao_itens (mantém) ---
    print("--- Tabela: movimentacao_itens")
    _print("  [colunas] PRAGMA table_info", cur.execute('PRAGMA table_info("movimentacao_itens")').fetchall())
    idx = cur.execute('PRAGMA index_list("movimentacao_itens")').fetchall()
    print("  [índices] PRAGMA index_list")
    print("  ->", idx)
    for ix in idx:
        ixname = ix[1]
        print(f"     - {ixname}:", cur.execute(f'PRAGMA index_info("{ixname}")').fetchall())
    print()

    # === NOVO: contratos_cabecalho ===
    print("--- Tabela: contratos_cabecalho")
    cab_cols = cur.execute('PRAGMA table_info("contratos_cabecalho")').fetchall()
    _print("  [colunas] PRAGMA table_info", cab_cols)
    cab_idx = cur.execute('PRAGMA index_list("contratos_cabecalho")').fetchall()
    print("  [índices] PRAGMA index_list")
    print("  ->", cab_idx)
    for ix in cab_idx:
        ixname = ix[1]
        print(f"     - {ixname}:", cur.execute(f'PRAGMA index_info("{ixname}")').fetchall())
    print()

    # DDLs úteis
    _print("[DDL contratos_cabecalho]",
           cur.execute('SELECT sql FROM sqlite_master WHERE type="table" AND name="contratos_cabecalho"').fetchall())
    _print("[DDL contratos]",
           cur.execute('SELECT sql FROM sqlite_master WHERE type="table" AND name="contratos"').fetchall())

    con.close()

if __name__ == "__main__":
    main()
