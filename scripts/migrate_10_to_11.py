#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
migrate_10_to_11.py  |  Versão: 1.1.0  (2025-08-26)
Objetivo: migrar estrutura do schema de v1.0 para v1.1 sem levar dados.
- Idempotente: roda quantas vezes precisar.
- Foca nas tabelas usadas pelo app: contratos_cabecalho, contratos, logs_importacao, import_lotes, app_meta.
- Cria colunas/índices ausentes.
- (Opcional) Recalcula backlog = valor_mensal * meses_restantes, se --calc-backlog for passado.

Uso:
  python migrate_10_to_11.py --db "C:\\caminho\\para\\dados.db" [--calc-backlog]

Por padrão, tenta DB em:
  ./dados.db
"""
import argparse, os, sqlite3, sys
from datetime import datetime

V_TARGET = "1.1"

def connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def table_has_column(conn, table, column):
    cur = conn.execute(f"PRAGMA table_info('{table}')")
    cols = [r[1].lower() for r in cur.fetchall()]
    return column.lower() in cols

def ensure_table(conn, name, create_sql):
    # cria tabela se não existir
    conn.execute(f"CREATE TABLE IF NOT EXISTS {name} (dummy_col__will_be_dropped INTEGER)")
    # se for tabela “dummy” recém-criada, substitui pelo schema correto
    # detecta pela existência da coluna dummy
    if table_has_column(conn, name, "dummy_col__will_be_dropped"):
        conn.execute("PRAGMA foreign_keys=OFF;")
        conn.execute("BEGIN;")
        try:
            conn.execute(f"DROP TABLE {name};")
            conn.execute(create_sql)
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise
        finally:
            conn.execute("PRAGMA foreign_keys=ON;")

def ensure_index(conn, name, create_sql):
    conn.execute(create_sql)

def add_column_if_missing(conn, table, col_def):
    """
    col_def exemplo: "cod_cli TEXT", "meses_restantes INTEGER DEFAULT 0"
    """
    col_name = col_def.split()[0]
    if not table_has_column(conn, table, col_name):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")

def set_meta(conn, key, value):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS app_meta (
          key TEXT PRIMARY KEY,
          value TEXT
        )
    """)
    cur = conn.execute("SELECT value FROM app_meta WHERE key=?", (key,))
    if cur.fetchone() is None:
        conn.execute("INSERT INTO app_meta(key, value) VALUES(?,?)", (key, value))
    else:
        conn.execute("UPDATE app_meta SET value=? WHERE key=?", (value, key))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=os.path.join(os.getcwd(), "dados.db"), help="Caminho do arquivo SQLite (ex.: C:\\...\\dados.db)")
    p.add_argument("--calc-backlog", action="store_true", help="(Opcional) Preencher backlog=valor_mensal*meses_restantes para linhas existentes")
    args = p.parse_args()

    if not os.path.exists(args.db):
        print(f"[erro] DB não encontrado: {args.db}")
        sys.exit(2)

    conn = connect(args.db)
    try:
        # --- TABELAS-CHAVE (schema v1.1) ---
        contratos_cabecalho_sql = """
        CREATE TABLE IF NOT EXISTS contratos_cabecalho (
          id                INTEGER PRIMARY KEY,
          cod_cli           TEXT,
          nome_cliente      TEXT,
          cnpj              TEXT,
          contrato_num      TEXT,
          prazo_contratual  INTEGER,
          indice_reajuste   REAL,
          vendedor          TEXT
        );
        """
        ensure_table(conn, "contratos_cabecalho", contratos_cabecalho_sql)

        # Garantir colunas essenciais (caso a tabela já existisse em v1.0)
        add_column_if_missing(conn, "contratos_cabecalho", "cod_cli TEXT")
        add_column_if_missing(conn, "contratos_cabecalho", "nome_cliente TEXT")
        add_column_if_missing(conn, "contratos_cabecalho", "cnpj TEXT")
        add_column_if_missing(conn, "contratos_cabecalho", "contrato_num TEXT")
        add_column_if_missing(conn, "contratos_cabecalho", "prazo_contratual INTEGER")
        add_column_if_missing(conn, "contratos_cabecalho", "indice_reajuste REAL")
        add_column_if_missing(conn, "contratos_cabecalho", "vendedor TEXT")

        # Índices úteis
        ensure_index(conn, "idx_cab_contrato_num",
            "CREATE INDEX IF NOT EXISTS idx_cab_contrato_num ON contratos_cabecalho(contrato_num)")
        ensure_index(conn, "idx_cab_cod_cli",
            "CREATE INDEX IF NOT EXISTS idx_cab_cod_cli ON contratos_cabecalho(cod_cli)")

        contratos_sql = """
        CREATE TABLE IF NOT EXISTS contratos (
          id                      INTEGER PRIMARY KEY,
          cabecalho_id            INTEGER REFERENCES contratos_cabecalho(id) ON DELETE SET NULL,
          ativo                   TEXT,
          serial                  TEXT,
          cod_pro                 TEXT,
          descricao_produto       TEXT,
          cod_cli                 TEXT,
          nome_cli                TEXT,
          data_envio              TEXT,
          contrato_n              TEXT,
          valor_mensal            REAL,
          periodo_contratual      INTEGER,
          meses_restantes         INTEGER DEFAULT 0,
          valor_global_contrato   REAL,
          backlog                 REAL DEFAULT 0
        );
        """
        ensure_table(conn, "contratos", contratos_sql)

        # Garantir colunas essenciais
        add_column_if_missing(conn, "contratos", "cabecalho_id INTEGER")
        add_column_if_missing(conn, "contratos", "ativo TEXT")
        add_column_if_missing(conn, "contratos", "serial TEXT")
        add_column_if_missing(conn, "contratos", "cod_pro TEXT")
        add_column_if_missing(conn, "contratos", "descricao_produto TEXT")
        add_column_if_missing(conn, "contratos", "cod_cli TEXT")
        add_column_if_missing(conn, "contratos", "nome_cli TEXT")
        add_column_if_missing(conn, "contratos", "data_envio TEXT")
        add_column_if_missing(conn, "contratos", "contrato_n TEXT")
        add_column_if_missing(conn, "contratos", "valor_mensal REAL")
        add_column_if_missing(conn, "contratos", "periodo_contratual INTEGER")
        add_column_if_missing(conn, "contratos", "meses_restantes INTEGER DEFAULT 0")
        add_column_if_missing(conn, "contratos", "valor_global_contrato REAL")
        add_column_if_missing(conn, "contratos", "backlog REAL DEFAULT 0")

        ensure_index(conn, "idx_contratos_cabecalho",
            "CREATE INDEX IF NOT EXISTS idx_contratos_cabecalho ON contratos(cabecalho_id)")
        ensure_index(conn, "idx_contratos_contrato_n",
            "CREATE INDEX IF NOT EXISTS idx_contratos_contrato_n ON contratos(contrato_n)")
        ensure_index(conn, "idx_contratos_ativo",
            "CREATE INDEX IF NOT EXISTS idx_contratos_ativo ON contratos(ativo)")

        # Tabela de logs de importação (utilizada no menu de importações)
        logs_importacao_sql = """
        CREATE TABLE IF NOT EXISTS logs_importacao (
          id                      INTEGER PRIMARY KEY,
          lote_id                 INTEGER,
          tp_transacao            TEXT,
          acao                    TEXT,
          contrato_id             INTEGER,
          contrato_cabecalho_id   INTEGER,
          ativo                   TEXT,
          cod_cli                 TEXT,
          descricao               TEXT,
          status                  TEXT,
          mensagem                TEXT,
          mov_hash                TEXT UNIQUE,
          data_mov                TEXT,
          data_modificacao        TEXT DEFAULT (datetime('now'))
        );
        """
        ensure_table(conn, "logs_importacao", logs_importacao_sql)
        ensure_index(conn, "idx_logs_lote",  "CREATE INDEX IF NOT EXISTS idx_logs_lote  ON logs_importacao(lote_id)")
        ensure_index(conn, "idx_logs_tipo",  "CREATE INDEX IF NOT EXISTS idx_logs_tipo  ON logs_importacao(tp_transacao)")
        ensure_index(conn, "idx_logs_hash",  "CREATE UNIQUE INDEX IF NOT EXISTS idx_logs_hash ON logs_importacao(mov_hash)")

        # Tabela de lotes de importação (metadados do lote)
        import_lotes_sql = """
        CREATE TABLE IF NOT EXISTS import_lotes (
          id             INTEGER PRIMARY KEY,
          arquivo        TEXT,
          linhas_total   INTEGER,
          inseridos      INTEGER,
          atualizados    INTEGER,
          trocas         INTEGER,
          retornos       INTEGER,
          erros          INTEGER,
          status         TEXT,
          mensagem       TEXT,
          processado_em  TEXT,
          created_at     TEXT DEFAULT (datetime('now'))
        );
        """
        ensure_table(conn, "import_lotes", import_lotes_sql)

        # --- (Opcional) recalcular backlog para linhas existentes ---
        if args.calc_backlog:
            conn.execute("""
                UPDATE contratos
                SET backlog = COALESCE(valor_mensal,0) * COALESCE(meses_restantes,0)
            """)

        # Marcar versão de schema
        set_meta(conn, "schema_version", V_TARGET)
        set_meta(conn, "schema_version_set_at", datetime.now().isoformat(timespec="seconds"))

        conn.commit()
        print(f"[ok] Migração concluída. schema_version={V_TARGET}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
