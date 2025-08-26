# scripts/fix_troca_1121_1315.py
# Versão: 1.0 (2025-08-21)
# Ajuste manual dos ativos da troca: 1121 (ENVIO) e 1315 (RETORNO) – OS 5910.
# - 1121: ATIVO, limpa data_retorno e marca data_troca
# - 1315: RETORNADO com data_retorno
#
# Lê o caminho do banco de:
#   1) env DATABASE_URL (ex.: sqlite:///C:/.../contratos.db)
#   2) fallback: <raiz do projeto>\contratos.db

import os
import re
import sqlite3
import shutil
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

# >>> AJUSTE A DATA DA TROCA/RETORNO SE PRECISAR <<<
DATA_TROCA_RET = "2025-08-13"  # formato ISO AAAA-MM-DD

ATIVO_ENVIO = "1121"
ATIVO_RETORNO = "1315"
OS_REF = "5910"  # opcional – só para constar na mensagem do log

def resolve_db_path() -> str:
    u = os.environ.get("DATABASE_URL", "").strip()
    if u.startswith("sqlite:///"):
        # sqlite:///C:/caminho/arquivo.db
        m = re.match(r"^[^:]+:///+(.*)$", u)
        if m:
            return unquote(m.group(1))
    # fallback: contratos.db na raiz do projeto (pasta pai de scripts)
    root = Path(__file__).resolve().parents[1]
    return str(root / "contratos.db")

def dump_estado(cur, ativo: str):
    print(f"\n[estado] ativo {ativo}")
    rows = cur.execute(
        """SELECT id, cabecalho_id, cod_cli, status, data_envio, data_retorno, data_troca, tp_transacao
           FROM contratos WHERE ativo=? ORDER BY id""",
        (ativo,),
    ).fetchall()
    for r in rows:
        print(" ->", r)
    if not rows:
        print(" -> (nenhum registro)")

def insert_log(cur, contrato_id: int, cab_id: int, cod_cli: str | None,
               ativo: str, tp: str, data_mov: str, msg: str):
    # Insere só os campos que existem (schema pode variar um pouco)
    cols = {c[1] for c in cur.execute("PRAGMA table_info('contratos_logs')").fetchall()}
    fields = []
    values = []
    params = []

    def add(k, v):
        fields.append(k)
        params.append(v)

    if "contrato_id" in cols:
        add("contrato_id", contrato_id)
    if "contrato_cabecalho_id" in cols:
        add("contrato_cabecalho_id", cab_id)
    if "cod_cli" in cols:
        add("cod_cli", cod_cli)
    if "ativo" in cols:
        add("ativo", ativo)
    if "tp_transacao" in cols:
        add("tp_transacao", tp)
    if "data_mov" in cols:
        add("data_mov", data_mov)
    if "status" in cols:
        add("status", "OK")
    if "mensagem" in cols:
        add("mensagem", msg)

    if not fields:
        return  # nada compatível, ignora

    sql = f"INSERT INTO contratos_logs ({', '.join(fields)}) VALUES ({', '.join(['?']*len(fields))})"
    cur.execute(sql, tuple(params))

def main():
    db_file = resolve_db_path()
    print("DATABASE_URL ->", os.environ.get("DATABASE_URL", "(vazio)"))
    print("DB file      ->", db_file, "| existe?", Path(db_file).exists())

    if not Path(db_file).exists():
        raise SystemExit("Arquivo .db não encontrado.")

    # backup
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    bkp = Path(db_file).with_suffix(f".db.bak.{ts}")
    shutil.copyfile(db_file, bkp)
    print("Backup feito em:", bkp)

    con = sqlite3.connect(db_file)
    cur = con.cursor()

    # Estado antes
    dump_estado(cur, ATIVO_ENVIO)
    dump_estado(cur, ATIVO_RETORNO)

    # --- 1315 deve ficar RETORNADO ---
    # pega (se existir) um registro qualquer para log
    row1315 = cur.execute(
        "SELECT id, cabecalho_id, cod_cli FROM contratos WHERE ativo=? ORDER BY id LIMIT 1",
        (ATIVO_RETORNO,),
    ).fetchone()
    cur.execute(
        """UPDATE contratos
           SET status='RETORNADO',
               data_retorno=?,
               tp_transacao='RETORNO'
           WHERE ativo=?""",
        (DATA_TROCA_RET, ATIVO_RETORNO),
    )
    if row1315:
        insert_log(
            cur,
            contrato_id=row1315[0],
            cab_id=row1315[1],
            cod_cli=row1315[2],
            ativo=ATIVO_RETORNO,
            tp="RETORNO",
            data_mov=DATA_TROCA_RET,
            msg=f"Correção manual OS {OS_REF}",
        )

    # --- 1121 deve ficar ATIVO ---
    row1121 = cur.execute(
        "SELECT id, cabecalho_id, cod_cli, COALESCE(data_troca,'') FROM contratos WHERE ativo=? ORDER BY id LIMIT 1",
        (ATIVO_ENVIO,),
    ).fetchone()
    # marca ATIVO, limpa data_retorno; define data_troca se ainda não houver
    if row1121 and (not row1121[3]):
        cur.execute(
            """UPDATE contratos
               SET status='ATIVO',
                   data_retorno=NULL,
                   data_troca=?,
                   tp_transacao='ENVIO'
               WHERE ativo=?""",
            (DATA_TROCA_RET, ATIVO_ENVIO),
        )
    else:
        cur.execute(
            """UPDATE contratos
               SET status='ATIVO',
                   data_retorno=NULL,
                   tp_transacao='ENVIO'
               WHERE ativo=?""",
            (ATIVO_ENVIO,),
        )
    if row1121:
        insert_log(
            cur,
            contrato_id=row1121[0],
            cab_id=row1121[1],
            cod_cli=row1121[2],
            ativo=ATIVO_ENVIO,
            tp="ENVIO",
            data_mov=DATA_TROCA_RET,
            msg=f"Correção manual OS {OS_REF}",
        )

    con.commit()

    # Estado depois
    dump_estado(cur, ATIVO_ENVIO)
    dump_estado(cur, ATIVO_RETORNO)

    con.close()
    print("\nOK! Ajuste concluído.")

if __name__ == "__main__":
    main()
