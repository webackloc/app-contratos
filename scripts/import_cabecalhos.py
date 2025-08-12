#!/usr/bin/env python3
# scripts/import_cabecalhos.py
# Versão 1.0.0 — 11/08/2025
# CHANGELOG:
# 1.0.0: Importador simples de cabeçalhos (CSV) com:
#        - detecção automática de delimitador/encoding
#        - aliases de colunas mais comuns
#        - validações de campos obrigatórios
#        - estratégia de duplicados: skip | update | error
#        - dry-run por padrão (--commit para gravar)
#
# Uso:
#   python scripts/import_cabecalhos.py caminho/arquivo.csv [--commit] [--on-duplicate skip|update|error]
#   Opções: --encoding utf-8-sig|latin1|utf-8 | --delimiter auto|;|,|||\\t

import os, sys, csv, argparse, json
from datetime import datetime

# Garanta que estamos na raiz do projeto (onde ficam database.py e models.py)
ROOT = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(ROOT, ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from database import SessionLocal  # usa sua conexão existente
from models import ContratoCabecalho  # usa seu modelo existente
from sqlalchemy.orm import Session
from sqlalchemy import func

__version__ = "1.0.0"

# ------------------ Helpers ------------------ #
def normalize_header(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "_").replace("-", "_")

ALIASES = {
    "contrato": ["contrato", "contrato_n", "contrato_num", "numero_contrato", "num_contrato", "n_contrato", "contrato n"],
    "nome_cliente": ["nome_cliente", "cliente", "razao_social", "razão_social", "nome do cliente", "nome"],
    "cnpj": ["cnpj", "documento", "cnpjs"],
    "prazo_contratual": ["prazo_contratual", "prazo", "periodo", "período", "periodo_contratual", "período contratual"],
    "indice_reajuste": ["indice_reajuste", "indice", "índice", "indice de reajuste", "índice de reajuste"],
    "vendedor": ["vendedor", "seller", "responsavel", "responsável"],
}

OBRIGATORIAS = ["contrato", "nome_cliente", "cnpj", "prazo_contratual", "indice_reajuste", "vendedor"]

def detect_delimiter(sample: str) -> str:
    try:
        return csv.Sniffer().sniff(sample[:2000], delimiters=[",", ";", "|", "\t"]).delimiter
    except Exception:
        return ";"

def resolve_indexes(header: list[str], mapping_json: str | None = None) -> dict:
    header_norm = [normalize_header(h) for h in header]
    index_map = {h: i for i, h in enumerate(header_norm)}
    mapping = {}
    if mapping_json:
        try:
            mapping = json.loads(mapping_json)
        except Exception:
            pass

    def idx_for(canon: str) -> int | None:
        # 1) mapping explícito
        if canon in mapping:
            return index_map.get(normalize_header(str(mapping[canon])))
        # 2) aliases
        for a in ALIASES.get(canon, []):
            i = index_map.get(normalize_header(a))
            if i is not None:
                return i
        # 3) nome exato
        return index_map.get(normalize_header(canon))

    out = {k: idx_for(k) for k in set(OBRIGATORIAS)}
    return out

def get_contract_field(model) -> str:
    cols = set(model.__table__.columns.keys())
    for name in ("contrato_n", "contrato_num", "numero_contrato", "num_contrato", "n_contrato"):
        if name in cols:
            return name
    # fallback: contrato_n (vai quebrar se não existir)
    return "contrato_n"

def to_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        try:
            return int(float(str(x).replace(",", ".").strip()))
        except Exception:
            return None

# ------------------ Importador ------------------ #
def importar_cabecalhos(
    csv_path: str,
    commit: bool = False,
    delimiter: str = "auto",
    encoding: str = "utf-8-sig",
    on_duplicate: str = "skip",   # skip | update | error
    mapping_json: str | None = None,
):
    if not os.path.exists(csv_path):
        print(f"ERRO: arquivo não encontrado: {csv_path}")
        sys.exit(1)

    with open(csv_path, "rb") as f:
        raw = f.read()

    try:
        text = raw.decode(encoding, errors="replace")
    except Exception:
        print(f"AVISO: não consegui usar encoding='{encoding}', tentando latin1…")
        text = raw.decode("latin1", errors="replace")

    use_delim = detect_delimiter(text) if delimiter == "auto" else {";": ";", ",": ",", "|": "|", "\\t": "\t"}.get(delimiter, delimiter)

    reader = csv.reader(text.splitlines(), delimiter=use_delim)
    try:
        header = next(reader)
    except StopIteration:
        print("ERRO: CSV vazio.")
        sys.exit(1)

    idx = resolve_indexes(header, mapping_json)
    faltando = [c for c in OBRIGATORIAS if idx.get(c) is None]
    if faltando:
        print(f"ERRO: Colunas obrigatórias ausentes no CSV: {faltando}")
        print("Header lido:", header)
        sys.exit(1)

    contract_field = get_contract_field(ContratoCabecalho)
    print(f"→ Delimitador: {repr(use_delim)}  |  Encoding: {encoding}")
    print(f"→ Campo de contrato no modelo: {contract_field}")
    print(f"→ Estratégia duplicados: {on_duplicate}  |  {'DRY-RUN (não grava)' if not commit else 'COMMIT (grava)'}")
    print("-" * 80)

    db: Session = SessionLocal()
    inserted = updated = skipped = errors = 0
    erros_detalhes: list[dict] = []

    def get(row, c):
        i = idx.get(c)
        return (row[i].strip() if (i is not None and i < len(row)) else "").strip()

    try:
        for ln, row in enumerate(reader, start=2):  # inicia em 2 por causa do header
            try:
                dados = {
                    "contrato": get(row, "contrato"),
                    "nome_cliente": get(row, "nome_cliente"),
                    "cnpj": get(row, "cnpj"),
                    "prazo_contratual": to_int(get(row, "prazo_contratual")),
                    "indice_reajuste": get(row, "indice_reajuste"),
                    "vendedor": get(row, "vendedor"),
                }
                # validações
                missing = [k for k in OBRIGATORIAS if not dados.get(k)]
                if missing or dados["prazo_contratual"] is None:
                    errors += 1
                    erros_detalhes.append({"linha": ln, "erro": f"Campos faltando/invalidos: {missing or []}; prazo_contratual={dados['prazo_contratual']}"})
                    continue

                # monta objeto
                kwargs = dict(
                    nome_cliente=dados["nome_cliente"],
                    cnpj=dados["cnpj"],
                    prazo_contratual=int(dados["prazo_contratual"]),
                    indice_reajuste=str(dados["indice_reajuste"]),
                    vendedor=dados["vendedor"],
                )

                # upsert por contrato
                existente = db.query(ContratoCabecalho).filter(
                    getattr(ContratoCabecalho, contract_field) == dados["contrato"]
                ).first()

                if existente:
                    if on_duplicate == "skip":
                        skipped += 1
                        continue
                    elif on_duplicate == "error":
                        errors += 1
                        erros_detalhes.append({"linha": ln, "erro": f"Contrato já existe: {dados['contrato']}"})
                        continue
                    elif on_duplicate == "update":
                        if commit:
                            existente.nome_cliente = kwargs["nome_cliente"]
                            existente.cnpj = kwargs["cnpj"]
                            existente.prazo_contratual = kwargs["prazo_contratual"]
                            existente.indice_reajuste = kwargs["indice_reajuste"]
                            existente.vendedor = kwargs["vendedor"]
                        updated += 1
                        continue
                else:
                    if commit:
                        novo = ContratoCabecalho(**kwargs)
                        setattr(novo, contract_field, dados["contrato"])
                        db.add(novo)
                    inserted += 1
            except Exception as e:
                errors += 1
                erros_detalhes.append({"linha": ln, "erro": str(e)})

        if commit:
            db.commit()
    finally:
        db.close()

    print("-" * 80)
    print("RESUMO:")
    print(f" Inseridos: {inserted}")
    print(f" Atualizados: {updated}")
    print(f" Ignorados (duplicado/skip): {skipped}")
    print(f" Erros: {errors}")

    if erros_detalhes:
        os.makedirs("runtime", exist_ok=True)
        out = {
            "executed_at": datetime.now().isoformat(),
            "arquivo": os.path.abspath(csv_path),
            "inserted": inserted,
            "updated": updated,
            "skipped": skipped,
            "errors": errors,
            "detalhes": erros_detalhes[:200],  # limita para não explodir o arquivo
        }
        path = os.path.join("runtime", "import_cabecalhos_result.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"→ Detalhes de erros salvos em: {path}")

def main():
    ap = argparse.ArgumentParser(description="Importa cabeçalhos de contratos (CSV) sem alterar o app.")
    ap.add_argument("csv", help="Caminho para o arquivo CSV")
    ap.add_argument("--commit", action="store_true", help="Grava no banco (por padrão é dry-run)")
    ap.add_argument("--on-duplicate", choices=["skip", "update", "error"], default="skip", help="Ação quando contrato já existe")
    ap.add_argument("--delimiter", default="auto", help="Delimitador: auto | ; | , | | | \\t")
    ap.add_argument("--encoding", default="utf-8-sig", help="Encoding: utf-8-sig | latin1 | utf-8")
    ap.add_argument("--mapping", default=None, help="JSON com mapeamento de colunas {canon->nome no CSV}")
    args = ap.parse_args()

    importar_cabecalhos(
        csv_path=args.csv,
        commit=args.commit,
        delimiter=args.delimiter,
        encoding=args.encoding,
        on_duplicate=args.on_duplicate,
        mapping_json=args.mapping,
    )

if __name__ == "__main__":
    main()
