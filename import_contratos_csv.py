# scripts/import_contratos_csv.py  — v1.0.0
# Uso:
#   python scripts/import_contratos_csv.py --csv dados/contratos.csv --taxa 0.01 --cabecalho-id 12
# Extras:
#   --encoding utf-8 --sep , --dry-run
#   --db-url sqlite:///./contratos.db   (se não conseguir importar a Session do seu projeto)

from __future__ import annotations
from typing import Optional, Dict, Any, Iterable, Tuple
from decimal import Decimal, InvalidOperation
from datetime import date, datetime
import argparse, csv, io, sys, os

# ----------------------------
# Tentativa de importar modelos e sessão do seu projeto
# ----------------------------
SessionLocal = None
get_db = None
Contrato = None
ContratoCabecalho = None

def _try_imports():
    global SessionLocal, get_db, Contrato, ContratoCabecalho
    # modelos
    for mod in ("models", "app.models"):
        try:
            m = __import__(mod, fromlist=["Contrato", "ContratoCabecalho"])
            Contrato = getattr(m, "Contrato", None)
            ContratoCabecalho = getattr(m, "ContratoCabecalho", None)
            if Contrato and ContratoCabecalho:
                break
        except Exception:
            pass
    # sessão
    # 1) database.SessionLocal
    try:
        dbm = __import__("database", fromlist=["SessionLocal"])
        SessionLocal = getattr(dbm, "SessionLocal", None)
    except Exception:
        pass
    # 2) main.SessionLocal ou main.get_db
    try:
        mm = __import__("main", fromlist=["SessionLocal", "get_db"])
        if SessionLocal is None:
            SessionLocal = getattr(mm, "SessionLocal", None)
        get_db = getattr(mm, "get_db", None)
    except Exception:
        pass

_try_imports()

# ----------------------------
# Fallback: criar sessão via --db-url, se necessário
# ----------------------------
def _build_session_from_url(db_url: str):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    engine = create_engine(db_url, future=True)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

def _get_session(db_url: Optional[str]):
    if SessionLocal is not None:
        return SessionLocal()
    if get_db is not None:
        # get_db normalmente é um generator; mas vamos abrir direto pela SessionLocal se existir
        try:
            return next(get_db())
        except Exception:
            pass
    if not db_url:
        raise RuntimeError(
            "Não foi possível localizar a Session do projeto. "
            "Informe --db-url (ex.: sqlite:///./app.db ou o mesmo usado no app)."
        )
    maker = _build_session_from_url(db_url)
    return maker()

# ----------------------------
# Utilidades de parsing e cálculo
# ----------------------------
_BR_MONTHS = {"jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,"jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12}

def _parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date):
        return value
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    # "MM/YYYY" ou "Mon/YYYY"
    try:
        if "/" in s and len(s) >= 7:
            left, right = s.split("/", 1)
            y = int(right)
            try:
                m = int(left)
            except ValueError:
                m = _BR_MONTHS.get(left.lower()[:3])
            if m:
                return date(y, m, 1)
    except Exception:
        pass
    return None

def _parse_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(".", "").replace(",", ".")  # 1.234,56 -> 1234.56
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None

def _months_between(d1: date, d2: date) -> int:
    """Meses inteiros (contando in/out) entre d1 e d2."""
    if d1 > d2:
        return 0
    months = (d2.year - d1.year) * 12 + (d2.month - d1.month)
    return months + 1

def _months_from_today_until(d2: date) -> int:
    today = date.today()
    if d2 <= today:
        return 0
    return _months_between(today.replace(day=1), d2.replace(day=1))

def _npv_fixed_payments(valor_parcela: Decimal, meses: int, taxa_mensal: Decimal) -> Decimal:
    """NPV de anuidade (parcelas mensais constantes)."""
    if meses <= 0 or valor_parcela is None:
        return Decimal("0.00")
    r = Decimal(taxa_mensal)
    if r <= 0:
        return (valor_parcela * Decimal(meses)).quantize(Decimal("0.01"))
    try:
        fator = (Decimal(1) - (Decimal(1) + r) ** (Decimal(-meses))) / r
        return (valor_parcela * fator).quantize(Decimal("0.01"))
    except Exception:
        # fallback
        total = Decimal("0")
        acc = Decimal(1) + r
        for k in range(1, meses + 1):
            total += valor_parcela / (acc ** k)
        return total.quantize(Decimal("0.01"))

def _first_attr(obj: Any, names: Iterable[str]) -> Tuple[str, Any]:
    for n in names:
        if hasattr(obj, n):
            return n, getattr(obj, n)
    return "", None

def _set_if_has(obj: Any, name: str, value: Any):
    if hasattr(obj, name):
        setattr(obj, name, value)

# ----------------------------
# Importador
# ----------------------------
def importar_csv(
    session,
    path_csv: str,
    taxa_mensal: Decimal,
    cabecalho_id: Optional[int],
    encoding: str,
    sep: str,
    dry_run: bool
):
    if not Contrato or not ContratoCabecalho:
        raise RuntimeError("Não foi possível importar modelos Contrato/ContratoCabecalho do seu projeto.")

    # Cabeçalho default (opcional)
    cabecalho_default = None
    if cabecalho_id:
        cabecalho_default = session.get(ContratoCabecalho, cabecalho_id)
        if not cabecalho_default:
            raise RuntimeError(f"Cabeçalho id={cabecalho_id} não encontrado.")

    # Mapeamento flexível de colunas
    colmap = {
        "numero": {"numero", "contrato", "numero_contrato", "n_contrato", "id_contrato"},
        "valor_parcela": {"valor_parcela", "vl_parcela", "parcela", "valor_mensal"},
        "parcelas_pagas": {"parcelas_pagas", "qtd_pagas", "pagas"},
        "inicio": {"inicio", "data_inicio", "competencia_inicio", "periodo_inicio"},
        "fim": {"fim", "data_fim", "competencia_fim", "periodo_fim"},
        "cabecalho_id": {"cabecalho_id", "id_cabecalho", "id_header"},
    }

    def get_row(row: Dict[str, Any], key: str) -> Any:
        for k in colmap.get(key, {key}):
            if k in row and row[k] not in (None, ""):
                return row[k]
        return None

    # Ler CSV
    with open(path_csv, "rb") as f:
        data = f.read()
    txt = None
    for enc in (encoding, "utf-8", "latin-1"):
        try:
            txt = data.decode(enc, errors="ignore")
            break
        except Exception:
            continue
    if txt is None:
        raise RuntimeError(f"Falha ao decodificar CSV. Tentado: {encoding}, utf-8, latin-1.")

    # Permitir ; como separador comum
    if sep.lower() == "auto":
        sniff = csv.Sniffer().sniff(txt.splitlines()[0])
        sep = sniff.delimiter
    reader = csv.DictReader(io.StringIO(txt), delimiter=sep)

    inseridos = 0
    atualizados = 0
    erros: list[str] = []

    for i, row in enumerate(reader, start=2):
        try:
            numero = (get_row(row, "numero") or "").strip()
            if not numero:
                erros.append(f"Linha {i}: número de contrato vazio.")
                continue

            cab = cabecalho_default
            if not cab:
                rid = get_row(row, "cabecalho_id")
                if rid:
                    try:
                        cab = session.get(ContratoCabecalho, int(str(rid).strip()))
                    except Exception:
                        cab = None

            cab_inicio = cab_fim = None
            if cab:
                _, cab_inicio = _first_attr(cab, ("periodo_inicio","data_inicio","competencia_inicio","inicio"))
                _, cab_fim    = _first_attr(cab, ("periodo_fim","data_fim","competencia_fim","fim"))

            inicio = cab_inicio or _parse_date(get_row(row, "inicio"))
            fim    = cab_fim    or _parse_date(get_row(row, "fim"))

            if not (inicio and fim):
                erros.append(f"Linha {i} (contrato {numero}): início/fim do período não encontrados (cabeçalho ou CSV).")
                continue

            meses_periodo = _months_between(inicio.replace(day=1), fim.replace(day=1))
            meses_restantes = _months_from_today_until(fim)

            v_parcela = _parse_decimal(get_row(row, "valor_parcela")) or Decimal("0")
            valor_global = (v_parcela * Decimal(meses_periodo)).quantize(Decimal("0.01"))
            valor_presente = _npv_fixed_payments(v_parcela, meses_restantes, taxa_mensal)

            # Upsert por "numero"
            contrato = session.query(Contrato).filter(Contrato.numero == numero).first()
            novo = contrato is None
            if novo:
                contrato = Contrato()

            _set_if_has(contrato, "numero", numero)
            _set_if_has(contrato, "periodo_inicio", inicio)
            _set_if_has(contrato, "periodo_fim", fim)
            _set_if_has(contrato, "valor_parcela", v_parcela)
            _set_if_has(contrato, "valor_global", valor_global)
            _set_if_has(contrato, "meses_restantes", meses_restantes)
            _set_if_has(contrato, "valor_presente", valor_presente)

            if cab and hasattr(contrato, "cabecalho_id"):
                _set_if_has(contrato, "cabecalho_id", cab.id)

            if not dry_run:
                if novo:
                    session.add(contrato)
                    inseridos += 1
                else:
                    atualizados += 1

        except Exception as ex:
            erros.append(f"Linha {i}: erro inesperado ({ex}).")

    if not dry_run:
        session.commit()

    return {"inseridos": inseridos, "atualizados": atualizados, "erros": erros, "dry_run": dry_run}

# ----------------------------
# CLI
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Importar contratos via CSV (uma vez, fora do app).")
    parser.add_argument("--csv", required=True, help="Caminho do arquivo CSV.")
    parser.add_argument("--taxa", default="0.01", help="Taxa mensal (ex.: 0.01 = 1%%/mês).")
    parser.add_argument("--cabecalho-id", type=int, default=None, help="ID do ContratoCabecalho padrão (opcional).")
    parser.add_argument("--encoding", default="utf-8", help="Encoding do CSV (default: utf-8).")
    parser.add_argument("--sep", default="auto", help="Separador: ',', ';' ou 'auto'.")
    parser.add_argument("--dry-run", action="store_true", help="Não grava no banco, apenas simula.")
    parser.add_argument("--db-url", default=None, help="URL do banco (apenas se a Session do projeto não for encontrada).")

    args = parser.parse_args()

    taxa = _parse_decimal(args.taxa)
    if taxa is None:
        print("Taxa inválida. Exemplo válido: 0.01 (1%/mês).", file=sys.stderr)
        sys.exit(2)

    session = _get_session(args.db_url)
    try:
        result = importar_csv(
            session=session,
            path_csv=args.csv,
            taxa_mensal=taxa,
            cabecalho_id=args.cabecalho_id,
            encoding=args.encoding,
            sep=args.sep,
            dry_run=args.dry_run
        )
        print("==== RESULTADO ====")
        print(f"Inseridos:  {result['inseridos']}")
        print(f"Atualizados:{result['atualizados']}")
        print(f"Dry-run:    {result['dry_run']}")
        if result["erros"]:
            print("\nErros:")
            for e in result["erros"]:
                print(" -", e)
    finally:
        try:
            session.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
