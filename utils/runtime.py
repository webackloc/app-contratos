from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RUNTIME_DIR = BASE_DIR / "runtime"
RUNTIME_DIR.mkdir(exist_ok=True)

def path_ultima_importacao(): return RUNTIME_DIR / "ultima_importacao.json"
def path_importacoes_jsonl(): return RUNTIME_DIR / "importacoes.jsonl"
