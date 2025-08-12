import sqlite3
from datetime import datetime
from dateutil.relativedelta import relativedelta

def calcular_meses_restantes(data_envio_str, periodo_contratual):
    try:
        data_envio = datetime.strptime(data_envio_str, "%Y-%m-%d")
        hoje = datetime.today()
        diff = relativedelta(hoje, data_envio)
        meses_passados = diff.years * 12 + diff.months
        return max(periodo_contratual - meses_passados, 0)
    except Exception as e:
        print(f"Erro ao processar data {data_envio_str}: {e}")
        return 0

# Caminho do banco de dados
db_path = "contratos.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Pega todos os contratos com data_envio e periodo_contratual
cursor.execute("SELECT id, data_envio, periodo_contratual FROM contratos")
contratos = cursor.fetchall()

for contrato in contratos:
    contrato_id, data_envio, periodo_contratual = contrato
    if data_envio and periodo_contratual:
        meses_restantes = calcular_meses_restantes(data_envio, periodo_contratual)
        cursor.execute(
            "UPDATE contratos SET meses_restantes = ? WHERE id = ?",
            (meses_restantes, contrato_id)
        )

conn.commit()
conn.close()

print("Atualização de meses_restantes concluída com sucesso.")
