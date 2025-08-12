import sqlite3

# Caminho do banco de dados
db_path = "contratos.db"

# Conectar ao banco
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Buscar contratos com os dados necessários
cursor.execute("SELECT id, valor_mensal, meses_restantes FROM contratos")
contratos = cursor.fetchall()

# Atualizar o valor_presente_contrato
for contrato in contratos:
    contrato_id, valor_mensal, meses_restantes = contrato
    try:
        if valor_mensal is not None and meses_restantes is not None:
            valor_presente = float(valor_mensal) * int(meses_restantes)
            cursor.execute(
                "UPDATE contratos SET valor_presente_contrato = ? WHERE id = ?",
                (valor_presente, contrato_id)
            )
    except Exception as e:
        print(f"Erro ao processar contrato ID {contrato_id}: {e}")

# Salvar e fechar
conn.commit()
conn.close()

print("Atualização de valor_presente_contrato concluída com sucesso.")
