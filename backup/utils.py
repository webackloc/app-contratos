# utils.py

import pandas as pd
from datetime import datetime
from typing import List, Dict

# Campos mínimos esperados no CSV
CAMPOS_OBRIGATORIOS = [
    'ativo', 'serial', 'cod_pro', 'descricao_produto',
    'cod_cli', 'nome_cli', 'data_envio',
    'contrato_n', 'valor_mensal', 'periodo_contratual'
]

def validar_colunas(df: pd.DataFrame):
    """Verifica se todas as colunas obrigatórias estão presentes."""
    colunas_csv = [col.strip().lower().replace(" ", "_") for col in df.columns]
    colunas_ausentes = [campo for campo in CAMPOS_OBRIGATORIOS if campo.lower() not in colunas_csv]

    if colunas_ausentes:
        raise ValueError(f"Colunas obrigatórias ausentes: {', '.join(colunas_ausentes)}")


def calcular_campos(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula campos derivados como meses_restantes, valor_global e valor_presente."""
    df['data_envio'] = pd.to_datetime(df['data_envio'], errors='coerce')

    df['valor_mensal'] = (
        df['valor_mensal']
        .astype(str)
        .str.replace(',', '.')
        .str.replace('.', '', 1)
        .astype(float)
    )

    df['periodo_contratual'] = (
        df['periodo_contratual']
        .astype(str)
        .str.extract(r'(\d+)')
        .astype(float)
    )

    df['meses_restantes'] = df.apply(
        lambda row: max(
            int(row['periodo_contratual'] - ((datetime.today() - row['data_envio']).days // 30)),
            0
        ),
        axis=1
    )

    df['valor_global_contrato'] = df['valor_mensal'] * df['periodo_contratual']
    df['valor_presente_contrato'] = df['valor_mensal'] * df['meses_restantes']

    return df


def process_csv(df: pd.DataFrame) -> List[Dict]:
    """Processa o DataFrame do CSV, validando e calculando campos."""
    try:
        # Padronizar os nomes das colunas
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # Validar presença de colunas obrigatórias
        validar_colunas(df)

        # Calcular campos adicionais
        df = calcular_campos(df)

        # Converter DataFrame em dicionários para inserção no banco
        contratos = df.to_dict(orient='records')

        print(f"{len(contratos)} contratos processados e prontos para inserção.")
        return contratos

    except Exception as e:
        raise Exception(f"Erro ao processar o arquivo: {str(e)}")
