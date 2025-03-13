import os
import pandas as pd
from sqlalchemy import create_engine
from prefect import task, flow

# Configuração do caminho do arquivo e banco de dados
caminho_arquivo = r'C:\Users\matheus.weinert\Downloads\Faturamento_teste.xlsx'
DATABASE_URL = "postgresql://postgres:***@ip:porta/DW"

# Criar conexão com o PostgreSQL
engine = create_engine(DATABASE_URL)

# Arquivo para armazenar a última modificação detectada
arquivo_timestamp = "ultima_modificacao_faturamento.txt"

@task
def verificar_atualizacao():
    """Verifica se a planilha foi modificada desde a última execução"""
    ultima_modificacao = os.path.getmtime(caminho_arquivo)

    if os.path.exists(arquivo_timestamp):
        with open(arquivo_timestamp, "r") as f:
            ultima_modificacao_salva = float(f.read().strip())

        if ultima_modificacao > ultima_modificacao_salva:
            with open(arquivo_timestamp, "w") as f:
                f.write(str(ultima_modificacao))
            return True
        else:
            return False
    else:
        with open(arquivo_timestamp, "w") as f:
            f.write(str(ultima_modificacao))
        return True

@task
def processar_excel():
    """Lê e processa a planilha"""
    df_dados = pd.read_excel(caminho_arquivo, engine='openpyxl', sheet_name="Acompanhamento Anual")

    # Ajusta as colunas para nomes legíveis
    df_dados.columns = df_dados.columns.str.split(' ').str[0]

    # Captura a coluna "MRC" e os dados de 2025
    df_teste = df_dados['MRC']
    df_2025 = df_dados.filter(like='2025')

    # Remove a coluna '2025' se existir
    df_headers_final = df_2025.drop(columns='2025', errors='ignore')

    # Junta os dados de MVNO com os valores de 2025
    df_final = pd.concat([df_teste, df_headers_final], axis=1)

    # Renomeia a coluna 'MRC' para 'mvnos'
    df_final = df_final.rename(columns={'MRC': 'mvnos'})

    # Remove a linha chamada 'TOTAL'
    df_final = df_final[df_final['mvnos'] != 'TOTAL']

    # Converte colunas de datas para linhas
    df_final_long = df_final.melt(id_vars=['mvnos'], var_name='data', value_name='valor')

    # Converte a coluna 'data' para DATE
    df_final_long['data'] = pd.to_datetime(df_final_long['data'], format='%d/%m/%Y')

    # Converte 'valor' para numérico
    df_final_long['valor'] = pd.to_numeric(df_final_long['valor'], errors='coerce')

    # Adiciona a coluna 'tipo_mvno' (caso precise de outra lógica, ajuste aqui)
    df_final_long['tipo_mvno'] = 'Desconhecido'

    # Remove valores NaN
    df_final_long = df_final_long.dropna(subset=['valor'])

    return df_final_long

@task
def inserir_no_postgres(df):
    """Insere os dados no PostgreSQL"""
    df.to_sql('faturamento_teste', engine, if_exists='append', index=False)
    print("Dados inseridos com sucesso no PostgreSQL!")

@flow
def fluxo_monitoramento():
    """Fluxo principal do Prefect"""
    if verificar_atualizacao():
        df_processado = processar_excel()
        inserir_no_postgres(df_processado)

if __name__ == "__main__":
    fluxo_monitoramento()
