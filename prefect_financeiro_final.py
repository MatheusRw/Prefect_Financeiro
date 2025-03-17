## SCRIPT FINAL DO MONITORAMENTO DA PLANILHA DO FINANCEIRO 
## SCRIPT FINAL DO MONITORAMENTO DA PLANILHA DO FINANCEIRO 
import os
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from prefect import task, flow

# Configuração do caminho do arquivo e banco de dados
caminho_arquivo = r'C:\Users\matheus.weinert\Downloads\Faturamento_teste.xlsx'
DATABASE_URL = "postgresql://*****:*****@******:******/DW"

# Criar conexão com o PostgreSQL
engine = create_engine(DATABASE_URL)

# Arquivo para armazenar a última modificação detectada
arquivo_timestamp = "ultima_modificacao_faturamento.txt"

# Dicionário para padronização dos nomes das MVNOs
padronizacao_nomes_mvno = {
    "GLOBALNET (FIXA)": "global",
    "ARROBA NET": "arrobanet",
    "TELECALL": "telecall",
    "SULNET": "sulnet",
    "FLUKE": "fluke",
    "LINKSFIELD": "linksfield",
    "TCHE TURBO": "tcheturbo",
    "BRPHONIA": "brphonia",
    "GL FIBRA": "glfibra",
    "1CEL": "1cel",
    "VCEL": "vcel",
    "ALLREDE": "allrede",
    "BRSULNET": "brsulnet",
    "LEFT FONE": "leftfone",
    "TODES TELECOM": "todes",
    "VIRTUEYES": "virtueyes",
    "P4 TELECOM": "p4",
    "SISTEL FIBRA": "sistel",
    "ULTRA": "ultra",
    "OQUEI": "oquei",
    "TRI TELECOM": "tri",
    "ABRUTELECOM": "abrutelecom",
    "gmx": "GMX",
    "CONESUL": "conesul",
    "FACILITA": "facilita",
    "TGN-BR": "tgn",
    "AMIGO MÓVEL": "amigo",
    "WSP TELECOM": "wsp",
    "FEDERAL": "federal",
    "DIGIREDES": "digiredes",
    "MAXCOMM": "maxxcom",
    "DALHE INTER": "internacional",
    "ABCREDE": "abcrede",
    "TIP": "tip",
    "HELLO": "hello",
    "VEEK": "veek"
}

# Dicionário de tipos MVNO atualizado
tipos_mvno = {
    "global": "pospago",
    "arrobanet": "pospago",
    "telecall": "pospago",
    "sulnet": "pospago",
    "fluke": "prepago",
    "linksfield": "m2m",
    "tcheturbo": "pospago",
    "brphonia": "pospago",
    "glfibra": "pospago",
    "1cel": "pospago",
    "vcel": "pospago",
    "allrede": "prepago",
    "brsulnet": "prepago",
    "leftfone": "prepago",
    "todes": "prepago",
    "virtueyes": "m2m",
    "p4": "pospago",
    "sistel": "pospago",
    "ultra": "m2m",
    "oquei": "pospago",
    "tri": "prepago",
    "abrutelecom": "pospago",
    "gmx": "pospago",
    "conesul": "pospago",
    "facilita": "pospago",
    "tgn": "prepago",
    "amigo": "prepago",
    "wsp": "pospago",
    "federal": "pospago",
    "digiredes": "pospago",
    "maxxcom": "pospago",
    "internacional": "prepago",
    "abcrede": "pospago",
    "tip": "pospago",
    "hello": "prepago",
    "veek": "prepago"
}


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
    
    # Padroniza os nomes das MVNOs
    df_final['mvnos'] = df_final['mvnos'].replace(padronizacao_nomes_mvno)
    
    # Converte colunas de datas para linhas
    df_final_long = df_final.melt(id_vars=['mvnos'], var_name='data', value_name='valor')
    
    # Converte a coluna 'data' para DATE
    df_final_long['data'] = pd.to_datetime(df_final_long['data'], format='%d/%m/%Y')
    
    # Converte 'valor' para numérico
    df_final_long['valor'] = pd.to_numeric(df_final_long['valor'], errors='coerce')
    
    # Adiciona a coluna 'tipo_mvno' com base no dicionário
    df_final_long['tipo_mvno'] = df_final_long['mvnos'].map(tipos_mvno)
    
    # Define como NULL os casos sem tipo
    df_final_long['tipo_mvno'] = df_final_long['tipo_mvno'].where(pd.notna(df_final_long['tipo_mvno']), None)
    
    # Remove valores NaN
    df_final_long = df_final_long.dropna(subset=['valor'])
    
    return df_final_long

@task
def inserir_no_postgres(df):
    """Insere ou atualiza os dados no PostgreSQL via upsert"""
    metadata = MetaData()
    faturamento_table = Table('faturamento_teste3', metadata, autoload_with=engine)
    
    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = insert(faturamento_table).values(
                mvnos=row['mvnos'],
                data=row['data'],
                valor=row['valor'],
                tipo_mvno=row['tipo_mvno']
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['mvnos', 'data'],
                set_={'valor': row['valor'], 'tipo_mvno': row['tipo_mvno']}
            )
            conn.execute(stmt)
    print("Dados inseridos/atualizados com sucesso no PostgreSQL!")

@flow
def fluxo_monitoramento():
    if verificar_atualizacao():
        df_processado = processar_excel()
        inserir_no_postgres(df_processado)

if __name__ == "__main__":
    fluxo_monitoramento()
