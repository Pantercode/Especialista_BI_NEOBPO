import os
import pandas as pd
import pyodbc
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Função para ler o arquivo CSV
def ler_csv():
    df_centro_de_custo = pd.read_csv(r'C:\Users\marce\OneDrive\Área de Trabalho\Marcell_Felipe_Mis_senior_neobpo\Marcell_Felipe_Mis_senior_neobpo\Questao 4 Extracao de Dados de PDF para CSV\CENTRO_DE_CUSTO.csv')
    logger.info("Arquivo CSV de centro de custo lido com sucesso.")
    return df_centro_de_custo

# Função para conectar ao banco de dados SQL Server
def conectar_sql_server(driver, servidor, database=None):
    try:
        if database:
            conexao = pyodbc.connect(
                f'DRIVER={driver};'
                f'SERVER={servidor};'
                f'DATABASE={database};'
                'Trusted_Connection=yes;'
            )
        else:
            conexao = pyodbc.connect(
                f'DRIVER={driver};'
                f'SERVER={servidor};'
                'Trusted_Connection=yes;'
            )
        logger.info(f"Conexão com o SQL Server estabelecida com sucesso {'para o banco de dados ' + database if database else ''}.")
        return conexao
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        logger.error(f"Erro ao conectar ao SQL Server: {sqlstate}")
        raise

# Função para criar a tabela CENTRO_DE_CUSTO
def criar_tabela_centro_de_custo(conexao):
    cursor = conexao.cursor()
    criar_tabela_query = """
    CREATE TABLE CENTRO_DE_CUSTO (
        CODIGOCENTROCUSTO INT PRIMARY KEY,
        DESCRICAO VARCHAR(255),
        CENTROCUSTO VARCHAR(255),
        CENTROCUSTO_COD INT
    );
    """
    try:
        cursor.execute(criar_tabela_query)
        conexao.commit()
        logger.info("Tabela CENTRO_DE_CUSTO criada com sucesso.")
    except pyodbc.Error as e:
        logger.error(f"Erro ao criar a tabela CENTRO_DE_CUSTO: {e}")
        raise
    cursor.close()

# Função para formatar a coluna CODIGOCENTROCUSTO
def formatar_codigocentrocusto(valor):
    if '.' in str(valor):
        return str(valor).replace('.', '')
    return valor

# Função para enviar dados para o SQL Server
def enviar_para_sql_server(conexao, df, nome_tabela):
    cursor = conexao.cursor()

    # Deletar dados existentes na tabela
    delete_query = f"DELETE FROM {nome_tabela}"
    cursor.execute(delete_query)
    logger.info(f"Dados existentes na tabela {nome_tabela} deletados com sucesso.")

    # Remover duplicatas do DataFrame
    df = df.drop_duplicates(subset=['CODIGOCENTROCUSTO'])

    # Formatar a coluna CODIGOCENTROCUSTO
    df['CODIGOCENTROCUSTO'] = df['CODIGOCENTROCUSTO'].apply(formatar_codigocentrocusto)

    for index, row in df.iterrows():
        colunas = ', '.join(df.columns)
        valores = ', '.join([f"'{str(val).replace("'", "''")}'" if not pd.isnull(val) else 'NULL' for val in row])
        
        inserir_query = f"INSERT INTO {nome_tabela} ({colunas}) VALUES ({valores})"
        logger.debug(f"Executando query: {inserir_query}")
        try:
            cursor.execute(inserir_query)
        except pyodbc.Error as e:
            logger.error(f"Erro ao executar query: {inserir_query}")
            logger.error(f"Erro: {e}")
            raise
    conexao.commit()
    logger.info(f"Dados inseridos na tabela {nome_tabela} com sucesso.")
    cursor.close()

# Definir parâmetros de conexão
driver = '{ODBC Driver 17 for SQL Server}'  # Altere conforme a versão do seu driver ODBC
servidor = r'DESKTOP-A0LJ7MK\SQLEXPRESS'  # Use raw string para evitar problemas com a barra invertida

# Ler arquivo CSV
df_centro_de_custo = ler_csv()

# Conectar ao SQL Server usando o banco de dados existente
conexao = conectar_sql_server(driver, servidor, 'CASE_NEOBPO')

# Criar a tabela CENTRO_DE_CUSTO
criar_tabela_centro_de_custo(conexao)

# Enviar dados para o SQL Server
enviar_para_sql_server(conexao, df_centro_de_custo, 'CENTRO_DE_CUSTO')

# Fechar a conexão
conexao.close()
