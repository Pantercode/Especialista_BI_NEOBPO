import os
import pandas as pd
import pyodbc
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Função para ler os arquivos CSV
def ler_csv():
    """
    Lê o arquivo CSV contendo as contas, remove pontos da coluna 'CODCONTA' e a converte para inteiro.

    Retorna:
        df_contas (DataFrame): DataFrame contendo os dados do arquivo CSV.
    """
    df_contas = pd.read_csv(r'C:\Users\marce\OneDrive\Área de Trabalho\Marcell_Felipe_Mis_senior_neobpo\Marcell_Felipe_Mis_senior_neobpo\ETL_BANCO\CONTAS_.csv')
    logger.info("Arquivo CSV de contas lido com sucesso.")
    
    # Remover pontos da coluna CODCONTA e convertê-la para inteiro
    df_contas['CODCONTA'] = df_contas['CODCONTA'].astype(str).str.replace('.', '').astype(int)
    
    return df_contas

# Função para conectar ao banco de dados SQL Server
def conectar_sql_server(driver, servidor, database=None):
    """
    Conecta ao banco de dados SQL Server.

    Parâmetros:
        driver (str): Driver ODBC a ser utilizado.
        servidor (str): Nome do servidor SQL.
        database (str, opcional): Nome do banco de dados. Default é None.

    Retorna:
        conexao (pyodbc.Connection): Objeto de conexão ao SQL Server.
    """
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

# Função para criar o banco de dados
def criar_banco_dados(conexao, nome_banco):
    """
    Cria o banco de dados se ele não existir.

    Parâmetros:
        conexao (pyodbc.Connection): Objeto de conexão ao SQL Server.
        nome_banco (str): Nome do banco de dados a ser criado.
    """
    cursor = conexao.cursor()
    try:
        cursor.execute(f"IF DB_ID(N'{nome_banco}') IS NULL CREATE DATABASE {nome_banco}")
        conexao.commit()
        logger.info(f"Banco de dados {nome_banco} criado com sucesso (se não existia).")
    except pyodbc.Error as e:
        logger.error(f"Erro ao criar o banco de dados {nome_banco}: {e}")
        raise
    cursor.close()

# Função para criar a tabela CONTAS
def criar_tabela_contas(conexao):
    """
    Cria a tabela CONTAS no banco de dados conectado.

    Parâmetros:
        conexao (pyodbc.Connection): Objeto de conexão ao SQL Server.
    """
    cursor = conexao.cursor()
    criar_tabela_query = """
    CREATE TABLE CONTAS (
        CODCONTA INT PRIMARY KEY,
        CONTA VARCHAR(255),
        GRUPO VARCHAR(255)
    );
    """
    try:
        cursor.execute(criar_tabela_query)
        conexao.commit()
        logger.info("Tabela CONTAS criada com sucesso.")
    except pyodbc.Error as e:
        logger.error(f"Erro ao criar a tabela CONTAS: {e}")
        raise
    cursor.close()

# Função para enviar dados para o SQL Server
def enviar_para_sql_server(conexao, df, nome_tabela):
    """
    Envia os dados do DataFrame para o banco de dados SQL Server.

    Parâmetros:
        conexao (pyodbc.Connection): Objeto de conexão ao SQL Server.
        df (DataFrame): DataFrame contendo os dados a serem inseridos.
        nome_tabela (str): Nome da tabela onde os dados serão inseridos.
    """
    cursor = conexao.cursor()
    
    # Deletar dados existentes na tabela
    try:
        cursor.execute(f"DELETE FROM {nome_tabela}")
        conexao.commit()
        logger.info(f"Dados existentes na tabela {nome_tabela} foram deletados com sucesso.")
    except pyodbc.Error as e:
        logger.error(f"Erro ao deletar dados existentes na tabela {nome_tabela}: {e}")
        raise
    
    # Inserir novos dados
    for index, row in df.iterrows():
        colunas = ', '.join(df.columns)
        valores = ', '.join([f"'{str(val).replace('\'', '\'\'')}'" if not pd.isnull(val) else "NULL" for val in row])
        inserir_query = f"INSERT INTO {nome_tabela} ({colunas}) VALUES ({valores})"
        
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
nome_banco = 'CASE_NEOBPO'

# Conectar ao SQL Server sem especificar o banco de dados
conexao = conectar_sql_server(driver, servidor)

# Criar o banco de dados se não existir
criar_banco_dados(conexao, nome_banco)

# Fechar a conexão inicial
conexao.close()

# Conectar ao SQL Server usando o banco de dados criado
conexao = conectar_sql_server(driver, servidor, nome_banco)

# Ler e corrigir arquivo CSV de contas
df_contas = ler_csv()

# Criar a tabela CONTAS
criar_tabela_contas(conexao)

# Enviar dados para o SQL Server
enviar_para_sql_server(conexao, df_contas, 'CONTAS')

# Fechar a conexão
conexao.close()
