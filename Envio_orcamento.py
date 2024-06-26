import pandas as pd
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Função para conectar ao banco de dados SQL Server usando SQLAlchemy
def conectar_sqlalchemy(server, database):
    try:
        connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url)
        logger.info(f"Conexão com o SQL Server estabelecida com sucesso para o banco de dados {database}.")
        return engine
    except Exception as e:
        logger.error(f"Erro ao conectar ao SQL Server: {e}")
        raise

# Função para criar a tabela ORCAMENTO
def criar_tabela_orcamento(engine):
    try:
        with engine.connect() as conn:
            create_table_query = """
            CREATE TABLE ORCAMENTO (
                ORCAMENTO_ULTDATA DATE,
                ORCAMENTO_FILIALCOD INT,
                ORCAMENTO_CONTACOD INT,
                ORCAMENTO_CENTROCUSTOCOD INT,
                ORCAMENTO_ORCADO DECIMAL(18, 2),
                ORCAMENTO_PERRATEIO DECIMAL(18, 2),
                PRIMARY KEY (ORCAMENTO_ULTDATA, ORCAMENTO_CONTACOD, ORCAMENTO_CENTROCUSTOCOD),
                FOREIGN KEY (ORCAMENTO_CONTACOD) REFERENCES CONTAS(CODCONTA),
                FOREIGN KEY (ORCAMENTO_CENTROCUSTOCOD) REFERENCES CENTRO_DE_CUSTO(CODIGOCENTROCUSTO)
            );
            """
            conn.execute(text(create_table_query))
            logger.info("Tabela ORCAMENTO criada com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao criar a tabela ORCAMENTO: {e}")
        raise

# Função para ler o arquivo CSV
def ler_csv(caminho_arquivo):
    try:
        df = pd.read_csv(caminho_arquivo)
        logger.info("Arquivo CSV lido com sucesso.")
        return df
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo CSV: {e}")
        raise

# Função para verificar e corrigir os valores de ORCAMENTO_CONTACOD
def verificar_e_corrigir_contas(engine, df, tabela_contas):
    try:
        query = text(f"SELECT CODCONTA FROM {tabela_contas}")
        valid_codcontas = pd.read_sql(query, engine)['CODCONTA'].tolist()
        
        # Adicionar registros faltantes na tabela CONTAS
        invalid_codcontas = df[~df['ORCAMENTO_CONTACOD'].isin(valid_codcontas)]['ORCAMENTO_CONTACOD'].unique()
        if len(invalid_codcontas) > 0:
            for codconta in invalid_codcontas:
                insert_query = text(f"INSERT INTO {tabela_contas} (CODCONTA) VALUES (:codconta)")
                with engine.connect() as conn:
                    conn.execute(insert_query, {'codconta': codconta})
            logger.info(f"{len(invalid_codcontas)} registros inválidos adicionados à tabela {tabela_contas}.")
        
        # Re-verificar e retornar dataframe corrigido
        valid_codcontas = pd.read_sql(query, engine)['CODCONTA'].tolist()
        df_valid = df[df['ORCAMENTO_CONTACOD'].isin(valid_codcontas)]
        return df_valid
    except Exception as e:
        logger.error(f"Erro ao verificar e corrigir contas: {e}")
        raise

# Função para verificar e corrigir os valores de ORCAMENTO_CENTROCUSTOCOD
def verificar_e_corrigir_centrocusto(engine, df, tabela_centro_custo):
    try:
        query = text(f"SELECT CODIGOCENTROCUSTO FROM {tabela_centro_custo}")
        valid_centro_custos = pd.read_sql(query, engine)['CODIGOCENTROCUSTO'].tolist()
        
        # Adicionar registros faltantes na tabela CENTRO_DE_CUSTO
        invalid_centro_custos = df[~df['ORCAMENTO_CENTROCUSTOCOD'].isin(valid_centro_custos)]['ORCAMENTO_CENTROCUSTOCOD'].unique()
        if len(invalid_centro_custos) > 0:
            for codigocentro in invalid_centro_custos:
                insert_query = text(f"INSERT INTO {tabela_centro_custo} (CODIGOCENTROCUSTO) VALUES (:codigocentro)")
                with engine.connect() as conn:
                    conn.execute(insert_query, {'codigocentro': codigocentro})
            logger.info(f"{len(invalid_centro_custos)} registros inválidos adicionados à tabela {tabela_centro_custo}.")
        
        # Re-verificar e retornar dataframe corrigido
        valid_centro_custos = pd.read_sql(query, engine)['CODIGOCENTROCUSTO'].tolist()
        df_valid = df[df['ORCAMENTO_CENTROCUSTOCOD'].isin(valid_centro_custos)]
        return df_valid
    except Exception as e:
        logger.error(f"Erro ao verificar e corrigir centro de custo: {e}")
        raise

# Função para enviar dados para o SQL Server
def enviar_para_sql_server(engine, df, nome_tabela):
    try:
        df.to_sql(nome_tabela, con=engine, if_exists='append', index=False)
        logger.info(f"Dados inseridos na tabela {nome_tabela} com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao inserir dados na tabela {nome_tabela}: {e}")
        raise

# Definir parâmetros de conexão
server = 'DESKTOP-A0LJ7MK\\SQLEXPRESS'  # Usar barra dupla para evitar problemas com strings
database = 'CASE_NEOBPO'
tabela_contas = 'CONTAS'
tabela_centro_custo = 'CENTRO_DE_CUSTO'
caminho_csv = r'C:\Users\marce\OneDrive\Área de Trabalho\Marcell_Felipe_Mis_senior_neobpo\Marcell_Felipe_Mis_senior_neobpo\ETL_BANCO\ORCAMENTO_.csv'

# Conectar ao SQL Server usando SQLAlchemy
engine = conectar_sqlalchemy(server, database)

# Criar a tabela ORCAMENTO
criar_tabela_orcamento(engine)

# Ler arquivo CSV
df_orcamento = ler_csv(caminho_csv)

# Verificar e corrigir os valores de ORCAMENTO_CONTACOD
df_orcamento_corrigido = verificar_e_corrigir_contas(engine, df_orcamento, tabela_contas)

# Verificar e corrigir os valores de ORCAMENTO_CENTROCUSTOCOD
df_orcamento_corrigido = verificar_e_corrigir_centrocusto(engine, df_orcamento_corrigido, tabela_centro_custo)

# Enviar dados para o SQL Server
enviar_para_sql_server(engine, df_orcamento_corrigido, 'ORCAMENTO')
