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

# Função para criar a tabela DESPESA_DETALHADA
def criar_tabela_despesa_detalhada(engine):
    try:
        with engine.connect() as conn:
            create_table_query = """
            IF OBJECT_ID('DESPESA_DETALHADA', 'U') IS NULL
            BEGIN
                CREATE TABLE DESPESA_DETALHADA (
                    DTBASE DATE,
                    CODIGOCENTROCUSTO INT PRIMARY KEY,
                    CENTROCUSTOMASTER INT,
                    VLDESPESA DECIMAL(18, 2),
                    CODFILIALPRINCIPAL INT,
                    CODCONTA INT,
                    MES_A DATE,
                
                );
            END

            """
            conn.execute(text(create_table_query))
            logger.info("Tabela DESPESA_DETALHADA criada com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao criar a tabela DESPESA_DETALHADA: {e}")
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

# Função para enviar dados para o SQL Server
def enviar_para_sql_server(engine, df, nome_tabela):
    try:
        df.to_sql(nome_tabela, con=engine, if_exists='append', index=False)
        logger.info(f"Dados inseridos na tabela {nome_tabela} com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao inserir dados na tabela {nome_tabela}: {e}")
        raise

# Função para visualizar os dados na tabela
def visualizar_dados(engine, nome_tabela):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {nome_tabela}"))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            logger.info(f"Dados da tabela {nome_tabela} visualizados com sucesso.")
            print(df)
    except Exception as e:
        logger.error(f"Erro ao visualizar dados na tabela {nome_tabela}: {e}")
        raise

# Definir parâmetros de conexão
server = 'DESKTOP-A0LJ7MK\\SQLEXPRESS'  # Usar barra dupla para evitar problemas com strings
database = 'CASE_NEOBPO'
caminho_csv = r'C:\Users\marce\OneDrive\Área de Trabalho\Marcell_Felipe_Mis_senior_neobpo\Marcell_Felipe_Mis_senior_neobpo\ETL_BANCO\DESPESAS_.csv'

# Conectar ao SQL Server usando SQLAlchemy
engine = conectar_sqlalchemy(server, database)

# Criar a tabela DESPESA_DETALHADA
criar_tabela_despesa_detalhada(engine)

# Ler arquivo CSV
df_despesas = ler_csv(caminho_csv)

# Enviar dados para o SQL Server
enviar_para_sql_server(engine, df_despesas, 'DESPESA_DETALHADA')

# Visualizar os dados na tabela DESPESA_DETALHADA
visualizar_dados(engine, 'DESPESA_DETALHADA')
