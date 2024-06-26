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

# Função para criar a tabela DespesasDetalhadas
def criar_tabela_despesas(engine):
    try:
        with engine.connect() as conn:
            create_table_query = """
            IF OBJECT_ID('DespesasDetalhadas', 'U') IS NULL
            BEGIN
                CREATE TABLE DespesasDetalhadas (
                    DTBASE DATE,
                    CODIGOCENTROCUSTO INT,
                    CENTROCUSTOMASTER INT,
                    VLDESPESA DECIMAL(18, 2),
                    CODFILIALPRINCIPAL INT,
                    CODCONTA INT,
                    MES_A DATE,
                    PRIMARY KEY (DTBASE, CODIGOCENTROCUSTO, CODCONTA)
                );
            END
            """
            conn.execute(text(create_table_query))
            logger.info("Tabela DespesasDetalhadas criada com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao criar a tabela DespesasDetalhadas: {e}")
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

# Função para criar a stored procedure para inserir ou atualizar dados
def criar_procedimento(engine):
    try:
        with engine.connect() as conn:
            procedure_query = """
            CREATE PROCEDURE sp_InserirAtualizarDespesasDetalhadas
            AS
            BEGIN
                SET NOCOUNT ON;

                BEGIN TRY
                    BEGIN TRANSACTION;

                    -- Inserir ou atualizar registros na tabela DespesasDetalhadas
                    MERGE DespesasDetalhadas AS target
                    USING (
                        SELECT 
                            CONVERT(DATE, DTBASE, 111) AS DTBASE,
                            CONVERT(INT, CODIGOCENTROCUSTO) AS CODIGOCENTROCUSTO,
                            CONVERT(INT, CENTROCUSTOMASTER) AS CENTROCUSTOMASTER,
                            CONVERT(DECIMAL(18,2), VLDESPESA) AS VLDESPESA,
                            CONVERT(INT, CODFILIALPRINCIPAL) AS CODFILIALPRINCIPAL,
                            CONVERT(INT, CODCONTA) AS CODCONTA,
                            CONVERT(DATE, MES_A, 111) AS MES_A
                        FROM DespesasDetalhadas
                    ) AS source
                    ON (target.DTBASE = source.DTBASE AND target.CODIGOCENTROCUSTO = source.CODIGOCENTROCUSTO AND target.CODCONTA = source.CODCONTA)
                    WHEN MATCHED THEN
                        UPDATE SET 
                            CENTROCUSTOMASTER = source.CENTROCUSTOMASTER,
                            VLDESPESA = source.VLDESPESA,
                            CODFILIALPRINCIPAL = source.CODFILIALPRINCIPAL,
                            MES_A = source.MES_A
                    WHEN NOT MATCHED THEN
                        INSERT (DTBASE, CODIGOCENTROCUSTO, CENTROCUSTOMASTER, VLDESPESA, CODFILIALPRINCIPAL, CODCONTA, MES_A)
                        VALUES (source.DTBASE, source.CODIGOCENTROCUSTO, source.CENTROCUSTOMASTER, source.VLDESPESA, source.CODFILIALPRINCIPAL, source.CODCONTA, source.MES_A);

                    COMMIT TRANSACTION;
                END TRY
                BEGIN CATCH
                    -- Tratamento de erros
                    IF @@TRANCOUNT > 0
                    BEGIN
                        ROLLBACK TRANSACTION;
                    END

                    -- Registra o erro
                    DECLARE @ErrorMessage NVARCHAR(4000);
                    DECLARE @ErrorSeverity INT;
                    DECLARE @ErrorState INT;

                    SELECT @ErrorMessage = ERROR_MESSAGE(),
                           @ErrorSeverity = ERROR_SEVERITY(),
                           @ErrorState = ERROR_STATE();

                    RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
                END CATCH
            END;
            """
            conn.execute(text(procedure_query))
            logger.info("Stored procedure sp_InserirAtualizarDespesasDetalhadas criada com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao criar a stored procedure: {e}")
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

# Criar a tabela DespesasDetalhadas
criar_tabela_despesas(engine)

# Ler arquivo CSV
df_despesas = ler_csv(caminho_csv)

# Enviar dados para o SQL Server
enviar_para_sql_server(engine, df_despesas, 'DespesasDetalhadas')

# Criar a stored procedure para inserir ou atualizar dados
criar_procedimento(engine)

# Executar a stored procedure para inserir/atualizar dados na tabela DespesasDetalhadas
with engine.connect() as conn:
    conn.execute(text("EXEC sp_InserirAtualizarDespesasDetalhadas"))

# Visualizar os dados na tabela DespesasDetalhadas
visualizar_dados(engine, 'DespesasDetalhadas')
