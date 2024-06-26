import os
import logging
import time
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi
from requests.exceptions import ConnectionError, Timeout
import zipfile

# Configurar o logging para registrar eventos críticos, erros e informações de diagnóstico
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Definir variáveis de ambiente para autenticação no Kaggle
os.environ['KAGGLE_USERNAME'] = 'marcellfelipeolivesf'
os.environ['KAGGLE_KEY'] = '34cd37fb731e4a81cf5fdb50f7e488e0'

# Função para definir o caminho da pasta onde os arquivos serão salvos
def definir_caminho_download(caminho):
    # Se o caminho não existir, criar o diretório
    if not os.path.exists(caminho):
        os.makedirs(caminho)
    # Mudar o diretório de trabalho para o caminho especificado
    os.chdir(caminho)

# Função para realizar download com tentativas de reconexão
def baixar_arquivo_com_tentativas(api, dataset, nome_arquivo, tentativas=5, fator_espera=1.0):
    # Tentar baixar o arquivo várias vezes
    for tentativa in range(tentativas):
        try:
            logger.info(f"Tentativa {tentativa + 1} de download do arquivo {nome_arquivo}...")
            # Realizar o download do arquivo usando a API do Kaggle
            api.dataset_download_file(dataset, nome_arquivo)
            logger.info(f"Download do arquivo {nome_arquivo} concluído com sucesso!")
            return
        except (ConnectionError, Timeout) as e:
            # Registrar erros de conexão ou timeout e esperar antes de tentar novamente
            logger.error(f"Erro ao baixar o arquivo {nome_arquivo}: {e}")
            time.sleep(fator_espera * (2 ** tentativa))
        except Exception as e:
            # Registrar qualquer erro inesperado e parar as tentativas
            logger.error(f"Erro inesperado ao baixar o arquivo {nome_arquivo}: {e}")
            break
    logger.error(f"Falha ao baixar o arquivo {nome_arquivo} após {tentativas} tentativas.")

# Autenticar na API do Kaggle
api = KaggleApi()
api.authenticate()

# Definir caminho da pasta onde os arquivos serão salvos
caminho_download = r'C:\Users\MarcellFelipedePaula\OneDrive - Saúde Petrobras\Área de Trabalho\Marcell_Felipe_Mis_senior_neobpo\Questao 1 Automacao para Download de Dados'
definir_caminho_download(caminho_download)

# Dataset no Kaggle
dataset = 'thomassimeo/contas-senior-neobpo'

# Arquivos a serem baixados
arquivos_a_baixar = ['orcamento.xml', 'despesas.xlsx', 'centro_de_custo.pdf']

# Baixar arquivos com tratamento de erros avançado
for nome_arquivo in arquivos_a_baixar:
    baixar_arquivo_com_tentativas(api, dataset, nome_arquivo)

# Função para validar os arquivos baixados (opcional)
def validar_arquivos_baixados(nomes_arquivos):
    for nome_arquivo in nomes_arquivos:
        # Verificar se o arquivo foi baixado corretamente
        if not os.path.exists(nome_arquivo):
            logger.error(f"Arquivo {nome_arquivo} não foi baixado corretamente.")
        else:
            logger.info(f"Arquivo {nome_arquivo} está presente.")

validar_arquivos_baixados(arquivos_a_baixar)

# Função para extrair arquivos zipados, se necessário
def extrair_arquivos_zip(pasta_destino):
    for arquivo in os.listdir(pasta_destino):
        if arquivo.endswith('.zip'):
            caminho_arquivo_zip = os.path.join(pasta_destino, arquivo)
            # Extrair o conteúdo do arquivo zip
            with zipfile.ZipFile(caminho_arquivo_zip, 'r') as zip_ref:
                zip_ref.extractall(pasta_destino)
            # Remover o arquivo zip após a extração
            os.remove(caminho_arquivo_zip)
            logger.info(f"Arquivo {arquivo} extraído e deletado.")

extrair_arquivos_zip(caminho_download)
