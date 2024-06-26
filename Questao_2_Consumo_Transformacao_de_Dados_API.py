import os
import requests
import csv
from datetime import datetime
import pandas as pd
import time

API_TOKEN = os.getenv("API_TOKEN")
API_URL = 'https://sheetdb.io/api/v1/inj4ilqkt4j3o'  

headers = {
    'Authorization': f'Bearer {API_TOKEN}'
}

# Caminho completo para o arquivo CSV
csv_path = r'C:\Users\marce\OneDrive\Área de Trabalho\Marcell_Felipe_Mis_senior_neobpo\Marcell_Felipe_Mis_senior_neobpo\Questao 2 Consumo e Transformacao de Dados via API\resultados.csv'

# Função para obter a última data de atualização do arquivo CSV
def obter_ultima_data_atualizacao(csv_path):
    try:
        with open(csv_path, mode='r') as file:
            reader = csv.DictReader(file)
            if 'last_updated' not in reader.fieldnames:
                return None
            datas = [row['last_updated'] for row in reader if row['last_updated']]
            if datas:
                return max(datas)
            else:
                return None
    except FileNotFoundError:
        print(f"Arquivo não encontrado: {csv_path}")
        return None
    except PermissionError:
        print(f"Permissão negada ao acessar o arquivo: {csv_path}")
        return None

# Obtém a última data de atualização do arquivo CSV
ultima_data_atualizacao = obter_ultima_data_atualizacao(csv_path)

# Função para buscar dados da API
def buscar_dados(api_url, headers, ultima_data_atualizacao):
    params = {
        'limit': 100  # Defina o limite de registros por página
    }
    if ultima_data_atualizacao:
        params['updated_after'] = ultima_data_atualizacao
    
    todos_os_dados = []
    proxima_url = api_url
    while proxima_url:
        try:
            response = requests.get(proxima_url, headers=headers, params=params)
            response.raise_for_status()
            dados = response.json()
            if not dados:
                break
            
            todos_os_dados.extend(dados)

            # Verifique se há uma próxima página
            proxima_url = response.links.get('next', {}).get('url')

        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 429:
                print("Limite de taxa excedido. Aguardando 120 segundos antes de tentar novamente.")
                time.sleep(120)  # Aguardar 120 segundos antes de tentar novamente
            else:
                print(f"Erro HTTP na requisição: {http_err}")
                break
        except requests.exceptions.RequestException as err:
            print(f"Erro na requisição: {err}")
            break

    return todos_os_dados

# Buscar dados da API
dados = buscar_dados(API_URL, headers, ultima_data_atualizacao)

# Processar e salvar os dados, se necessário
if dados:
    try:
        df = pd.DataFrame(dados)
        df.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), index=False)
    except PermissionError:
        print(f"Permissão negada ao salvar o arquivo: {csv_path}")
    except Exception as e:
        print(f"Erro ao salvar os dados: {e}")

print("Processamento concluído.")
