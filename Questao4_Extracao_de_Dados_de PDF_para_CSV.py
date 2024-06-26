import os
import logging
import pandas as pd
import pdfplumber
import re

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Caminho para o arquivo PDF
pdf_path = r'C:\Users\marce\OneDrive\Área de Trabalho\Marcell_Felipe_Mis_senior_neobpo\Marcell_Felipe_Mis_senior_neobpo\Questao 4 Extracao de Dados de PDF para CSV\CENTRO_DE_CUSTO.pdf'

# Função para extrair texto das páginas do PDF
def extrair_texto_pdf(pdf_path):
    if not os.path.exists(pdf_path):
        logger.error(f'O arquivo PDF não foi encontrado: {pdf_path}')
        return []

    logger.info('Extraindo texto do PDF...')
    textos = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, pagina in enumerate(pdf.pages):
            texto = pagina.extract_text()
            if texto:
                textos.append(texto)
            else:
                logger.warning(f'Nenhum texto encontrado na página {i + 1}.')
    return textos

# Função para processar o texto extraído e reconstruir as tabelas
def processar_texto(textos):
    logger.info('Processando texto extraído...')
    dados = []
    for texto in textos:
        linhas = texto.split('\n')
        for linha in linhas:
            # Use expressão regular para capturar exatamente 4 grupos de dados
            match = re.match(r'(\S+)\s+(.*\S)\s+(\S+)\s+(\d+)', linha.strip())
            if match:
                codigocentrocusto = re.sub(r'\D', '', match.group(1))  # Remover caracteres não numéricos
                dados.append([codigocentrocusto, match.group(2), match.group(3), match.group(4)])
            else:
                logger.warning(f'Linha ignorada devido ao formato inesperado: {linha}')
    return dados

# Função para salvar dados em formato CSV
def salvar_dados_csv(dados, output_csv_path):
    if not dados:
        logger.error('Nenhum dado válido foi encontrado para salvar.')
        return

    logger.info('Salvando dados no formato CSV...')
    df = pd.DataFrame(dados, columns=['CODIGOCENTROCUSTO', 'DESCRICAO', 'CENTROCUSTO', 'CENTROCUSTO_COD'])

    # Remover colunas vazias
    df = df.dropna(axis=1, how='all')

    df.to_csv(output_csv_path, index=False)
    logger.info(f'Dados salvos como {output_csv_path}')

# Função principal
def principal(pdf_path, output_csv_path):
    textos = extrair_texto_pdf(pdf_path)
    if not textos:
        return pd.DataFrame()  # Retornar DataFrame vazio se não houver textos válidos
    
    dados = processar_texto(textos)
    salvar_dados_csv(dados, output_csv_path)
    
    if dados:
        df = pd.DataFrame(dados, columns=['CODIGOCENTROCUSTO', 'DESCRICAO', 'CENTROCUSTO', 'CENTROCUSTO_COD'])
        print("Primeiras linhas dos dados:")
        print(df.head())
        return df
    else:
        return pd.DataFrame()  # Retornar DataFrame vazio se não houver dados válidos

if __name__ == '__main__':
    output_csv_path = r'C:\Users\marce\OneDrive\Área de Trabalho\Marcell_Felipe_Mis_senior_neobpo\Marcell_Felipe_Mis_senior_neobpo\Questao 4 Extracao de Dados de PDF para CSV\CENTRO_DE_CUSTO.csv'
    dados_concatenados = principal(pdf_path, output_csv_path)

    if not dados_concatenados.empty:
        # Mostrar colunas vazias
        colunas_vazias = [col for col in dados_concatenados.columns if dados_concatenados[col].isnull().all()]
        print(f'Colunas vazias: {colunas_vazias}')

        # Remover colunas vazias
        dados_sem_colunas_vazias = dados_concatenados.dropna(axis=1, how='all')

        # Salvar DataFrame sem colunas vazias no mesmo arquivo CSV
        dados_sem_colunas_vazias.to_csv(output_csv_path, index=False)
        logger.info(f'Dados sem colunas vazias salvos como {output_csv_path}')
    else:
        logger.error('Nenhum dado válido foi encontrado para processar.')
