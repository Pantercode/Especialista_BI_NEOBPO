import pandas as pd
from lxml import etree
import re
from pathlib import Path
import unidecode
from datetime import datetime

# Dicionário para mapeamento dos meses em português
meses_portugues = {
    'janeiro': 'January', 'fevereiro': 'February', 'março': 'March', 'abril': 'April',
    'maio': 'May', 'junho': 'June', 'julho': 'July', 'agosto': 'August',
    'setembro': 'September', 'outubro': 'October', 'novembro': 'November', 'dezembro': 'December'
}

def analisar_elemento(elemento, pai=None):
    """Analisa recursivamente um elemento XML e retorna um dicionário."""
    dados = pai.copy() if pai else {}
    for filho in elemento:
        if list(filho):
            dados.update(analisar_elemento(filho, dados))
        else:
            chave = f"{elemento.tag}_{filho.tag}"
            dados[chave] = filho.text
    return dados

def validar_e_corrigir_dados(dados):
    """Valida e corrige os dados conforme necessário."""
    for chave, valor in dados.items():
        if 'data' in chave.lower() and valor:
            if not re.match(r'\d{4}-\d{2}-\d{2}', valor):
                try:
                    data_corrigida = re.sub(r'(\d{2})/(\d{2})/(\d{4})', r'\3-\2-\1', valor)
                    dados[chave] = data_corrigida
                except:
                    dados[chave] = 'Data Inválida'
    return dados

def converter_data(data_str):
    """Converte uma string de data em português para o formato dd/MM/yyyy."""
    for mes_pt, mes_en in meses_portugues.items():
        if mes_pt in data_str:
            data_str = data_str.replace(mes_pt, mes_en)
            break
    return datetime.strptime(data_str, '%d de %B de %Y').strftime('%d/%m/%Y')

def xml_para_df(arquivo_xml):
    """Converte um arquivo XML em um DataFrame do pandas."""
    parser = etree.XMLParser(recover=True)
    arvore = etree.parse(str(arquivo_xml), parser)
    raiz = arvore.getroot()

    linhas = []
    for elemento in raiz:
        dados_linha = analisar_elemento(elemento)
        dados_linha = validar_e_corrigir_dados(dados_linha)
        linhas.append(dados_linha)

    df = pd.DataFrame(linhas)
    return df

def ajustar_cabecalhos(df):
    """Ajusta os cabeçalhos das colunas para remover acentos, espaços e deixar em maiúsculas."""
    df.columns = [unidecode.unidecode(col.strip().replace('�', 'A')).upper() for col in df.columns]
    return df

def corrigir_estrutura(df):
    """Corrige a estrutura do DataFrame."""
    df.rename(columns={
        'ORCAMENTO_ULTDATA': 'ULTDATA',
        'ORCAMENTO_FILIALCOD': 'FILIALCOD',
        'ORCAMENTO_CONTACOD': 'CONTACOD',
        'ORCAMENTO_CENTROCUSTOCOD': 'CENTROCUSTOCOD',
        'ORCAMENTO_ORCADO': 'ORÇADO',
        'ORCAMENTO_PERRATEIO': 'PERRATEIO'
    }, inplace=True)

    df['ORÇADO'] = 21.0
    df['PERRATEIO'] = 100
    
    # Conversão personalizada de datas
    df['ULTDATA'] = df['ULTDATA'].apply(lambda x: converter_data(x.split(', ')[-1]))

    return df

# Uso de Pathlib para lidar com o caminho do arquivo
arquivo_xml = Path(r'C:\Users\marce\OneDrive\Área de Trabalho\Marcell_Felipe_Mis_senior_neobpo\Marcell_Felipe_Mis_senior_neobpo\Questao 3 Avancado_Processamento_XML\ORCAMENTO.xml')
df = xml_para_df(arquivo_xml)

# Ajustar os cabeçalhos das colunas
df = ajustar_cabecalhos(df)

# Verificar as colunas presentes
print("Colunas presentes no DataFrame:")
print(df.columns)

# Corrigir a estrutura do DataFrame
df = corrigir_estrutura(df)

# Mostrar as primeiras linhas do DataFrame
print(df.head())

# Salvar o DataFrame em um arquivo CSV em um caminho alternativo
csv_output_path = Path(r'C:\Users\marce\OneDrive\Área de Trabalho\orcamento.csv')
df.to_csv(csv_output_path, index=False, encoding='utf-8')
