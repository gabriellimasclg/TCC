# -*- coding: utf-8 -*-
"""
Created on Tue Aug 26 10:01:40 2025

@author: glima
"""
import urllib3
import requests
import os
import pandas as pd
import numpy as np
from clean_text import clean_text

#%% Função para analisar CNPJs e CPFs

def CNPJAnalysis(df, cnpj_column='mv.num_cpf_cnpj'):
    """
    Analyzes CNPJ/CPF data in a DataFrame, classifying documents and counting digit lengths.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing the documents to analyze
        cnpj_column (str): Name of the column containing CPF/CNPJ numbers (default: 'mv.num_cpf_cnpj')
    
    Returns:
        dict: A dictionary with the count of documents by digit length
        pd.DataFrame: The original DataFrame with added 'tipo' column (CNPJ/CPF/outro)
    """
    
    # 1. Classify documents by length
    df['tipo'] = np.where(
        df[cnpj_column].str.len() == 14, 'CNPJ',
        np.where(
            df[cnpj_column].str.len() == 11, 'CPF',
            'outro'
        )
    )
    
    # 2. Initialize counter for digit lengths (4-14)
    contagem = {length: 0 for length in range(4, 15)}
    total_documents = len(df)
    
    # 3. Count documents by cleaned digit length
    for doc in df[cnpj_column]:
        # Remove all non-digit characters
        cleaned_doc = ''.join(filter(str.isdigit, str(doc)))
        length = len(cleaned_doc)
        
        if length in contagem:
            contagem[length] += 1
    
    # 4. Verification and reporting
    total_counted = sum(contagem.values())
    
    if total_counted == total_documents:
        print("✅ Todos os documentos foram contabilizados!")
    else:
        print(f"⚠️ Atenção: {total_documents - total_counted} documentos não se enquadram nos tamanhos 4-14 dígitos")
    
    # 5. Detailed report
    print("\nDistribuição de dígitos:")
    for length, quantity in sorted(contagem.items()):
        if quantity > 0:  # Only show lengths that actually appear
            print(f'{quantity:>5} documentos com {length:>2} dígitos ({quantity/total_documents:.1%})')
    
    print(f"\nTotal contado: {total_counted} de {total_documents} documentos\nApenas serão considerados documentos com 14 dígitos (CNPJs)")
    
    return

#%% Funções de download da base de dados

# Desabilita avisos de requisições HTTPS sem verificação de certificado
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_ibama_ctf_data(repo_path):
    '''
    Automatiza o processo de download, processamento e consolidação de dados de
    Pessoas Jurídicas do Cadastro Técnico Federal (CTF) do IBAMA.

    O fluxo de trabalho da função é o seguinte:
    1. Cria as estruturas de pastas necessárias ('inputs/dadosIbamaCNPJ_Download'
       para arquivos brutos e 'inputs/dadosIbamaCNPJ_Consolidado' para o final).
    2. Pergunta ao usuário se os arquivos já foram baixados para pular a etapa de download.
    3. Se o download for necessário, itera por todas as Unidades da Federação (UFs),
       baixa o arquivo CSV correspondente e o salva na pasta de dados brutos.
    4. Cada arquivo baixado é processado individualmente para padronizar CNPJs e
       nomes de colunas.
    5. Ao final, todos os dados processados são consolidados em um único DataFrame.
    6. Colunas de texto ('Municipio', 'Razao Social') passam por uma limpeza final.
    7. O DataFrame consolidado é salvo como um único arquivo CSV na pasta de
       dados processados.

    Parâmetros:
        repo_path (str): Caminho para a pasta raiz do projeto. As subpastas de
                         dados serão criadas dentro deste caminho.

    Retorna:
        pandas.DataFrame: Um DataFrame contendo todos os dados consolidados e limpos.
        None: Retorna None se o processo falhar ou se nenhum arquivo for processado.
    '''
    
    # Define os caminhos para as pastas de dados brutos e processados
    raw_dir = os.path.join(repo_path, 'inputs', 'MaterialBaixado','PJ_UF')
    processed_dir = os.path.join(repo_path, 'inputs', 'MaterialBaixado')
    
    # Garante que as pastas de destino existam; se não, elas são criadas
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    # Interage com o usuário para saber se a etapa de download pode ser pulada
    start = input('Você já tem os arquivos baixados? (s/n) ')
    
    # Se o usuário responder 's', o script carrega o arquivo já consolidado
    if start.lower() == 's':
        
        # Caminho para o arquivo final que deveria existir
        final_file_path = os.path.join(processed_dir, "PJ_BR.csv")
        print(f"Carregando arquivo consolidado de: {final_file_path}")
        
        # Lê o CSV consolidado, garantindo a tipagem correta de colunas-chave
        df_final = pd.read_csv(final_file_path,
                               dtype={'CNPJ': str,
                                      'Codigo da atividade': str,
                                      'Codigo da categoria': str},
                               keep_default_na=False)
               
        return df_final
    
    else:
        # Bloco de execução para o download e processamento dos dados
        print("Iniciando o download e processamento dos dados...")
        base_url = "http://dadosabertos.ibama.gov.br/dados/CTF/APP/"
        
        # Lista de todas as Unidades da Federação do Brasil
        ufs = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO',
            'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI',
            'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]
        
        # Lista vazia para armazenar os DataFrames de cada estado após o processamento
        pessoas_juridicas_br = []
        
        # Itera sobre cada UF para baixar e processar os dados
        for uf in ufs:
            try:
                # Constrói a URL completa para o arquivo CSV do estado atual
                url = f"{base_url}{uf}/pessoasJuridicas.csv"
                print(f"Baixando {uf}...")
                
                # Faz a requisição GET para baixar o arquivo
                # timeout=30: define um tempo limite de 30 segundos
                # verify=False: ignora erros de certificado SSL (usar com cautela)
                response = requests.get(url, verify=False, timeout=30) 
                
                # Verifica se a requisição foi bem-sucedida (status code 2xx)
                response.raise_for_status() 
                
                # Define o caminho completo para salvar o arquivo bruto baixado
                output_path = os.path.join(raw_dir, f"pessoasJuridicas_{uf}.csv")
                
                # Abre o arquivo em modo de escrita binária ('wb') e salva o conteúdo
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                
                # Inicia o processamento do arquivo recém-baixado
                try:
                    # Carrega o arquivo CSV em um DataFrame, especificando o delimitador e os tipos de dados
                    df = pd.read_csv(output_path,
                                     delimiter=';',
                                     dtype={'CNPJ': str,
                                            'Codigo da atividade': str,
                                            'Codigo da categoria': str},
                                     keep_default_na=False)
                    
                    # Padroniza a coluna de CNPJ para ter sempre 14 dígitos, com zeros à esquerda
                    df['CNPJ'] = df['CNPJ'].str.zfill(14) 
                    
                    # Limpa e padroniza os nomes das colunas usando a função externa clean_text
                    df.columns = df.columns.map(clean_text)
                    
                    # Adiciona o DataFrame do estado, já limpo, à lista de consolidação
                    pessoas_juridicas_br.append(df)
                    
                # Captura e informa erros que possam ocorrer durante o processamento do arquivo
                except Exception as e:
                    print(f"Erro ao processar {uf}: {e}")
                    
            # Captura e informa erros relacionados ao download (ex: rede, URL inválida)
            except requests.exceptions.RequestException as e:
                print(f"Erro ao baixar {uf}: {e}")
        
        # Após o loop, verifica se algum dado foi processado antes de consolidar
        if pessoas_juridicas_br:
            print("\nConsolidando todos os dados...")
            # Junta todos os DataFrames da lista em um único DataFrame
            df_final = pd.concat(pessoas_juridicas_br, ignore_index=True)
            
            # Aplica a função de limpeza nas células das colunas 'Municipio' e 'Razao Social'
            df_final['MUNICIPIO'] = df_final['MUNICIPIO'].apply(clean_text)
            df_final['RAZAO SOCIAL'] = df_final['RAZAO SOCIAL'].apply(clean_text)
            df_final['MUNICIPIO'] = df_final['MUNICIPIO'].str.replace(
                r"TRAJANO DE MORAIS", "TRAJANO DE MORAES", regex=True
                )
            
            # Extrair os anos de inicio e fim das datas
            # ANO_INICIO
            df_final['ANO_INICIO'] = (
                df_final['DATA DE INICIO DA ATIVIDADE']
                .fillna('0000')                    # trata valores nulos
                .replace('', '0000')               # trata strings vazias
                .str[-4:]                          # pega os 4 últimos caracteres
                .astype(int)                       # converte para inteiro
            )
            
            # ANO_FIM
            df_final['ANO_FIM'] = (
                df_final['DATA DE TERMINO DA ATIVIDADE']
                .fillna('0000')                    # trata valores nulos
                .replace('', '0000')               # trata strings vazias
                .str[-4:]
                .astype(int)
            )
            #Onde a data de inicio e fim de atividade for vazio, inserir np.nan
   
            # Define o caminho e nome do arquivo final e o salva em formato CSV
            output_file = os.path.join(processed_dir, "PJ_BR.csv")
            df_final.to_csv(output_file, index=False, encoding='utf-8')
            print("Dados baixados e processados com sucesso!")
            
            # Retorna o DataFrame final
            return df_final
            
    # Se a lista 'pessoas_juridicas_br' estiver vazia, retorna None
    print("Nenhum dado foi processado.")
    return None

#%% Funções de importação de base de dados

def ibama_production_data_v1(repo_path):
    """
    Processa dados de produção do IBAMA a partir de arquivos XLSX, limpando e
    consolidando os dados.
    V1 - Base de dados de 2017 até 2023
    Vou pegar apenas até 2020 e mesclar com o outro
    
    Parâmetros:
        repo_path (str): Caminho raiz do repositório onde estão as pastas inputs
        /outputs
    
    Retorna:
        pd.DataFrame: DataFrame com os dados processados
        str: Caminho do arquivo CSV salvo
    """

    # Define os caminhos para as pastas de dados brutos e processados
    raw_dir = os.path.join(repo_path, 'inputs','DadosProduçãoIndustrial')
    processed_dir = os.path.join(repo_path, 'inputs','DadosProduçãoIndustrial')
    
    # Garante que as pastas de destino existam; se não, elas são criadas
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    try:
        # Caminho completo do arquivo
        file_path = os.path.join(raw_dir, 'DadosProduçãoBruto.xlsx')
        
        # Lê as duas abas do Excel
        aba1 = pd.read_excel(file_path, sheet_name=0, dtype={'mv.num_cpf_cnpj': str}) # 0 para a primeira aba
        aba2 = pd.read_excel(file_path, sheet_name=1, dtype={'mv.num_cpf_cnpj': str}) # 1 para segunda aba
                
        # Combina as abas, deleta as linhas duplicadas
        df_ibama_prod = pd.concat([aba1, aba2], ignore_index=True).drop_duplicates()
        
        # Padronização dos dados com a função clean_text
        df_ibama_clean = df_ibama_prod.copy()
        df_ibama_clean['mv.nom_municipio'] = df_ibama_clean['mv.nom_municipio'].apply(clean_text)
        df_ibama_clean['mv.nom_pessoa'] = df_ibama_clean['mv.nom_pessoa'].apply(clean_text)
        df_ibama_clean['mv.nom_municipio'] = df_ibama_clean['mv.nom_municipio'].str.replace(
            r"SANT'? ?ANA DO LIVRAMENTO", "SANTANA DO LIVRAMENTO", regex=True
            )
        df_ibama_clean['mv.nom_municipio'] = df_ibama_clean['mv.nom_municipio'].str.replace(
            r"PRESIDENTE CASTELLO BRANCO", "PRESIDENTE CASTELO BRANCO", regex=True
            )
        
        #vou pegar apenas os anos de 17 até 20
        df_ibama_clean_17_20 = df_ibama_clean.loc[df_ibama_clean['num_ano']<2021] 
        
        # Salva o resultado
        output_file = os.path.join(processed_dir, 'DadosProduçãoTratadoV1.csv')
        df_ibama_clean_17_20.to_csv(output_file, index=False, encoding='utf-8') #Remove índices
        
        return df_ibama_clean_17_20
        
    # Caso dê erro, informa ao usuário
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo RAPP.xlsx não encontrado em {raw_dir}")
    except Exception as e:
        raise Exception(f"Erro ao processar dados: {str(e)}")
        
        
def ibama_production_data_v2(repo_path):
    """
    Processa dados de produção do IBAMA a partir de arquivos XLSX, limpando e
    consolidando os dados.
    V2 - Base de dados de 2021 até 2024 (+1 ano em relação ao v1, mas começa depois)
    vou pegar inteiro p mesclar com o outro
    
    Parâmetros:
        repo_path (str): Caminho raiz do repositório onde estão as pastas inputs
        /outputs
    
    Retorna:
        pd.DataFrame: DataFrame com os dados processados
        str: Caminho do arquivo CSV salvo
    """

    # Define os caminhos para as pastas de dados brutos e processados
    raw_dir = os.path.join(repo_path, 'inputs','DadosProduçãoIndustrial')
    processed_dir = os.path.join(repo_path, 'inputs','DadosProduçãoIndustrial')
    
    # Garante que as pastas de destino existam; se não, elas são criadas
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    try:
        # Caminho completo do arquivo
        file_path = os.path.join(raw_dir, 'DadosProduçãoBrutoV2.xlsx')
        
        # Lê as duas abas do Excel
        df_ibama_prod = pd.read_excel(file_path, sheet_name=0, dtype={'mv.num_cpf_cnpj': str}) # 0 para a primeira aba
                
        # Padronização dos dados com a função clean_text
        df_ibama_clean_21_24 = df_ibama_prod.copy()
        df_ibama_clean_21_24['mv.nom_municipio'] = df_ibama_clean_21_24['mv.nom_municipio'].apply(clean_text)
        df_ibama_clean_21_24['mv.nom_pessoa'] = df_ibama_clean_21_24['mv.nom_pessoa'].apply(clean_text)
        df_ibama_clean_21_24['mv.nom_municipio'] = df_ibama_clean_21_24['mv.nom_municipio'].str.replace(
            r"SANT'? ?ANA DO LIVRAMENTO", "SANTANA DO LIVRAMENTO", regex=True
            )
        df_ibama_clean_21_24['mv.nom_municipio'] = df_ibama_clean_21_24['mv.nom_municipio'].str.replace(
            r"PRESIDENTE CASTELLO BRANCO", "PRESIDENTE CASTELO BRANCO", regex=True
            )

        # Salva o resultado
        output_file = os.path.join(processed_dir, 'DadosProduçãoTratadoV2.csv')
        df_ibama_clean_21_24.to_csv(output_file, index=False, encoding='utf-8') #Remove índices
        
        return df_ibama_clean_21_24
        
    # Caso dê erro, informa ao usuário
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo RAPP.xlsx não encontrado em {raw_dir}")
    except Exception as e:
        raise Exception(f"Erro ao processar dados: {str(e)}")
        
def import_products_code(repo_path):
    '''
    Importa, limpa e exporta a tabela de códigos de produtos do IBGE (PRODLIST).
    
    Verificar online: https://servicos.ibama.gov.br/ctfcd/manual/html/lista_produtos.htm
    Baixar excel: https://www.ibge.gov.br/estatisticas/metodos-e-classificacoes/classificacoes-e-listas-estatisticas/9153-lista-de-produtos-da-industria.html
   
    A função lê um arquivo Excel, remove cabeçalhos repetidos e linhas
    indesejadas, e retorna um DataFrame pronto para uso.

    Parâmetros:
        repo_path (str): Caminho para a pasta raiz do projeto.

    Retorna:
        pd.DataFrame: Tabela de códigos de produtos limpa.   
    '''
    raw_dir = os.path.join(repo_path, 'inputs','MaterialBaixado')
    processed_dir = os.path.join(repo_path, 'outputs')
    
    # Garante que as pastas de destino existam; se não, elas são criadas
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    # Caminho do arquivo
    xlsx_path = os.path.join(raw_dir,'CódigosProdutosIBGE.xlsx')

    # Lê o CSV com header na linha 1 (índice 1)
    cod_produto = pd.read_excel(xlsx_path, header=2, dtype={'PRODLIST': str})
    # Remove linhas com 'PRODLIST' ou NaN na coluna 'PRODLIST'
    cod_produto = cod_produto[~cod_produto['PRODLIST'].isin(['PRODLIST'])]
    cod_produto = cod_produto[~cod_produto['PRODLIST'].astype(str).str.startswith('CNAE')]
    cod_produto = cod_produto.dropna(subset=['PRODLIST'])

    # Reinicia o índice
    cod_produto.reset_index(drop=True, inplace=True)
    
    output_file = os.path.join(processed_dir,'CodProdutoParaClassificar.xlsx')
    cod_produto.to_excel(output_file, index=False)
    
    return cod_produto

def import_treat_export_food_code(repo_path):
    '''
    Importa e limpa a tabela de códigos de produtos do IBGE (PRODLIST)
    APENAS PRODUTOS ALIMENTÍCIOS (escopo inicial do TCC).
    
    Verificar online: https://servicos.ibama.gov.br/ctfcd/manual/html/lista_produtos.htm
    Baixar excel: https://www.ibge.gov.br/estatisticas/metodos-e-classificacoes/classificacoes-e-listas-estatisticas/9153-lista-de-produtos-da-industria.html
   
    A função lê um arquivo Excel, remove cabeçalhos repetidos e linhas
    indesejadas, e retorna um DataFrame pronto para uso.

    Parâmetros:
        repo_path (str): Caminho para a pasta raiz do projeto.

    Retorna:
        pd.DataFrame: Tabela de códigos de produtos limpa.   
    '''
    raw_dir = os.path.join(repo_path, 'inputs','MaterialBaixado')
    processed_dir = os.path.join(repo_path, 'outputs')
    
    # Garante que as pastas de destino existam; se não, elas são criadas
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    # Caminho do arquivo
    xlsx_path = os.path.join(raw_dir,'CódigosProdutosIBGE.xlsx')

    # Lê o CSV com header na linha 1 (índice 1)
    cod_produto = pd.read_excel(xlsx_path, header=2, dtype={'PRODLIST': str})
    # Remove linhas com 'PRODLIST' ou NaN na coluna 'PRODLIST'
    cod_produto = cod_produto[~cod_produto['PRODLIST'].isin(['PRODLIST'])]
    cod_produto = cod_produto[~cod_produto['PRODLIST'].astype(str).str.startswith('CNAE')]
    cod_produto = cod_produto.dropna(subset=['PRODLIST'])

    # Reinicia o índice
    cod_produto.reset_index(drop=True, inplace=True)
       
    return cod_produto

#%% Funções para mesclar e filtrar base de dados

def merge_cnpj_prod(cnpj,prod):
    '''
    
    Processa dados de atividade por CNPJ e Município para obter coordenada,
    código de atividade e código de categoria; Remove os que são CPF   
    
    Parâmetros: 
        - cnpj, prod = Base de dados de CNPJ e de Produção do ibama, e baixado pelas funções:
            -download_ibama_ctf_data
            -ibama_production_data
            
     Retorna:
         pd.DataFrame: DataFrame com os dados concatenados
         
    '''
    
    # Uma mesma indústria com o mesmo cnpj e lat lon produz produtos diferentes
    # Ou seja, multiplos codigo de categoria e codigo de atividade
    # Por isso, fiz um groupby onde os códigos de categoria e atividade viram uma lista para um mesmo CNPJ
    # acabei não utilizando estes códigos, mas mantive por segurança
    cnpj = cnpj.groupby(['CNPJ', 'MUNICIPIO', 'LATITUDE', 'LONGITUDE','ESTADO','SITUACAO CADASTRAL']).agg({
        'CODIGO DA CATEGORIA': list,
        'CODIGO DA ATIVIDADE': list,
        'ANO_INICIO': list,
        'ANO_FIM': list
    }).reset_index()
    
    
    # Fazer o merge com o df_ibama (sem duplicar linhas)
    df_ibama_completo = pd.merge(
        left=cnpj, #tabela unida a esqueda
        right=prod, #tabela unida a direita
        left_on=['CNPJ', 'MUNICIPIO'], #chaves da tabela a esqueda
        right_on=['mv.num_cpf_cnpj', 'mv.nom_municipio'], #chaves da tabela a direita
        how='right', # todas as linhas da tabela da direita (prod) serão mantidas.
        indicator=True #informa a condição da mesclagem (right_only - não estava em CNPJ)
    )
    
       
    return df_ibama_completo

def conecta_ibama_ef(df_ibama, df_ef, df_conector):
    # Garantir que os códigos estejam no mesmo tipo
    df_conector[['PRODLIST', 'NFR', 'Table']] = df_conector[['PRODLIST', 'NFR', 'Table']].astype(str)
    df_ibama['cod_produto'] = df_ibama['cod_produto'].astype(str)
    
    # Mesclar df_ibama com o conector via código do produto
    df_merged = df_ibama.merge(
        df_conector[['PRODLIST', 'NFR', 'Table']],
        left_on='cod_produto',
        right_on='PRODLIST',
        how='left'
    )

    # Realizar o merge com a base ef (EEA)
    df_final = df_merged.merge(
        df_ef,
        left_on=['NFR', 'Table'],
        right_on=['NFR', 'Table'],
        how='left'
    )

    # Remover coluna auxiliar
    df_final = df_final.drop(columns=['PRODLIST'])

    return df_final

def converter_para_hl(df_conversao, qtd_produzida, unidade_medida, cod_produto=None):
    """
    Converte uma quantidade para hectolitros (hL) baseado nas regras do CSV.
    
    Parâmetros:
    - qtd_produzida: quantidade a ser convertida
    - unidade_medida: unidade de medida original
    - cod_produto: código do produto (opcional, para regras específicas)
    
    Retorna:
    - Quantidade convertida em hL ou np.nan se não encontrar conversão
    """
    # Primeiro tenta encontrar por código específico do produto
    if cod_produto is not None:
        # Verifica se o código começa com algum valor específico no CSV
        mascara_cod = df_conversao['cod_produto'].astype(str).str.startswith(str(cod_produto))
        mascara_unidade = df_conversao['unidade'] == unidade_medida
        resultado_especifico = df_conversao[mascara_cod & mascara_unidade]
        
        if not resultado_especifico.empty:
            return qtd_produzida * resultado_especifico.iloc[0]['hl']
    
    # Se não encontrou específico, procura nas regras gerais
    mascara_geral = (df_conversao['cod_produto'] == 'geral') & (df_conversao['unidade'] == unidade_medida)
    resultado_geral = df_conversao[mascara_geral]
    
    if not resultado_geral.empty:
        return qtd_produzida * resultado_geral.iloc[0]['hl']
    
    # Se não encontrou em nenhum lugar, retorna NaN
    return pd.NA


def agrupar_e_somar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa um DataFrame por um conjunto de colunas-chave e agrega os valores.

    As colunas numéricas (não presentes nas chaves de agrupamento) são somadas.
    As colunas não numéricas são preenchidas com o primeiro valor do grupo.

    Args:
        df (pd.DataFrame): O DataFrame de entrada para ser processado.

    Returns:
        pd.DataFrame: Um novo DataFrame com os dados agrupados e agregados.
    """
    # 1. Define as colunas que serão a "chave" para o agrupamento
    colunas_agrupamento = [
        'mv.num_cpf_cnpj', 
        'mv.nom_pessoa',
        'mv.nom_municipio',
        'num_ano',
        'cod_produto',
        'unidade_medida',
        'sig_unidmed'
    ]

    print(f"Número de linhas antes do agrupamento: {len(df)}")

    # 2. Cria dinamicamente as regras de agregação
    # O objetivo é dizer ao pandas o que fazer com cada coluna que NÃO está no agrupamento.
    regras_agregacao = {}
    colunas_para_agregar = [col for col in df.columns if col not in colunas_agrupamento]

    for coluna in colunas_para_agregar:
        # Verifica se a coluna contém dados numéricos (int, float, etc.)
        if pd.api.types.is_numeric_dtype(df[coluna]):
            regras_agregacao[coluna] = 'sum'  # Se for numérica, some
        else:
            regras_agregacao[coluna] = 'first' # Se não for, pegue o primeiro valor

    # 3. Executa o agrupamento e a agregação
    # .groupby() cria os grupos
    # .agg() aplica as regras que definimos
    # .reset_index() transforma as colunas de agrupamento de volta em colunas normais
    df_agrupado = df.groupby(colunas_agrupamento).agg(regras_agregacao).reset_index()

    print(f"Número de linhas após o agrupamento: {len(df_agrupado)}")
    
    return df_agrupado

#%% Função de tratamento de outliers

def tratamento_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica uma série de filtros e tratamentos em um DataFrame de produção.
    
    Esta versão calcula uma mediana refinada (excluindo outliers) para ser usada
    especificamente no preenchimento de dados faltantes, tornando a imputação
    mais robusta.

    Args:
        df (pd.DataFrame): O DataFrame de entrada.

    Returns:
        pd.DataFrame: Um novo DataFrame com os outliers tratados, as séries 
                      temporais preenchidas e a coluna de observação.
    """
    
    print("Iniciando o tratamento de outliers e preenchimento de dados...")
    
    # --- Validação e Pré-processamento ---

    # Lista de colunas essenciais para o funcionamento da função.
    colunas_necessarias = [
        'mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto', 'num_ano',
        'SITUACAO CADASTRAL', 'Produção (Ton ou hL)'
    ]
    # Valida se todas as colunas necessárias existem no DataFrame de entrada.
    for col in colunas_necessarias:
        if col not in df.columns:
            raise ValueError(f"A coluna obrigatória '{col}' não foi encontrada no DataFrame.")

    # Define as colunas que identificam uma série temporal única (unidade produtiva + produto).
    group_cols = ['mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto']
    
    # Define as colunas para identificar registros duplicados (mesma unidade, produto e ano).
    agg_cols = group_cols + ['num_ano']
    # Verifica se existem registros duplicados.
    if df.duplicated(subset=agg_cols).any():
        print("Detectados registros duplicados. Agregando valores...")
        # Cria um dicionário de agregação: soma a produção e pega o primeiro valor para as outras colunas.
        agg_dict = {'Produção (Ton ou hL)': 'sum'}
        other_cols = [col for col in df.columns if col not in agg_cols and col != 'Produção (Ton ou hL)']
        for col in other_cols:
            agg_dict[col] = 'first'
        # Agrupa os dados e aplica as regras de agregação para consolidar as duplicatas.
        df = df.groupby(agg_cols, as_index=False).agg(agg_dict)
        print("Agregação de duplicatas concluída.")

    # --- 1º Filtro: Verificar histórico de reporte ---
    print("Aplicando o 1º Filtro: Verificação do histórico de reporte.")

    # Função auxiliar para verificar se um grupo tem histórico de reporte suficiente.
    def _verificar_historico(anos_serie):
        anos_unicos = sorted(anos_serie.unique())
        # Critério 1: Pelo menos 5 anos distintos de reporte.
        if len(anos_unicos) >= 5: return True
        # Critério 2: Pelo menos 3 anos consecutivos de reporte.
        if len(anos_unicos) >= 3:
            for i in range(len(anos_unicos) - 2):
                if anos_unicos[i+2] - anos_unicos[i] == 2: return True
        return False

    # Aplica a função de verificação a cada grupo e cria uma máscara booleana.
    mascara_historico = df.groupby(group_cols)['num_ano'].transform(_verificar_historico)
    # Filtra o DataFrame, mantendo apenas os grupos com histórico suficiente.
    df_filtrado_1 = df[mascara_historico].copy()
    
    print(f"O DataFrame foi reduzido para {len(df_filtrado_1)} linhas após o 1º filtro.")
    
    # Se nenhum dado passar pelo filtro, retorna um DataFrame vazio.
    if df_filtrado_1.empty:
        print("Nenhum dado passou pelo primeiro filtro. Retornando DataFrame vazio.")
        return pd.DataFrame()
        
    # Inicializa a coluna de rastreamento do tratamento, marcando todos os dados como 'Original' por padrão.
    df_filtrado_1['obs_tratamento'] = 'Original'

    # --- 2º Filtro: Substituir outliers pela mediana ---
    print("Aplicando o 2º Filtro: Tratamento de outliers.")
    
    # Calcula a mediana da produção para cada grupo. 'transform' alinha o resultado de volta ao DataFrame original.
    medianas_grupo = df_filtrado_1.groupby(group_cols)['Produção (Ton ou hL)'].transform('median')
    # Cria uma máscara booleana para identificar outliers (produção >= 3x a mediana do grupo).
    mascara_outliers = df_filtrado_1['Produção (Ton ou hL)'] >= (3 * medianas_grupo)
    
    # Usa a máscara para atualizar a coluna de rastreamento, marcando os outliers.
    df_filtrado_1.loc[mascara_outliers, 'obs_tratamento'] = 'Outlier substituído'
    
    # Cria uma coluna temporária para armazenar os valores de produção tratados.
    df_filtrado_1['Producao_Tratada'] = df_filtrado_1['Produção (Ton ou hL)']
    # Substitui os valores dos outliers pela mediana do grupo correspondente na coluna tratada.
    df_filtrado_1.loc[mascara_outliers, 'Producao_Tratada'] = medianas_grupo[mascara_outliers]
    
    num_outliers = mascara_outliers.sum()
    print(f"{num_outliers} outliers foram identificados e substituídos.")

    # --- 3º Filtro: Preencher anos faltantes ---
    print("Aplicando o 3º Filtro: Preenchimento de anos faltantes.")

    # Função auxiliar que será aplicada a cada grupo para preencher suas lacunas.
    def _preencher_anos_faltantes(grupo):
        # Define 'num_ano' como índice para permitir a reindexação baseada no tempo.
        grupo = grupo.set_index('num_ano')
        
        # Coleta informações do grupo para definir o range de anos.
        situacao = grupo['SITUACAO CADASTRAL'].iloc[0]
        ano_min = grupo.index.min()
        ano_max_reportado = grupo.index.max()
        ANO_FINAL_ATIVAS = 2024
        
        ''' Estou com dúvida se ele está aplicando o ultimo ano op cada cnpj'''
        # Determina o último ano da série temporal: 2024 para empresas ativas, ou o último ano reportado para as demais.
        if isinstance(situacao, str) and situacao.upper() == 'ATIVA':
            ano_final = ANO_FINAL_ATIVAS
        else:
            ano_final = ano_max_reportado
            
        # Cria um novo índice contendo todos os anos, do início ao fim da série.
        novo_indice = pd.Index(range(ano_min, ano_final + 1), name='num_ano')
        # Reindexa o grupo. Isso cria novas linhas com valores NaN para os anos que estavam faltando.
        grupo_completo = grupo.reindex(novo_indice)
        
        # As novas linhas criadas terão NaN na coluna 'obs_tratamento'. Preenchemos com a marcação correta.
        grupo_completo['obs_tratamento'] = grupo_completo['obs_tratamento'].fillna('Dado preenchido')
        
        # --- LÓGICA DA MEDIANA REFINADA ---
        # 1. Filtra o grupo original para pegar apenas os dados que não foram classificados como outliers.
        grupo_sem_outliers = grupo[grupo['obs_tratamento'] == 'Original']
        
        # 2. Calcula a mediana "refinada" com base nesses dados 'limpos'.
        #    Se um grupo for composto apenas de outliers (caso raro), usa a mediana do grupo todo como fallback.
        if not grupo_sem_outliers.empty:
            mediana_para_preenchimento = grupo_sem_outliers['Producao_Tratada'].median()
        else:
            mediana_para_preenchimento = grupo['Producao_Tratada'].median() # Fallback

        # 3. Usa essa mediana refinada para preencher os valores de produção das novas linhas (que eram NaN).
        grupo_completo['Producao_Tratada'] = grupo_completo['Producao_Tratada'].fillna(mediana_para_preenchimento)
        # --- FIM DA LÓGICA ---
        
        # Atualiza a coluna de produção final com os valores tratados e preenchidos.
        grupo_completo['Produção (Ton ou hL)'] = grupo_completo['Producao_Tratada']
        
        # Preenche os valores das outras colunas para as novas linhas (ex: CNPJ, situação) usando os valores existentes.
        # Retorna o índice 'num_ano' para ser uma coluna novamente.
        return grupo_completo.ffill().bfill().reset_index()

    # Aplica a função de preenchimento a cada grupo do DataFrame.
    df_final = df_filtrado_1.groupby(group_cols, group_keys=False).apply(_preencher_anos_faltantes)
    
    # Remove a coluna temporária de produção tratada, pois já foi copiada para a coluna final.
    df_final.drop(columns=['Producao_Tratada'], inplace=True)
    # Garante que a coluna de ano seja do tipo inteiro.
    df_final['num_ano'] = df_final['num_ano'].astype(int)
    
    print(f"Processo finalizado. O DataFrame final contém {len(df_final)} linhas.")
    
    # Reseta o índice do DataFrame final para ser uma sequência limpa (0, 1, 2, ...).
    return df_final.reset_index(drop=True)