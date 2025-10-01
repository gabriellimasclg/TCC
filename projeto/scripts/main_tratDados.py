# -*- coding: utf-8 -*-
"""
Created on Mon Jul 21 09:47:06 2025

@author: glima
"""

#%%============================ Bibliotecas====================================
import os
import pandas as pd
import numpy as np
from functions_TratDados import sinalizar_variacoes_producao_v2, verif_outliers_manual_v02, sinalizar_variacoes_producao, verif_outliers_manual, tratamento_outliers_V2, CNPJAnalysis, download_ibama_ctf_data, ibama_production_data_v1,ibama_production_data_v2, import_treat_export_food_code, agrupar_e_somar_dados, merge_cnpj_prod, conecta_ibama_ef, tratamento_outliers

#%%===========================Criação da Base Geral============================

# Caminho da pasta do projeto
# Dentro do repo_path deve-se ter as pastas
# repo_path
#   |--figures
#   |--inputs
#   |--outputs
#       |--log
#   |--scripts

repo_path = os.path.dirname(os.getcwd())

# Faz o downloado da base de dados com CNPJ + Coordenadas
df_ibama_cnpj = download_ibama_ctf_data(repo_path)

# Importa DF com Dados de produção com CNPJ + Código de Produto + Produção
df_ibama_prod_v1 = ibama_production_data_v1(repo_path) 
df_ibama_prod_v2 = ibama_production_data_v2(repo_path) 
df_ibama_prod = pd.concat([df_ibama_prod_v1,df_ibama_prod_v2])
df_ibama_prod = agrupar_e_somar_dados(df_ibama_prod)

# DF com Produção + Código de produto + Coordenadas
df_ibama = merge_cnpj_prod(df_ibama_cnpj,df_ibama_prod) #mesclar p obter coordenadas e código de atividade

#contabiliza quantos são CPF (11 dígitos) e quantos são CNPJ (14 dígitos)
CNPJAnalysis(df_ibama)

#Exporta log com dados desconsiderados
df_ibama.to_excel(os.path.join(repo_path,'outputs','log','log_v01_remocaoCPF.xlsx'))

#Remover CPFs (desconsiderável)
df_ibama = df_ibama[df_ibama['mv.num_cpf_cnpj'].str.len() == 14]


#%%============== Base com NFR + Código do Produto + Produção =================

#Base de dados com todos os Códigos de Produto
cod_produto= import_treat_export_food_code(repo_path)

#### ADICIONAR COLUNA "ALIMENTOS E BEBIDAS"
#vou filtrar os códigos de produto de interesse e exportar (itens que se enquadram no 2.h.2 no nfr)
prefixos = ('10', '11')

cod_produto_interesse = cod_produto[
    cod_produto['PRODLIST'].astype(str).str.startswith(prefixos)
]

#exportei para classificar manualmente
cod_produto_interesse.to_excel(os.path.join(repo_path, 'outputs','CodProdutoParaClassificar.xlsx'), index=False)

#Exportei manualmente para a pasta inputs

#Importei material gerado manualmente
CodProdutoClassificadoNFR = pd.read_excel(os.path.join(repo_path,'inputs','MaterialGeradoManualmente','CodProdutoClassificadoNFR.xlsx'),
                                          dtype={'PRODLIST': str})

#filtrei os alimentos que tem emissões a serem consideradas
CodProdutoClassificadoNFR = CodProdutoClassificadoNFR[CodProdutoClassificadoNFR['EmissaoNMCOV_NFR'] != 'Não']

# Base de dados dos fatores de emissão tier 2
eea_ef = pd.read_csv(os.path.join(repo_path, 'inputs','MaterialBaixado', 'EF_tier2.csv'))

# Conexão das bases de dados de apenas os classificados como emissores de NMCOV
df_ibama_EF = conecta_ibama_ef(df_ibama,eea_ef,CodProdutoClassificadoNFR)

#log que indica os removidos por não serem alimentos emissores de COV
# 1. Crie a coluna com o valor padrão para todas as linhas
df_ibama_EF['status_v02'] = 'Dado filtrado'

# 2. Use .loc para encontrar as linhas que batem com a condição e mude o valor apenas nelas
df_ibama_EF.loc[df_ibama_EF['NFR'] == '2.H.2', 'status_v02'] = 'Alimento emissor de COV - Dado considerado'

#Exporta log com dados desconsiderados
df_ibama_EF.to_excel(os.path.join(repo_path,'outputs','log','log_v02_apenasAlimentosEmissoresMantidos.xlsx'))

#Filtrar apenas os produtos com emissão
df_ibama_EF = df_ibama_EF[df_ibama_EF['Table'].notna()]


#%% ETAPA POSTERIOR DE VALIDAÇÃO DAS UNIDADES

# 1. Adicionar a coluna com a contagem total de aparições de cada grupo
# A função transform() aplica a contagem de volta a cada linha do DataFrame original.
df_ibama_EF['contagem_total_grupo'] = df_ibama_EF.groupby(
    ['CNPJ', 'MUNICIPIO', 'cod_produto']
)['cod_produto'].transform('count')

# 2. Agora, agrupe incluindo a nova coluna e conte as unidades de medida
df_valid_final = df_ibama_EF.groupby(
    ['CNPJ', 'MUNICIPIO', 'cod_produto', 'contagem_total_grupo']
)['unidade_medida'].value_counts().reset_index(name='contagem_unidade')

# 3. Filtrando os grupos inconsistentes (a lógica é a mesma)
grupos_inconsistentes = df_valid_final[df_valid_final.duplicated(
    subset=['CNPJ', 'MUNICIPIO', 'cod_produto'], keep=False
)]

# 4. Criando lista com industrias para verificar
df_para_verificar = grupos_inconsistentes[['CNPJ', 'MUNICIPIO', 'cod_produto']] \
                        .drop_duplicates() \
                        .reset_index(drop=True)

#Esse DF vai indicar quais CNPJs e Produtos verificar manualmente
df_para_verificar.to_excel(os.path.join(repo_path,'outputs','Auxiliar_dfUnidadesInconsistentes_VerifManual.xlsx'), index=False)


# colunas para rastreabilidade
df_ibama_EF['status_v03'] = 'Unidade mantida conforme dado original'
df_ibama_EF['sig_unidmed_novo'] = df_ibama_EF['sig_unidmed'] 
df_ibama_EF['qtd_produzida_novo'] = df_ibama_EF['qtd_produzida'] 

#vou exportar e, se eu alterar, vou trocar a coluna para
# 'Unidade Alterada. Justificativa: XXX' --> inserir justificativa
#   ex: Unidade estava incondizente com grandeza das demais

df_ibama_EF.to_excel(os.path.join(repo_path,
                                  'outputs',
                                  'vefirManual_01_unidadesPorProdutoPorProdutor.xlsx'))

# aí dps de verificar manualmente, colocar no inputs manual 

df_ibama_EF_und = pd.read_excel(os.path.join(repo_path,
                                             'inputs',
                                             'MaterialGeradoManualmente',
                                             'vefirManual_01_unidadesPorProdutoPorProdutor.xlsx'),
                                dtype={'cod_produto':'string'})

#trecho de código que coloca no log
df_ibama_EF_und.to_excel(os.path.join(repo_path,
                                      'outputs',
                                      'log',
                                      'log_v03_AnaliseManualUndInconsistente.xlsx'))

#%%=============== Ajuste das unidades para calcular emissão ==================

# fazer um unique; Sendo Unit a unidade desejada e os outros referentes ao RAPP
# Criei contagem para ter mais segurança ao descartar alguma unidade
# (vou colocar fator zero nesses)

# Define as colunas para agrupar
colunas_agrupamento = ['cod_produto', 'nom_produto', 'sig_unidmed_novo', 'Unit']

# Agrupa pelas colunas e conta o tamanho de cada grupo
df_unidades_bruto = df_ibama_EF_und.groupby(colunas_agrupamento).size().reset_index(name='contagem')

#exportei para colocar os fatores de conversão manualmente
df_unidades_bruto.to_excel(os.path.join(repo_path, 'outputs','UnidadesFatorConversãoBruto.xlsx'), index=False)

#Importar base com fatores de conversão
df_unidades = pd.read_excel(os.path.join(repo_path, 'inputs','MaterialGeradoManualmente', 'UnidadesFatorConversão.xlsx'),
                            dtype={'cod_produto_fc': str})


#%%===========Geração do DF de produção bruto com unidades adequadas===========

# fzr merge de cod_produto e sig_unidmed
df_producao_bruto = pd.merge(
        left=df_ibama_EF_und, #tabela unida a esqueda
        right=df_unidades, #tabela unida a direita
        left_on=['cod_produto', 'sig_unidmed_novo'], #chaves da tabela a esqueda
        right_on=['cod_produto_fc', 'sig_unidmed_fc'], #chaves da tabela a direita
        how='left', # todas as linhas da tabela da esquerda (df_ibama_EF) serão mantidas.
    )

#multiplicar a produção pelo fatorConversão
df_producao_bruto['Produção (Ton ou hL)'] = (df_producao_bruto['qtd_produzida_novo'].astype(float) * 
                                       df_producao_bruto['fatorConversao'].astype(float))

df_producao_notnull = df_producao_bruto.copy()

#exportei para verificação
df_producao_notnull.to_excel(os.path.join(repo_path, 'outputs','df_producao_notnullVerif.xlsx'), index=False)


#%%=============== Remoção de outliers e calculo das emissões ==================

#Ajuste dos outliers de produção
df_producao_para_verificar = tratamento_outliers_V2(df_producao_notnull)

#exportar para ter registro. Vou verificar os outliers posteriormente em conjunto
#com o Q95 para focar nos, de fato, maiores
df_producao_para_verificar.to_excel(os.path.join(repo_path,'outputs','log','log_v04_outliersTratado.xlsx'), index=False)

df_producao = df_producao_para_verificar.copy()

#Remover algumas colunas
#colunas_remover = ['CNPJ', 'MUNICIPIO', 'SITUACAO CADASTRAL', 'mv.nom_pessoa', 'unidade_medida_x',
 #                  'sig_unidmed', 'nom_produto_x', 'qtd_produzida', 'lei_sigilo','mv.sig_uf', 'tipo',
  #                 'Reference', 'contagem','Fonte', 'Observaçoes', 'obs_tratamento']


#%% verif percentil 90

#Prox Etapas
'''  
verificar no material exportado se é outlier iq1,5 e está no q90
Verificar via satelite e no site
remover qualquer um q seja apenas importação e exportação (da p ver no nome)
e verificar valores meio sem sentido

então, aplicar no código para:
substituir com a mediana do valor anterior e posterior. Ou só um deles em
casos dos extremos
'''
# Classificar o tipo de bebida
def classificar_produto(codigo):
    if codigo.startswith('Sugar'):
        return 'Açucar','beige'
    elif codigo.startswith('Coffee'):
        return 'Torrefação do café','brown'
    elif codigo.startswith('Margarine'):
        return 'Margarina e gorduras sólidas','yellow'
    elif codigo.startswith('Cakes'):
        return 'Bolos, biscoitos e cereais matinais','grey'
    elif codigo.startswith('Meat'):
        return 'Preparação de Carnes','salmon'
    elif codigo.startswith('Wine'):
        return 'Vinho','purple'
    elif codigo.startswith('White bread'):
        return 'Pão','pink'
    elif codigo.startswith('Beer'):
        return 'Cerveja','goldenrod'
    else:
        return 'Destilados','lightblue'

df_producao['tipo_industria_nfr'], df_producao['food_color'] = zip(
    *df_producao['Technology'].map(classificar_produto)
)

# 1. Definir as colunas de agrupamento e a coluna de valor
group_cols = 'tipo_industria_nfr'
value_col = 'Produção (Ton ou hL)'
percentil = 0.90

# 2. Calcular o valor do percentil 95 para cada grupo
# Usamos transform() para que o resultado tenha o mesmo índice do df original
p90_grupo = df_producao.groupby(group_cols)[value_col].transform(lambda x: x.quantile(percentil))

# 3. Adicionar o valor do p95 ao DataFrame para facilitar a análise no Excel
df_producao['p90_do_grupo'] = p90_grupo

# 4. Filtrar as linhas onde a produção é maior que o p95 do seu grupo
mascara_outliers_p90 = (df_producao[value_col] > df_producao['p90_do_grupo'])

df_producao['status_v05'] = np.where(mascara_outliers_p90==True,
                                             'produção_superior_q90',
                                             'produção_inferior_q90'
                                             )

df_producao['status_v06'] = 'Dado coerente. Manter'
#Caso não seja coerente, vou colocar "",
# e trocarei pela mediana do mesmo cnpj cord produto. Se não for
#principal critério --> mudar de log. Ex 10^8 p 10^9. Mas não de 999 para 1001. algo mais brusco
# VERIFIQUEI parametros como pandemia e verificava online se era de fato produção
#verificação via satelite
# talvez automatizar para uma verificação automatica da mediana + ou - 20% ?

# Exportar df_prod_notnull como inventário_intermed para verif manual 2
df_producao.to_excel(os.path.join(repo_path,'outputs','vefirManual_02_outliers_q90_verificacao.xlsx'), index=False)

df_outliers_para_verificar =df_producao['mv.num_cpf_cnpj'][mascara_outliers_p90].copy().drop_duplicates()

# 5. Exportar o resultado para um arquivo Excel
df_outliers_para_verificar.to_excel(os.path.join(repo_path,'outputs','Auxiliar_outliers_q90_para_verificacao.xlsx'), index=False)

#importar resultado desta última verificação
df_tratado = pd.read_excel(os.path.join(repo_path,
                                             'inputs',
                                             'MaterialGeradoManualmente',
                                             'vefirManual_02_outliers_q90_verificacao.xlsx'),
                                dtype={'cod_produto':'string'})


#trecho de código que coloca no log
df_ibama_EF_und.to_excel(os.path.join(repo_path,
                                      'outputs',
                                      'log',
                                      'log_v03_AnaliseManualUndInconsistente.xlsx'))

#%% 

# completar com mediana do grupo onde tiver "dado incoerente". Zerar nos outros
# Aplica a função para gerar o DataFrame final corrigido
df_producao_quase = verif_outliers_manual(df_tratado)

df_producao_quase.to_excel(os.path.join(repo_path,
                                      'outputs',
                                      'log',
                                      'log_v03.1_verif_outliers_manual.xlsx'))

# Supondo que 'df_producao_final' seja o seu DataFrame já tratado pela função manual
df_com_flags = sinalizar_variacoes_producao_v2(df_producao_quase)


df_producao_final =  verif_outliers_manual_v02(df_com_flags)


#%%
df_tratado.columns
#Cálculo das emissões
#df_inventario = df_producao.copy().drop(columns=colunas_remover)
df_inventario = df_producao_final.copy()
df_inventario['Emissão NMCOV (kg)'] = (df_inventario['Produção (Ton ou hL)_Revisado_V2'] * df_inventario['Value'].astype(float))
df_inventario['Emissão NMCOV CI_lower (kg)'] = (df_inventario['Produção (Ton ou hL)_Revisado_V2'] * df_inventario['CI_lower'].astype(float))
df_inventario['Emissão NMCOV CI_upper (kg)'] = (df_inventario['Produção (Ton ou hL)_Revisado_V2'] * df_inventario['CI_upper'].astype(float))

# Como edgar é em ton, vou deixar tudo em toneladas
df_inventario['Emissão NMCOV (ton)'] = df_inventario['Emissão NMCOV (kg)']/1000
df_inventario['Emissão NMCOV CI_lower (ton)'] = df_inventario['Emissão NMCOV CI_lower (kg)']/1000
df_inventario['Emissão NMCOV CI_upper (ton)'] = df_inventario['Emissão NMCOV CI_upper (kg)']/1000

#%% Exportar para realizar análises em outro código

df_inventario = df_inventario[df_inventario['Produção (Ton ou hL)_Revisado_V2'] != 0].copy()
df_inventario.to_csv(os.path.join(repo_path,'outputs','inventarioEmissoesIndustriaisIndustriaAlimenticiaBR_V2.csv'), index = False)

df_inventario.columns

df_filtrado = df_inventario[['num_ano', 'mv.num_cpf_cnpj','mv.nom_pessoa','LATITUDE',
                             'LONGITUDE','Emissão NMCOV (ton)','Emissão NMCOV CI_lower (ton)',
                             'Emissão NMCOV CI_upper (ton)']]

df_filtrado.to_csv(os.path.join(repo_path,'outputs','InventarioFiltrado.csv'), index = False)

#%%Verificação: Pq o Acre só tem emissões a partir de 2020?

df_ibama_cnpj_acre = df_ibama_cnpj[df_ibama_cnpj.ESTADO=='ACRE']
df_ibama_cnpj_acre_comida = df_ibama_cnpj_acre[df_ibama_cnpj_acre['CODIGO DA CATEGORIA']==16]
df_acre = merge_cnpj_prod(df_ibama_cnpj_acre_comida,df_ibama_prod)
df_acre_filtro = df_acre[df_acre['CNPJ'].notna()]
# Conexão das bases de dados de apenas os classificados como emissores de NMCOV
df_acre_EF = conecta_ibama_ef(df_acre_filtro,eea_ef,CodProdutoClassificadoNFR)

#Filtrar apenas os produtos com emissão
df_acre_EF = df_acre_EF[df_acre_EF['Table'].notna()]

'''Está certo. De fato, a única empresa com emissões de NMCOV no Acre p/ indústria alimentícia
teve seus primeiros registros em 2020;'''