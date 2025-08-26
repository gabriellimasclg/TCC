# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 10:02:47 2025

@author: glima
"""

#%% Importando bibliotecas necessárias

import pandas as pd
import os
from functions_AnaliseDados import plot_emissoes_estado, plotar_mosaico_estado, plotar_mosaico_emissoes, analisar_tendencia_nmvc, plot_emissao, criar_cubo_emissoes_geograficas
import matplotlib.pyplot as plt

#%% Definindo Paths e importando 
repo_path = os.path.dirname(os.getcwd())
figpath = os.path.join(repo_path,'figures')

#importar csv com inventário
df = pd.read_csv(os.path.join(repo_path,'inputs','inventarioEmissoesIndustriaisIndustriaAlimenticiaBR.csv'))
df['LONGITUDE'] = df['LONGITUDE'].str.replace(',', '.', regex=False).astype(float)
df['LATITUDE'] = df['LATITUDE'].str.replace(',', '.', regex=False).astype(float)

#%% análises e gráficos iniciais

# Análise de tendência no BR
tendênciaBR = analisar_tendencia_nmvc(df, ['NFR'])

# Análise de tendência por estado
tendênciaUF = analisar_tendencia_nmvc(df, ['ESTADO'])


# Analisar emissão por estado (ver quais emitem mais p analisar no TCC)
df_emissoes_uf = (
    df.groupby('ESTADO')[
        ['Emissão NMCOV (ton)',
         'Emissão NMCOV CI_lower (ton)',
         'Emissão NMCOV CI_upper (ton)']
    ].sum()
)

df_emissoes_uf_tendencia = df_emissoes_uf.merge(tendênciaUF[['ESTADO','tendência']], on='ESTADO', how='left')

# Todos os estados
plot_emissoes_estado(df_emissoes_uf_tendencia,figpath)

plot_emissao(df,figpath)

#%% Geração do cube-data

# Definir parâmetros (se quiser mudar os padrões)
limites_brasil = {'xmin': -74, 'xmax': -34, 'ymin': -34, 'ymax': 6}
resolucao_graus = 0.5 # Resolução de 0.5x0.5 graus

# Geração do cube-data
ds_emissoes_completo = criar_cubo_emissoes_geograficas(
    df_emissoes=df,
    coluna_emissao='Emissão NMCOV (ton)',
    resolucao=resolucao_graus,
    limites_grid=limites_brasil
)

#%% Geração do gráfico de emissão agregada pixelada nacional

# Chamar a função para gerar e salvar o gráfico
figura, eixos = plotar_mosaico_emissoes(
    ds=ds_emissoes_completo, #Dataset com os dados
    titulo='Evolução da Emissão de NMVOC da Indústria Alimentícia',
    cbar_label='Emissão de NMVOC (ton)',
    scale='log', # Pode ser 'log' ou 'linear'
    save_path=os.path.join(figpath,'Emissões NMCOV no Brasil Anual Geolocalizada.png')
)

#%% Mosaico de análise por estado

#Estados analisados
tendencia_uf = analisar_tendencia_nmvc(df, ['ESTADO'])
estados_para_analisar = sorted(df['ESTADO'].dropna().unique())

#Geração do mosaico de análise por estado
for uf in estados_para_analisar:
       
    save_path = os.path.join(figpath, f'mosaico_emissao_{uf.replace(" ", "_")}.png')
    
    fig = plotar_mosaico_estado(
        df=df,
        ds=ds_emissoes_completo,
        tendencia_uf_df=tendencia_uf,
        estado_alvo=uf,
        save_path=save_path
    )
    plt.close(fig)


