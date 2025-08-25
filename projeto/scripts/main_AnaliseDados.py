# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 10:02:47 2025

@author: glima
"""

#%% Importando bibliotecas necessárias

import pandas as pd
import os
from analiseDados import plotar_mosaico_estado, plotar_mosaico_emissoes, analisar_tendencia_nmvc, plot_emissao, criar_cubo_emissoes_geograficas
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
# tendênciaBR = analisar_tendencia_nmvc(df, ['NFR'])

# Análise de tendência por estado
# tendênciaUF = analisar_tendencia_nmvc(df, ['ESTADO'])

# plot_emissao(df,figpath)

# plot_emissao(df, figpath,coluna='ESTADO')


#%% Geração do cube-data

# Definir parâmetros (se quiser mudar os padrões)
limites_brasil = {'xmin': -74, 'xmax': -34, 'ymin': -34, 'ymax': 6}
resolucao_graus = 0.5 # Resolução de 0.5x0.5 graus

# Geração do cube-data
ds_emissoes_completo = criar_cubo_emissoes_geograficas(
    df_emissoes=df,
    coluna_emissao='Emissão NMCOV (kg)',
    resolucao=resolucao_graus,
    limites_grid=limites_brasil
)

#%% Geração do gráfico de emissão agregada pixelada nacional

# Chamar a função para gerar e salvar o gráfico
figura, eixos = plotar_mosaico_emissoes(
    ds=ds_emissoes_completo, #Dataset com os dados
    titulo='Evolução da Emissão de NMVOC da Indústria Alimentícia',
    cbar_label='Emissão de NMVOC (kg)',
    scale='log', # Pode ser 'log' ou 'linear'
    save_path=os.path.join(figpath,'Emissões NMCOV no Brasil Anual Geolocalizada.png')
)

#%%

print("Analisando a tendência geral para todos os estados (isso pode levar um momento)...")
tendencia_uf = analisar_tendencia_nmvc(df, ['ESTADO'])
print("Análise por estado concluída.")

estados_para_analisar = sorted(df['ESTADO'].dropna().unique())

print(f"\nIniciando a geração de mosaicos para {len(estados_para_analisar)} estado(s)...")

for uf in estados_para_analisar:
    print(f"\n--- Gerando mosaico para: {uf} ---")
    
    # Define o caminho de saída para a figura do estado
    save_path = os.path.join(figpath, f'mosaico_emissao_{uf.replace(" ", "_")}.png')
    
    # Chama a função principal que agora faz todo o trabalho
    fig = plotar_mosaico_estado(
        df=df,
        ds=ds_emissoes_completo,
        tendencia_uf_df=tendencia_uf,
        estado_alvo=uf,
        save_path=save_path
    )
    
    # Fecha a figura após salvar para liberar memória e evitar sobreposição de plots
    plt.close(fig)

print("\n\nProcesso finalizado! Todos os mosaicos foram salvos na pasta 'figures'.")
