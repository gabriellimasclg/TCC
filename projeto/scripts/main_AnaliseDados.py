# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 10:02:47 2025

@author: glima
"""

#%% Importando bibliotecas necessárias

import pandas as pd
import os
from functions_AnaliseDados import plot_producao_empilhada,plot_emissoes_estado_ano, plot_emissoes_estado, plotar_mosaico_estado, plotar_mosaico_emissoes, analisar_tendencia_nmvc, plot_emissao, criar_cubo_emissoes_geograficas
import matplotlib.pyplot as plt
import xarray as xr

plt.rcParams['font.family'] = 'Arial'
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

plot_emissao(df,figpath)

#%% Analisar emissão acumuladas por estado com tendencia

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

#%% Analisar emissão acumuladas por estado com barras empilhadas anuais
'''
NÃO FICA LEGAL POIS - linear são paulo fica mt maior q todos
log ele fica distorcido (2017, primeiro ano, vai parecer maior q os demais,
                         atrapalhando interpretação).

# Analisar emissão por estado (ver quais emitem mais p analisar no TCC)
df_emissoes_uf_ano = (
    df.groupby(['ESTADO','num_ano'])
        [['Emissão NMCOV (ton)',
         'Emissão NMCOV CI_lower (ton)',
         'Emissão NMCOV CI_upper (ton)'
    ]].sum()
)

#df_emissoes_uf_tendencia = df_emissoes_uf.merge(tendênciaUF[['ESTADO','tendência']], on='ESTADO', how='left')

# Todos os estados
plot_emissoes_estado_ano(df_emissoes_uf_ano,figpath)
'''
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

#%% Localização das industrias

import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point
import math
import os

# cria coluna de contagem
df['contagem'] = 1

# soma contagem por tipo e cor
tipos_plot = df.groupby(['tipo_industria_nfr', 'food_color'])['contagem'].sum().reset_index()

# cria geometria dos pontos
geometry = [Point(xy) for xy in zip(df['LONGITUDE'], df['LATITUDE'])]
df_gpd = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

# lê limites dos estados
url_estados = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
estados = gpd.read_file(url_estados)

# parâmetros de subplot
nrows, ncols = 3, 3
plots_por_figura = nrows * ncols

# loop por páginas
for pagina in range(math.ceil(len(tipos_plot) / plots_por_figura)):
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(4*ncols, 4.5*nrows))
    axes = axes.flatten()

    subset = tipos_plot.iloc[pagina*plots_por_figura : (pagina+1)*plots_por_figura]

    for i, row in enumerate(subset.itertuples()):
        comida = row.tipo_industria_nfr
        cor = row.food_color
        contagem = row.contagem

        ax = axes[i]

        # mapa base
        estados.plot(ax=ax, color='white', edgecolor='black')

        # pontos
        df_gpd[df_gpd['tipo_industria_nfr'] == comida].plot(
            ax=ax, color=cor, markersize=30, edgecolor='black'
        )

        # título com contagem
        ax.set_title(f"{comida} [{int(contagem)} und]", fontsize=12)
        ax.axis("off")

    # esconde eixos vazios
    for j in range(len(subset), plots_por_figura):
        axes[j].axis("off")

    plt.tight_layout()
    fig.suptitle('Localização das Indústrias Emissoras de NMCOV', fontsize=22, weight='bold', y=1.03)
    plt.savefig(
        os.path.join(repo_path, 'figures', f'localizacao_subplots_pagina{pagina+1}.png'),
        bbox_inches="tight", dpi=300
    )
    plt.show()
#%% Tabela Produção

col_ano="num_ano"
col_valor="Produção (Ton ou hL)"
col_categoria="tipo_industria_nfr"
col_cor="food_color"


dados_cores = df.groupby([col_categoria, col_cor])

dados = df.groupby([col_ano, col_categoria])[col_valor].sum().reset_index().set_index(col_ano)

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# supondo que seu DataFrame se chama df
# df = pd.read_csv("seu_arquivo.csv") ou já está carregado

# pivot table: linhas = alimentos, colunas = anos, valores = produção
tabela = dados.pivot_table(
    index='tipo_industria_nfr',
    columns='num_ano',
    values='Produção (Ton ou hL)',
    aggfunc='sum'  # caso tenha duplicatas
)

# plotando como heatmap
plt.figure(figsize=(13, 5))
sns.heatmap(
    tabela,
    annot=True,       # mostra os valores dentro das células
    fmt=".0f",        # sem casas decimais
    cmap="Reds",      # esquema de cores
    cbar=True,        # mostra barra de cores
    linewidths=0.6,   # linhas entre células
    linecolor='black',
)



plt.tick_params(
    axis='both',       # Aplica para os eixos x e y
    which='major',     # Apenas para os ticks principais
    direction='in',    # Direção dos ticks (para dentro do gráfico)
    length=0,          # Comprimento dos ticks
    bottom=True,       # Garante que os ticks de baixo estejam ligados
    left=True          # Garante que os ticks da esquerda estejam ligados
)
    
#plt.title("Produção por Ano e Tipo de Indústria")
plt.xlabel("")
plt.ylabel("")
plt.tight_layout()
plt.savefig(os.path.join(figpath,'produção_anual.png'),dpi=300, bbox_inches='tight')
plt.show()


#%% Plot Produção Empilhada


plot_producao_empilhada(
    df,
    col_ano="num_ano",
    col_valor="Produção (Ton ou hL)",
    col_categoria="tipo_industria_nfr",
    col_cor="food_color",
    titulo="Produção Anual de Alimentos",
    figpath=os.path.join(repo_path,'figures','produção_anual_barras.png')
)


#%% EDGAR

#esses dados agrupam food_paper :(

edgar = ['v8.1_FT2022_AP_NMVOC_2017_FOO_PAP_emi.nc',
         'v8.1_FT2022_AP_NMVOC_2018_FOO_PAP_emi.nc',
         'v8.1_FT2022_AP_NMVOC_2019_FOO_PAP_emi.nc',
         'v8.1_FT2022_AP_NMVOC_2020_FOO_PAP_emi.nc',
         'v8.1_FT2022_AP_NMVOC_2021_FOO_PAP_emi.nc',
         'v8.1_FT2022_AP_NMVOC_2022_FOO_PAP_emi.nc'
         ]


ds2017,ds2018,ds2019,ds2020,ds2021,ds2022 = [xr.open_dataset(os.path.join(repo_path,'inputs','Edgar',file)) for file in edgar]

print(ds2022)

#%% tabelinha edgar

edgar_dict = {
    '2017': 9014.98,
    '2018': 9621.9,
    '2019': 10279.21,
    '2020': 8912.8,
    '2021': 8374.95,
    '2022': 8363.45
}

# Exemplo: agregando do seu df
df_agg = df.groupby('num_ano', as_index=False).agg({
    'Emissão NMCOV (ton)': 'sum'
})
df_agg = df_agg[~df_agg['num_ano'].isin([2023, 2024])]

# Criando DataFrame do EDGAR
edgar = pd.DataFrame(list(edgar_dict.items()), columns=["ano", "emi_ton"])
edgar["ano"] = edgar["ano"].astype(int)

# --- Juntar tabelas pelos anos
merged = pd.merge(df_agg, edgar, left_on="num_ano", right_on="ano", how="inner")

# --- Calcular correlação de Pearson
corr = merged['Emissão NMCOV (ton)'].corr(merged['emi_ton'])
print(f"Correlação (Pearson): {corr:.3f}")

# --- Normalizar ambas as séries tomando 2017 como base = 100
df_agg_norm = df_agg.copy()
df_agg_norm["emi_norm"] = df_agg_norm["Emissão NMCOV (ton)"] / df_agg_norm.loc[df_agg_norm['num_ano'] == 2017, 'Emissão NMCOV (ton)'].values[0] * 100

edgar_norm = edgar.copy()
edgar_norm["emi_norm"] = edgar_norm["emi_ton"] / edgar_norm.loc[edgar_norm['ano'] == 2017, 'emi_ton'].values[0] * 100

# --- Plot das duas séries normalizadas
plt.figure(figsize=(10, 4))
plt.plot(df_agg_norm['num_ano'], df_agg_norm['emi_norm'],
         marker='o', linestyle='-', label='Inventário (índice, base 2017=100)')
plt.plot(edgar_norm['ano'], edgar_norm['emi_norm'],
         marker='s', linestyle='--', label='EDGAR (índice, base 2017=100)')
#plt.xlabel('Ano')
plt.ylabel('Índice (2017 = 100)')
plt.grid(True)
plt.legend()
plt.tight_layout()

# --- Adiciona texto com correlação embaixo do gráfico
plt.figtext(0.5, -0.03, f"Correlação de Pearson: {corr:.3f}", 
            ha="center", fontsize=12)

plt.savefig(os.path.join(figpath,'edgar_inventario_+_pearson.png'),
            dpi=300, bbox_inches='tight')
plt.show()

#%% tabela comparação edgar e eu
import pandas as pd
import matplotlib.pyplot as plt

# --- Formatação da tabela para visualização ---
# 1. Copiar o DataFrame para não alterar o original
tabela_formatada = merged.copy()

# 2. Arredondar as colunas de emissão para 1 casa decimal
tabela_formatada['Emissão NMCOV (ton)'] = tabela_formatada['Emissão NMCOV (ton)'].round(1)
tabela_formatada['emi_ton'] = tabela_formatada['emi_ton'].round(1)
tabela_formatada['num_ano'] = tabela_formatada['num_ano'].round(0)
tabela_formatada['ano'] = tabela_formatada['ano'].round(0)

# 3. Renomear as colunas para a tabela final
tabela_formatada = tabela_formatada.rename(columns={
    "num_ano": "Ano",
    "Emissão NMCOV (ton)": "Inventário (ton)",
    "emi_ton": "EDGAR (ton)"
})

tabela_final = tabela_formatada[['Ano', 'Inventário (ton)', 'EDGAR (ton)']]

# --- 1. MUDANÇA DE FONTE ---
# Define a fonte padrão para Arial em todo o gráfico
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial']


# --- 2. FORÇAR FORMATAÇÃO DO TEXTO (CORREÇÃO DO ANO) ---
# Crie uma cópia do dataframe para exibir como texto
tabela_texto = tabela_final.copy()
# Formate cada coluna como string no formato desejado
tabela_texto['Ano'] = tabela_texto['Ano'].astype(int).astype(str)
tabela_texto['Inventário (ton)'] = tabela_texto['Inventário (ton)'].apply(lambda x: f'{x:.1f}')
tabela_texto['EDGAR (ton)'] = tabela_texto['EDGAR (ton)'].apply(lambda x: f'{x:.1f}')


# --- Geração da imagem da tabela ---
fig, ax = plt.subplots(figsize=(6, 2))
ax.axis('off')
ax.axis('tight')

cor_cabecalho = '#E0E0E0'

tabela_plot = ax.table(
    # Use o dataframe com os textos formatados
    cellText=tabela_texto.values,
    colLabels=tabela_texto.columns,
    loc='center',
    cellLoc='center',
    colColours=[cor_cabecalho] * len(tabela_texto.columns)
)

# Aumenta a escala da fonte no plot
tabela_plot.auto_set_font_size(False)
tabela_plot.set_fontsize(11)

cell_height = 0.2 # Defina a altura desejada para cada célula (valor padrão é 0.6)

# Iterar sobre as células da tabela e ajustar a altura
for (row, col), cell in tabela_plot.get_celld().items():
    cell.set_height(cell_height)
    if row == 0:  # Aplica apenas à primeira coluna (índice 0)
        cell.get_text().set_fontweight('bold')    
    
# Salvar a imagem final
plt.savefig(os.path.join(figpath,'tabela_final_correta.png'), dpi=300, bbox_inches='tight')
plt.show()

#%% gráfico do edgar
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

# leitura (já feita por você; manteve para contexto)
edgar = pd.read_csv(os.path.join(repo_path,
                                 'inputs',
                                 'MaterialBaixado',
                                 'EDGAR_dadosemissaoNMVOC_BR.csv'),
                    encoding='latin1',
                    dtype={"Ano": int},
                    index_col="Ano")

# garantir numéricos
for col in edgar.columns:
    edgar[col] = pd.to_numeric(edgar[col], errors="coerce")

# calcular total e percentual
edgar["Total"] = edgar.sum(axis=1)
edgar["% Produção de alimentos"] = (edgar["Produção de alimentos"] / edgar["Total"]) * 100

# colunas a plotar (exclui Total e %)
plot_cols = [c for c in edgar.columns if c not in ["Total", "% Produção de alimentos"]]

# ---- figura com mosaic (A = stacked area, B = percentual)
fig, axes = plt.subplot_mosaic([["A"], ["B"]], figsize=(10, 10), constrained_layout=False)

ax_main = axes["A"]
ax_pct  = axes["B"]

# stacked area no painel A
stack_artist = ax_main.stackplot(edgar.index, edgar[plot_cols].T, labels=plot_cols, alpha=0.8)

# destacar Produção de alimentos com linha por cima
food_col = "Produção de alimentos"
line_food, = ax_main.plot(edgar.index, edgar[food_col], linewidth=2.5, linestyle='-', marker='o', color='red')

ax_main.set_title("Emissões por setor (NMVOC) — Destaque: Produção de alimentos")
ax_main.set_ylabel("Emissões (ton)")
ax_main.set_xlabel("")  # ano ficará no eixo x do painel B também

# >>> força os limites do eixo x
ax_main.set_xlim(edgar.index.min(), edgar.index.max())
ax_pct.set_xlim(edgar.index.min(), edgar.index.max())

# painel B: participação percentual
ax_pct.plot(edgar.index, edgar["% Produção de alimentos"], marker="o", linewidth=2, color='red')
ax_pct.set_title("Participação da Produção de alimentos (%)")
ax_pct.set_ylabel("% do total")
ax_pct.grid(True, linestyle="--", alpha=0.5)

# --- legenda única para a figura (combina áreas + linha de destaque)
# cria entradas da legenda: um patch para cada setor + linha para produção de alimentos
legend_entries = []
legend_labels = []

# usar cores dos stack_artist (PolyCollections) para criar patches
for poly, label in zip(stack_artist, plot_cols):
    # poly.get_facecolor() dá RGBA array; usar Patch para legenda
    legend_entries.append(Patch(facecolor=poly.get_facecolor()[0]))
    legend_labels.append(label)

# garantir que a entrada da linha de destaque apareça (sobrescreve/repete se já estiver)
legend_entries.append(Line2D([0], [0], color=line_food.get_color(), lw=3, marker='o'))
legend_labels.append("Destaque: " + food_col)

# ajustar espaço inferior para a legenda
fig.subplots_adjust(bottom=0.18)  # cria espaço para a legenda abaixo da figura

# legenda do fig inteira
fig.legend(legend_entries, legend_labels, loc="lower center",
           bbox_to_anchor=(0.5, 0.02), ncol=3, frameon=False, title="Setores")

plt.savefig(os.path.join(figpath,'edgar_alimentos.png'), dpi=300, bbox_inches='tight')

plt.show()
