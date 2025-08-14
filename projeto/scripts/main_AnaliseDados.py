# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 10:02:47 2025

@author: glima
"""

import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import pymannkendall as mk
from analiseDados import analisar_tendencia_nmvc

repo_path = os.path.dirname(os.getcwd())

#importar csv com inventário
df = pd.read_csv(os.path.join(repo_path,'inputs','inventarioEmissoesIndustriaisIndustriaAlimenticiaBR.csv'))

# Análise de tendência no BR
tendênciaBR = analisar_tendencia_nmvc(df, ['NFR'])

# Análise de tendência por estado
tendênciaUF = analisar_tendencia_nmvc(df, ['ESTADO'])

#######

# Agrupando os dados por ano e somando as emissões e os intervalos de confiança.
# Esta etapa é importante caso você tenha múltiplas entradas para o mesmo ano.
# Ordenando os dados pelo ano
df_agg = df.groupby('num_ano').sum().reset_index().sort_values('num_ano')


# Criando o gráfico
plt.figure(figsize=(10, 6))

# Plotando a linha da emissão média
plt.plot(df_agg['num_ano'], df_agg['Emissão NMCOV (kg)'], marker='o', linestyle='-', label='Emissão de NMCOV (kg)')

# Preenchendo a área do intervalo de confiança
plt.fill_between(df_agg['num_ano'],
                 df_agg['Emissão NMCOV CI_lower (kg)'],
                 df_agg['Emissão NMCOV CI_upper (kg)'],
                 color='b',
                 alpha=0.2,
                 label='Margem de Erro (Intervalo de Confiança)')

# Adicionando títulos e rótulos
plt.title('Emissão de NMCOV (kg) por Ano')
plt.xlabel('Ano')
plt.ylabel('Emissão de NMCOV (kg)')
plt.grid(True)
plt.legend()
plt.tight_layout()

# Salvando a figura
plt.savefig(os.path.join(repo_path,'figures','TCC','grafico_emissao_nmcov.png'))

def plot_emissao(df, coluna=None):
    """
    Plota gráficos de emissões de NMCOV (kg) por ano com intervalo de confiança.
    
    Parâmetros:
    - df: DataFrame com as colunas:
        'num_ano', 'Emissão NMCOV (kg)', 
        'Emissão NMCOV CI_lower (kg)', 'Emissão NMCOV CI_upper (kg)'
    - coluna: str (opcional) -> coluna para separar gráficos (ex: 'estado')
    
    Exemplos:
    plot_emissao(df)              # Brasil inteiro
    plot_emissao(df, coluna='UF') # Gráfico por estado
    """
    
    def plot_um(df_plot, titulo_extra):
        """Plota um gráfico para um conjunto filtrado."""
        # Agrupa por ano
        df_agg = df_plot.groupby('num_ano', as_index=False).agg({
            'Emissão NMCOV (kg)': 'mean',
            'Emissão NMCOV CI_lower (kg)': 'mean',
            'Emissão NMCOV CI_upper (kg)': 'mean'
        })
        
        plt.figure(figsize=(10, 6))
        plt.plot(df_agg['num_ano'], df_agg['Emissão NMCOV (kg)'], 
                 marker='o', linestyle='-', label='Emissão de NMCOV (kg)')
        plt.fill_between(df_agg['num_ano'],
                         df_agg['Emissão NMCOV CI_lower (kg)'],
                         df_agg['Emissão NMCOV CI_upper (kg)'],
                         color='b', alpha=0.2, label='Margem de Erro (IC)')
        plt.title(f'Emissão de NMCOV (kg) por Ano - {titulo_extra}')
        plt.xlabel('Ano')
        plt.ylabel('Emissão de NMCOV (kg)')
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.show()
    
    if coluna is None:
        # Um único gráfico Brasil
        plot_um(df, "Brasil")
    else:
        # Um gráfico para cada valor único da coluna
        for valor in sorted(df[coluna].dropna().unique()):
            df_filtro = df[df[coluna] == valor]
            plot_um(df_filtro, f"{coluna}: {valor}")


plot_emissao(df, coluna='ESTADO')


# Dicionário de conversão Estado -> UF
mapa_uf = {
    'ACRE': 'AC', 'ALAGOAS': 'AL', 'AMAZONAS': 'AM', 'BAHIA': 'BA',
    'CEARA': 'CE', 'DISTRITO FEDERAL': 'DF', 'ESPIRITO SANTO': 'ES',
    'GOIAS': 'GO', 'MARANHAO': 'MA', 'MATO GROSSO': 'MT',
    'MATO GROSSO DO SUL': 'MS', 'MINAS GERAIS': 'MG', 'PARA': 'PA',
    'PARAIBA': 'PB', 'PARANA': 'PR', 'PERNAMBUCO': 'PE', 'PIAUI': 'PI',
    'RIO DE JANEIRO': 'RJ', 'RIO GRANDE DO NORTE': 'RN',
    'RIO GRANDE DO SUL': 'RS', 'RONDONIA': 'RO', 'SANTA CATARINA': 'SC',
    'SAO PAULO': 'SP', 'SERGIPE': 'SE', 'TOCANTINS': 'TO'
}


import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import numpy as np # Importe numpy para ajudar a gerar as cores

def plot_emissao_multi_rotulo_regiao_inicio(df, coluna='ESTADO', log_y=True):
    plt.figure(figsize=(12, 16))
    ultima_y = []

    # --- ALTERAÇÃO 1: GERAR UMA COR ÚNICA PARA CADA UF ---
    # Pega a lista de UFs únicas no dataframe
    ufs_unicas = sorted(df[coluna].dropna().unique())
    # Usa um colormap do Matplotlib para gerar uma lista de cores distintas
    # 'nipy_spectral' é um bom colormap para muitas categorias diferentes
    cores = plt.cm.get_cmap('nipy_spectral', len(ufs_unicas))
    # Cria um dicionário mapeando cada UF a uma cor
    cores_uf = {uf: cores(i) for i, uf in enumerate(ufs_unicas)}

    for valor in ufs_unicas: # Itera sobre a lista já criada
        df_filtro = df[df[coluna] == valor]
        df_agg = df_filtro.groupby('num_ano', as_index=False).agg({
            'Emissão NMCOV (kg)': 'sum'
        }).sort_values('num_ano')

        # --- ALTERAÇÃO 2: USA O NOVO DICIONÁRIO DE CORES ---
        # A lógica de 'regiao' e 'cores_regiao' foi removida
        cor = cores_uf.get(valor, 'gray') # Pega a cor específica da UF

        plt.plot(df_agg['num_ano'], df_agg['Emissão NMCOV (kg)'],
                 marker='o', linestyle='-', color=cor, alpha=0.8)

        # O resto da lógica para posicionar o texto permanece igual
        if not df_agg.empty:
            x_inicio = df_agg['num_ano'].min()
            y_inicio = df_agg['Emissão NMCOV (kg)'].iloc[0]

            ajuste = 0.05
            while any(abs(y_inicio + ajuste - y) < (y_inicio * 0.05) for y in ultima_y):
                ajuste += y_inicio * 0.05
            ultima_y.append(y_inicio + ajuste)

            x_offset = -0.05
            plt.text(x_inicio + x_offset, y_inicio + ajuste,
                     mapa_uf.get(valor, valor),
                     color=cor, va='center', fontsize=7, ha='right')

    # --- ALTERAÇÃO 3: TÍTULO ATUALIZADO ---
    plt.title('Emissão de NMCOV (kg) por Ano - UF') # Removemos a menção de cores por região
    plt.xlabel('Ano')
    plt.ylabel('Emissão de NMCOV (kg)')

    if log_y:
        plt.yscale('log')

    # --- ALTERAÇÃO 4: LEGENDA REMOVIDA ---
    # As linhas 'handles', 'labels' e 'plt.legend' foram completamente removidas.

    plt.grid(True, which='both', linestyle='--', linewidth=0.7)
    plt.tight_layout()
    plt.show()

# Para usar a função:
plot_emissao_multi_rotulo_regiao_inicio(df, coluna='ESTADO', log_y=True)