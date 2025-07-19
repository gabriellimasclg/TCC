# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 09:09:14 2025

@author: Gabriel
"""

import pandas as pd
import unicodedata

def clean_text(text):
    """
    Normaliza texto: remove acentos, espaços extras e converte para maiúsculas.
    
    Parâmetros:
        text (str): Texto a ser limpo
    
    Retorna:
        str: Texto normalizado ou o valor original se for NA
    """
    if pd.isna(text):
        return text
    text = str(text).strip().upper()
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    return text




'''
#barras empilhadas por estado de porcentagem

import matplotlib.pyplot as plt
import pandas as pd

# 1. Certifique-se de que 'volume_hl' é numérico
df_bebidas['volume_hl'] = pd.to_numeric(df_bebidas['volume_hl'], errors='coerce').fillna(0)

# 2. Agregação por estado e bebida
bebidas_por_estado = df_bebidas.groupby(['Estado', 'bebida'])['volume_hl'].sum().unstack()

bebidas_por_estado = bebidas_por_estado.fillna(0)

# 3. Normaliza os valores para porcentagem por estado (linha)
bebidas_percent = bebidas_por_estado.div(bebidas_por_estado.sum(axis=1), axis=0) * 100

# 4. Criação do gráfico
plt.figure(figsize=(15, 8))

cores = {
    'Vinho': '#8B0000',
    'Cerveja': '#FFD700',
    'Destilado': '#4682B4'
}

bottom = pd.Series([0] * len(bebidas_percent), index=bebidas_percent.index)

for bebida in ['Vinho', 'Cerveja', 'Destilado']:
    plt.bar(
        bebidas_percent.index,
        bebidas_percent[bebida],
        bottom=bottom,
        label=bebida,
        color=cores[bebida]
    )
    bottom += bebidas_percent[bebida]

# Personalização
plt.title('Participação Percentual da Produção de Bebidas por Estado', fontsize=16)
plt.xlabel('Estados', fontsize=12)
plt.ylabel('Percentual (%)', fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.ylim(0, 100)

plt.legend(title='Bebidas', bbox_to_anchor=(1.05, 1))
plt.tight_layout()
plt.show()
'''
