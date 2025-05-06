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