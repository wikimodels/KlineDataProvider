"""
Инструменты Технического Анализа (Indicators)
=============================================

Этот пакет собирает различные функции для расчета технических индикаторов.

Каждая основная функция расчета импортируется сюда для
удобства доступа.

Пример использования:
-------------------
import pandas as pd
from indicators import calculate_rsi, calculate_macd

# ... (загрузка данных 'df' с колонкой 'close')
rsi = calculate_rsi(df['close'], length=14)
macd_df = calculate_macd(df['close'])
"""

# Импорт основных функций расчета из каждого модуля
from .adx import calculate_adx
from .anchored_vwap import calculate_anchored_vwap
from .atr import calculate_atr
from .bollinger_bands import calculate_bollinger_bands
from .cmf import calculate_cmf
from .ema import calculate_ema
from .highest_lowest import calculate_highest
from .highest_lowest import calculate_lowest
from .kama import calculate_kama
from .keltner import calculate_keltner_channel
from .macd import calculate_macd
from .obv import calculate_obv
from .rsi import calculate_rsi
from .patterns import recognize_patterns
from .rvwap import calculate_rvwap
from .slope import calculate_slope
from .vzo import calculate_vzo
from .z_score import calculate_z_score

# Определяем __all__, чтобы указать, что является "публичным" API этого пакета.
# Это контролирует, что импортируется при `from indicators import *`
# Включает ВСЕ 18 импортированных элементов.
__all__ = [
    'calculate_adx',
    'calculate_anchored_vwap',
    'calculate_atr',
    'calculate_bollinger_bands',
    'calculate_cmf',
    'calculate_ema',
    'calculate_highest',
    'calculate_lowest',
    'calculate_kama',
    'calculate_keltner_channel',
    'calculate_macd',
    'calculate_obv',
    'calculate_rsi',
    'recognize_patterns',
    'calculate_rvwap',
    'calculate_slope',
    'calculate_vzo',
    'calculate_z_score'
]
