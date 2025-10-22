import pandas as pd
import numpy as np
from typing import Dict, List

# Карта для перевода таймфреймов в миллисекунды.
# Взято из api_helpers.py для изоляции модуля.
TIMEFRAME_MS_MAP: Dict[str, int] = {
    '1m': 60000, '5m': 300000, '15m': 900000, '30m': 1800000,
    '1h': 3600000, '4h': 14400000, '8h': 28800000, '12h': 43200000, '1d': 86400000
}

def _get_time_window_str(timeframe: str) -> str:
    """
    Определяет размер временного окна в формате для pandas.rolling()
    на основе таймфрейма свечей. Логика 1-в-1 как в timeStep() из Pine Script.
    """
    tf_in_ms = TIMEFRAME_MS_MAP.get(timeframe, 0)
    
    MS_IN_MIN = 60000
    MS_IN_HOUR = 3600000
    MS_IN_DAY = 86400000

    if tf_in_ms <= MS_IN_MIN:     # <= 1m
        return '1H'
    elif tf_in_ms <= MS_IN_MIN * 5: # <= 5m
        return '4H'
    elif tf_in_ms <= MS_IN_HOUR:    # <= 1h
        return '1D'
    elif tf_in_ms <= MS_IN_HOUR * 4:  # <= 4h
        return '3D'
    elif tf_in_ms <= MS_IN_HOUR * 12: # <= 12h
        return '7D'
    elif tf_in_ms <= MS_IN_DAY:     # <= 1d
        return '30D'
    else:                           # > 1d (на всякий случай)
        return '90D'

def calculate_rvwap(df: pd.DataFrame, timeframe: str, stdev_mults: List[float] = [1.0, 2.0]) -> pd.DataFrame:
    """
    Рассчитывает Rolling VWAP и его полосы стандартного отклонения для нескольких множителей.
    Логика полностью повторяет индикатор из TradingView.

    Args:
        df (pd.DataFrame): DataFrame с колонками 'openTime', 'highPrice', 
                           'lowPrice', 'closePrice', 'quoteVolume'.
        timeframe (str): Текущий таймфрейм ('1h', '4h', etc.).
        stdev_mults (List[float]): Список множителей для полос стандартного отклонения.

    Returns:
        pd.DataFrame: DataFrame с рассчитанными колонками RVWAP и его полос.
    """
    # Формируем список колонок для случая, если расчет невозможен
    output_columns = ['rvwap']
    for mult in stdev_mults:
        mult_str = str(mult).replace('.', '_')
        output_columns.extend([
            f'rvwap_upper_band_{mult_str}', 
            f'rvwap_lower_band_{mult_str}', 
            f'rvwap_width_{mult_str}'
        ])

    if df.empty or 'quoteVolume' not in df.columns or df['quoteVolume'].sum() == 0:
        return pd.DataFrame(columns=output_columns)

    # Создаем копию, чтобы не изменять оригинальный DataFrame
    df_copy = df.copy()

    # 1. Подготовка данных
    df_copy['openTime'] = pd.to_datetime(df_copy['openTime'], unit='ms')
    df_copy.set_index('openTime', inplace=True)
    
    src = (df_copy['highPrice'] + df_copy['lowPrice'] + df_copy['closePrice']) / 3
    volume = df_copy['quoteVolume']

    # 2. Вычисляем компоненты для RVWAP и StDev
    src_volume = src * volume
    src_src_volume = volume * (src ** 2)
    
    # 3. Определяем окно и выполняем rolling расчеты
    window = _get_time_window_str(timeframe)
    
    sum_src_vol = src_volume.rolling(window).sum()
    sum_vol = volume.rolling(window).sum()
    sum_src_src_vol = src_src_volume.rolling(window).sum()

    # 4. Рассчитываем RVWAP
    rvwap = sum_src_vol / sum_vol.replace(0, np.nan)

    # 5. Рассчитываем стандартное отклонение
    variance = (sum_src_src_vol / sum_vol.replace(0, np.nan)) - (rvwap ** 2)
    variance = variance.clip(lower=0)
    stdev = np.sqrt(variance)

    # 6. Формируем итоговый DataFrame
    result_df = pd.DataFrame(index=df_copy.index)
    result_df['rvwap'] = rvwap

    # 7. Рассчитываем полосы и ширину для каждого множителя
    for mult in stdev_mults:
        upper_band = rvwap + stdev * mult
        lower_band = rvwap - stdev * mult
        width = (upper_band - lower_band) / rvwap.replace(0, np.nan)
        
        mult_str = str(mult).replace('.', '_')
        result_df[f'rvwap_upper_band_{mult_str}'] = upper_band
        result_df[f'rvwap_lower_band_{mult_str}'] = lower_band
        result_df[f'rvwap_width_{mult_str}'] = width

    # Возвращаем оригинальный индекс для совместимости
    return result_df.reset_index(drop=True)

