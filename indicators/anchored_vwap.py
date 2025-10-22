import pandas as pd
import numpy as np

def calculate_anchored_vwap(df: pd.DataFrame, anchor: str, stdev_mult: float = 1.0) -> pd.DataFrame:
    """
    Рассчитывает Anchored VWAP с привязкой к началу недели или месяца.

    Args:
        df (pd.DataFrame): DataFrame с колонками 'openTime', 'highPrice', 
                           'lowPrice', 'closePrice', 'volume'.
        anchor (str): Период привязки. 'W' для недели, 'M' для месяца.
        stdev_mult (float): Множитель для полос стандартного отклонения.

    Returns:
        pd.DataFrame: DataFrame с рассчитанными значениями.
    """
    if df.empty or 'volume' not in df.columns or df['volume'].sum() == 0:
        prefix = 'w' if anchor == 'W' else 'm'
        return pd.DataFrame(columns=[
            f'{prefix}_avwap', 
            f'{prefix}_avwap_upper_band', 
            f'{prefix}_avwap_lower_band'
        ])

    df_copy = df.copy()

    # 1. Подготовка данных: преобразуем timestamp в datetime для группировки
    df_copy['timestamp'] = pd.to_datetime(df_copy['openTime'], unit='ms')
    df_copy.set_index('timestamp', inplace=True)
    
    # 2. Расчет базовых величин для каждой свечи
    src = (df_copy['highPrice'] + df_copy['lowPrice'] + df_copy['closePrice']) / 3
    vol = df_copy['volume']
    
    df_copy['src_vol'] = src * vol
    df_copy['src_src_vol'] = (src ** 2) * vol
    df_copy['vol'] = vol

    # 3. Определяем частоту для группировки
    # 'W-MON' - неделя, начинающаяся в понедельник. 'MS' - начало месяца.
    freq = 'W-MON' if anchor == 'W' else 'MS'
    
    # 4. Группируем и применяем кумулятивные расчеты внутри каждой группы
    # .transform() - эффективный способ выполнить расчеты для групп и вернуть 
    # результат в виде Series, выровненной по исходному индексу.
    grouper = pd.Grouper(freq=freq)
    cum_src_vol = df_copy.groupby(grouper)['src_vol'].transform('cumsum')
    cum_vol = df_copy.groupby(grouper)['vol'].transform('cumsum')
    cum_src_src_vol = df_copy.groupby(grouper)['src_src_vol'].transform('cumsum')
    
    # 5. Рассчитываем VWAP и стандартное отклонение
    vwap = cum_src_vol / cum_vol.replace(0, np.nan)
    variance = (cum_src_src_vol / cum_vol.replace(0, np.nan)) - (vwap ** 2)
    variance = variance.clip(lower=0) # Дисперсия не может быть отрицательной
    stdev = np.sqrt(variance)

    # 6. Рассчитываем полосы
    upper_band = vwap + stdev * stdev_mult
    lower_band = vwap - stdev * stdev_mult

    # 7. Формируем итоговый DataFrame с правильными именами колонок
    prefix = 'w' if anchor == 'W' else 'm'
    result_df = pd.DataFrame({
        f'{prefix}_avwap': vwap,
        f'{prefix}_avwap_upper_band': upper_band,
        f'{prefix}_avwap_lower_band': lower_band
    })

    return result_df.reset_index(drop=True)
