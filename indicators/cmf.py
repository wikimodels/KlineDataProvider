import pandas as pd
import numpy as np
from .ema import calculate_ema

def calculate_cmf(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, length: int = 20, ema_length: int = None) -> pd.DataFrame:
    """
    Рассчитывает Chaikin Money Flow (CMF) и опционально его EMA.

    Args:
        high (pd.Series): Серия максимальных цен.
        low (pd.Series): Серия минимальных цен.
        close (pd.Series): Серия цен закрытия.
        volume (pd.Series): Серия объемов.
        length (int): Период для расчета CMF.
        ema_length (int, optional): Период для EMA от CMF.

    Returns:
        pd.DataFrame: DataFrame с результатами.
    """
    # 1. Рассчитываем Money Flow Multiplier
    # Предотвращаем деление на ноль, если high == low
    h_minus_l = high - low
    h_minus_l = h_minus_l.replace(0, np.nan)
    mfm = ((close - low) - (high - close)) / h_minus_l
    mfm = mfm.fillna(0) # Заполняем NaN, если они были, нулем

    # 2. Рассчитываем Money Flow Volume
    mfv = mfm * volume

    # 3. Рассчитываем CMF
    cmf_numerator = mfv.rolling(window=length).sum()
    cmf_denominator = volume.rolling(window=length).sum()
    
    # Снова предотвращаем деление на ноль
    cmf_denominator = cmf_denominator.replace(0, np.nan)
    cmf = cmf_numerator / cmf_denominator

    # Формируем итоговый DataFrame
    result_df = pd.DataFrame({
        f'cmf': cmf
    })

    # 4. Опционально рассчитываем EMA от CMF
    if ema_length and ema_length > 0:
        cmf_ema = calculate_ema(cmf, length=ema_length)
        result_df[f'cmf_ema'] = cmf_ema
        
    return result_df
