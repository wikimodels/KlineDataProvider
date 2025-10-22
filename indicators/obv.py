import pandas as pd
import numpy as np
from .ema import calculate_ema

def calculate_obv(close: pd.Series, volume: pd.Series, ema_length: int = None) -> pd.DataFrame:
    """
    Рассчитывает On-Balance Volume (OBV) и опционально его EMA.

    Args:
        close (pd.Series): Серия цен закрытия.
        volume (pd.Series): Серия объемов.
        ema_length (int, optional): Период для EMA от OBV.

    Returns:
        pd.DataFrame: DataFrame с результатами.
    """
    # 1. Определяем направление движения цены
    price_change_direction = np.sign(close.diff()).fillna(0)

    # 2. Рассчитываем направленный объем
    directional_volume = price_change_direction * volume
    
    # 3. Рассчитываем OBV как кумулятивную сумму
    obv = directional_volume.cumsum()

    # Формируем итоговый DataFrame
    result_df = pd.DataFrame({
        'obv': obv
    })

    # 4. Опционально рассчитываем EMA от OBV
    if ema_length and ema_length > 0:
        obv_ema = calculate_ema(obv, length=ema_length)
        result_df[f'obv_ema'] = obv_ema
        
    return result_df
