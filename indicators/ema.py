import pandas as pd
import numpy as np

def calculate_ema(close: pd.Series, length: int) -> pd.Series:
    """
    Рассчитывает экспоненциальное скользящее среднее (EMA) по классической формуле.

    Args:
        close (pd.Series): Серия цен закрытия.
        length (int): Период для EMA.

    Returns:
        pd.Series: Серия со значениями EMA.
    """
    # Создаем пустую серию для результатов
    ema = pd.Series(np.nan, index=close.index)
    
    # Первое значение EMA равно SMA за тот же период
    sma = close.rolling(window=length, min_periods=length).mean().iloc[length-1]
    ema.iloc[length-1] = sma

    # Множитель сглаживания
    k = 2 / (length + 1)

    # Рассчитываем остальные значения
    for i in range(length, len(close)):
        ema.iloc[i] = (close.iloc[i] * k) + (ema.iloc[i-1] * (1 - k))
        
    return ema
