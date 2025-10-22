import pandas as pd

def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    """
    Рассчитывает Average True Range (ATR) с использованием сглаживания Уайлдера.
    Формула соответствует стандарту TradingView.

    Args:
        high (pd.Series): Серия максимальных цен.
        low (pd.Series): Серия минимальных цен.
        close (pd.Series): Серия цен закрытия.
        length (int): Период для сглаживания.

    Returns:
        pd.Series: Серия со значениями ATR.
    """
    # 1. Рассчитываем True Range (TR)
    high_low = high - low
    high_close_prev = (high - close.shift(1)).abs()
    low_close_prev = (low - close.shift(1)).abs()
    
    tr = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)
    
    # 2. Рассчитываем ATR, используя сглаживание Уайлдера (RMA)
    # Первый ATR - это простое среднее первых 'length' значений TR
    atr = tr.rolling(window=length).mean()
    
    # Последующие значения сглаживаются по формуле
    for i in range(length, len(tr)):
        atr.iloc[i] = (atr.iloc[i-1] * (length - 1) + tr.iloc[i]) / length
        
    return atr
