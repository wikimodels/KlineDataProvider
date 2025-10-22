import pandas as pd
import numpy as np

def calculate_vzo(close: pd.Series, volume: pd.Series, length: int = 14) -> pd.Series:
    """
    Рассчитывает Volume Zone Oscillator (VZO).

    Args:
        close (pd.Series): Серия цен закрытия.
        volume (pd.Series): Серия объемов.
        length (int): Период для EMA.

    Returns:
        pd.Series: Серия со значениями VZO.
    """
    # 1. Определяем направление цены и создаем "направленный объем"
    # np.sign(close.diff()) вернет 1 для роста, -1 для падения, 0 если нет изменений
    directed_volume = volume * np.sign(close.diff())
    
    # 2. Рассчитываем EMA от направленного объема
    ema_directed_volume = directed_volume.ewm(span=length, adjust=False).mean()
    
    # 3. Рассчитываем EMA от общего объема
    ema_volume = volume.ewm(span=length, adjust=False).mean()
    
    # 4. Рассчитываем VZO
    # Предотвращаем деление на ноль
    vzo = 100 * (ema_directed_volume / ema_volume.replace(0, np.nan))
    
    return vzo
