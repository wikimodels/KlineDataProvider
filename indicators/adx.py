import pandas as pd
import numpy as np

def _wilder_smooth(series: pd.Series, length: int) -> pd.Series:
    """
    Применяет сглаживание Уайлдера (RMA - Running Moving Average).
    Это специфический вид EMA с alpha = 1 / length.
    """
    # Создаем пустую серию для результатов, заполненную NaN
    smoothed = pd.Series(np.nan, index=series.index)
    
    # Первое значение - это простое среднее за 'length' периодов
    sma = series.rolling(window=length).mean().iloc[length-1]
    smoothed.iloc[length-1] = sma
    
    # Рассчитываем остальные значения по формуле Уайлдера
    for i in range(length, len(series)):
        smoothed.iloc[i] = (smoothed.iloc[i-1] * (length - 1) + series.iloc[i]) / length
        
    return smoothed


def calculate_adx(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.DataFrame:
    """
    Рассчитывает Average Directional Index (ADX), +DI, и -DI.
    Расчет максимально приближен к реализации в TradingView.

    Args:
        high (pd.Series): Серия максимальных цен.
        low (pd.Series): Серия минимальных цен.
        close (pd.Series): Серия цен закрытия.
        length (int): Период для расчета ADX.

    Returns:
        pd.DataFrame: DataFrame с колонками adx, di_plus и di_minus.
    """
    # 1. Рассчитываем True Range (TR)
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # 2. Рассчитываем Directional Movement (+DM, -DM)
    move_up = high.diff(1)
    move_down = -low.diff(1)

    plus_dm = pd.Series(np.where((move_up > move_down) & (move_up > 0), move_up, 0.0), index=high.index)
    minus_dm = pd.Series(np.where((move_down > move_up) & (move_down > 0), move_down, 0.0), index=low.index)

    # 3. Сглаживаем TR, +DM, -DM с помощью сглаживания Уайлдера
    atr = _wilder_smooth(tr, length)
    plus_dm_smooth = _wilder_smooth(plus_dm, length)
    minus_dm_smooth = _wilder_smooth(minus_dm, length)
    
    # Предотвращаем деление на ноль
    atr = atr.replace(0, np.nan)

    # 4. Рассчитываем Directional Indicators (+DI, -DI)
    plus_di = 100 * (plus_dm_smooth / atr)
    minus_di = 100 * (minus_dm_smooth / atr)

    # 5. Рассчитываем Directional Movement Index (DX)
    di_sum = plus_di + minus_di
    di_sum = di_sum.replace(0, np.nan) # Предотвращаем деление на ноль
    dx = 100 * (abs(plus_di - minus_di) / di_sum)
    
    # 6. Рассчитываем Average Directional Index (ADX) - сглаженный DX
    adx = _wilder_smooth(dx, length)

    # 7. Формируем итоговый DataFrame
    adx_df = pd.DataFrame({
        'adx': adx,
        'di_plus': plus_di,
        'di_minus': minus_di
    })

    return adx_df