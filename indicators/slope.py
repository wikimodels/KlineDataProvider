import pandas as pd

def calculate_slope(series: pd.Series, period: int, normalize: bool = False) -> pd.Series:
    """
    Рассчитывает наклон (rate of change) для временного ряда.
    Формула аналогична используемой в Pine Script.

    Args:
        series (pd.Series): Временной ряд (например, линия EMA или VWAP).
        period (int): Период, за который рассчитывается изменение.
        normalize (bool): Если True, наклон нормализуется путем деления
                          на скользящее среднее значение самой серии.
                          Это позволяет сравнивать наклоны разных активов.

    Returns:
        pd.Series: Серия со значениями наклона.
    """
    # Рассчитываем абсолютное изменение за период
    change = series.diff(period)
    
    # Базовый (абсолютный) наклон
    slope = change / period
    
    # Если включена нормализация, делим на среднее значение
    if normalize:
        rolling_mean = series.rolling(window=period).mean()
        # Предотвращаем деление на ноль
        slope = slope / rolling_mean.replace(0, pd.NA)

    # Используем множитель для удобства отображения, как в TradingView
    return slope * 10000

