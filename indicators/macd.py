import pandas as pd
from .ema import calculate_ema

def calculate_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """
    Рассчитывает Moving Average Convergence Divergence (MACD).

    Args:
        close (pd.Series): Серия цен закрытия.
        fast (int): Период быстрой EMA.
        slow (int): Период медленной EMA.
        signal (int): Период сигнальной EMA.

    Returns:
        pd.DataFrame: DataFrame с колонками 'macd', 'macd_signal', 'macd_hist'.
    """
    if close.empty or len(close) < slow:
        return pd.DataFrame(columns=['macd', 'macd_signal', 'macd_hist'])

    # Рассчитываем быструю и медленную EMA, используя нашу функцию
    ema_fast = calculate_ema(close, length=fast)
    ema_slow = calculate_ema(close, length=slow)

    # Рассчитываем линию MACD
    macd_line = ema_fast - ema_slow

    # Рассчитываем сигнальную линию
    signal_line = calculate_ema(macd_line, length=signal)

    # Рассчитываем гистограмму
    histogram = macd_line - signal_line

    # Формируем итоговый DataFrame
    result_df = pd.DataFrame({
        'macd': macd_line,
        'macd_signal': signal_line,
        'macd_hist': histogram
    })

    return result_df
