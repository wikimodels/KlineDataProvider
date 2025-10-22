import pandas as pd

def calculate_rsi(close: pd.Series, length: int = 14) -> pd.Series:
    """
    Рассчитывает Relative Strength Index (RSI).

    Args:
        close (pd.Series): Серия цен закрытия.
        length (int): Период для RSI.

    Returns:
        pd.Series: Серия со значениями RSI.
    """
    delta = close.diff()

    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=length, min_periods=length).mean()
    avg_loss = loss.rolling(window=length, min_periods=length).mean()

    # Сглаживание по методу Уайлдера
    for i in range(length, len(close)):
        avg_gain.iloc[i] = (avg_gain.iloc[i - 1] * (length - 1) + gain.iloc[i]) / length
        avg_loss.iloc[i] = (avg_loss.iloc[i - 1] * (length - 1) + loss.iloc[i]) / length

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi