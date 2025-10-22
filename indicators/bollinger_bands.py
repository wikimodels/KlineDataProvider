import pandas as pd

def calculate_bollinger_bands(close: pd.Series, length: int = 20, mult: float = 2.0) -> pd.DataFrame:
    """
    Рассчитывает Полосы Боллинджера (Bollinger Bands) и их ширину (BBW).
    Формулы соответствуют стандарту TradingView.

    Args:
        close (pd.Series): Серия цен закрытия.
        length (int): Период для SMA и стандартного отклонения.
        mult (float): Множитель стандартного отклонения.

    Returns:
        pd.DataFrame: DataFrame с колонками 'bb_basis', 'bb_upper', 
                      'bb_lower', 'bb_width'.
    """
    # 1. Рассчитываем среднюю линию (простое скользящее среднее)
    basis = close.rolling(window=length).mean()
    
    # 2. Рассчитываем стандартное отклонение
    std = close.rolling(window=length).std()
    
    # 3. Рассчитываем верхнюю и нижнюю полосы
    upper = basis + mult * std
    lower = basis - mult * std
    
    # 4. Рассчитываем ширину полос Боллинджера (BBW)
    # (Верхняя - Нижняя) / Средняя
    width = (upper - lower) / basis.replace(0, pd.NA)
    
    result_df = pd.DataFrame({
        'bb_basis': basis,
        'bb_upper': upper,
        'bb_lower': lower,
        'bb_width': width
    })
    
    return result_df
