import pandas as pd
from typing import List

def calculate_z_score(df: pd.DataFrame, columns: List[str], window: int = 50) -> pd.DataFrame:
    """
    Рассчитывает скользящий Z-score для указанных колонок в DataFrame.

    Z-score показывает, на сколько стандартных отклонений значение
    отличается от своего среднего значения в заданном окне.

    Args:
        df (pd.DataFrame): Входной DataFrame с данными.
        columns (List[str]): Список имен колонок, для которых нужно рассчитать Z-score.
        window (int): Размер скользящего окна.

    Returns:
        pd.DataFrame: DataFrame с новыми колонками Z-score.
    """
    result_df = pd.DataFrame(index=df.index)

    for col_name in columns:
        if col_name in df.columns:
            # 1. Рассчитываем скользящее среднее
            rolling_mean = df[col_name].rolling(window=window, min_periods=1).mean()
            
            # 2. Рассчитываем скользящее стандартное отклонение
            rolling_std = df[col_name].rolling(window=window, min_periods=1).std()

            # 3. Рассчитываем Z-score
            # (Текущее значение - Среднее) / Стандартное отклонение
            # Заменяем 0 в знаменателе на NaN, чтобы избежать деления на ноль
            z_score_series = (df[col_name] - rolling_mean) / rolling_std.replace(0, pd.NA)

            new_col_name = f'{col_name}_z_score'
            result_df[new_col_name] = z_score_series
    
    return result_df
