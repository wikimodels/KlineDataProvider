import pandas as pd
from typing import List

def calculate_highest(series: pd.Series, periods: List[int]) -> pd.DataFrame:
    """
    Рассчитывает скользящий максимум (highest high) для временного ряда
    по нескольким периодам.

    Args:
        series (pd.Series): Временной ряд (например, df['highPrice']).
        periods (List[int]): Список периодов для расчета (например, [50, 100]).

    Returns:
        pd.DataFrame: DataFrame с колонками вида 'highest_50', 'highest_100'.
    """
    result_df = pd.DataFrame(index=series.index)
    for period in periods:
        result_df[f'highest_{period}'] = series.rolling(window=period).max()
    return result_df

def calculate_lowest(series: pd.Series, periods: List[int]) -> pd.DataFrame:
    """
    Рассчитывает скользящий минимум (lowest low) для временного ряда
    по нескольким периодам.

    Args:
        series (pd.Series): Временной ряд (например, df['lowPrice']).
        periods (List[int]): Список периодов для расчета (например, [50, 100]).

    Returns:
        pd.DataFrame: DataFrame с колонками вида 'lowest_50', 'lowest_100'.
    """
    result_df = pd.DataFrame(index=series.index)
    for period in periods:
        result_df[f'lowest_{period}'] = series.rolling(window=period).min()
    return result_df
