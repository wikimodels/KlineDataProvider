import pandas as pd
import numpy as np
from typing import Tuple

def calculate_kama(close: pd.Series, length: int = 14, fast_length: int = 2, slow_length: int = 30) -> Tuple[pd.Series, pd.Series]:
    """
    Рассчитывает Адаптивную Скользящую Среднюю Кауфмана (KAMA) и её
    коэффициент адаптации (smoothing constant).

    Args:
        close (pd.Series): Серия цен закрытия.
        length (int): Период для расчета коэффициента эффективности.
        fast_length (int): Период для быстрой EMA.
        slow_length (int): Период для медленной EMA.

    Returns:
        Tuple[pd.Series, pd.Series]: Кортеж, содержащий:
            - Серию со значениями KAMA.
            - Серию со значениями smoothing constant (скорости адаптации).
    """
    if close.empty or len(close) < length:
        empty_series = pd.Series(index=close.index, dtype=float)
        return empty_series, empty_series

    # 1. Рассчитываем направление (Momentum) и волатильность
    direction = abs(close.diff(length))
    volatility = abs(close.diff()).rolling(window=length).sum()

    # 2. Рассчитываем Коэффициент Эффективности (Efficiency Ratio - ER)
    er = (direction / volatility.replace(0, np.nan)).fillna(0)

    # 3. Рассчитываем сглаживающие константы
    fast_alpha = 2 / (fast_length + 1)
    slow_alpha = 2 / (slow_length + 1)

    # 4. Рассчитываем динамический альфа (Smoothing Constant)
    smoothing_constant = (er * (fast_alpha - slow_alpha) + slow_alpha) ** 2

    # 5. Итеративно рассчитываем KAMA
    kama_values = np.zeros(len(close))
    kama_values[0] = close.iloc[0]

    for i in range(1, len(close)):
        sc = smoothing_constant.iloc[i]
        if np.isnan(sc):
            kama_values[i] = kama_values[i-1]
        else:
            kama_values[i] = kama_values[i-1] + sc * (close.iloc[i] - kama_values[i-1])
            
    kama_series = pd.Series(kama_values, index=close.index)

    return kama_series, smoothing_constant

