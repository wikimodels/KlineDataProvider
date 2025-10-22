import pandas as pd
import numpy as np


def _wilder_smooth(series: pd.Series, length: int) -> pd.Series:
    """
    Применяет сглаживание Уайлдера (RMA - Running Moving Average).
    Это специфический вид EMA с alpha = 1 / length.
    Скопировано из adx.py для консистентности расчета ATR.
    """
    # Создаем пустую серию для результатов, заполненную NaN
    smoothed = pd.Series(np.nan, index=series.index)
   
    # Первое значение - это простое среднее за 'length' периодов
    # Убедимся, что у нас достаточно данных для SMA
    if len(series) < length:
        return smoothed # Возвращаем NaN, если данных не хватает

    sma = series.rolling(window=length).mean().iloc[length-1]
    smoothed.iloc[length-1] = sma
   
    # Рассчитываем остальные значения по формуле Уайлдера
    for i in range(length, len(series)):
        smoothed.iloc[i] = (smoothed.iloc[i-1] * (length - 1) + series.iloc[i]) / length
       
    return smoothed


def calculate_keltner_channel(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    length: int = 20,
    multiplier: float = 2.0,
    use_exponential: bool = True,
    bands_style: str = "ATR",
    atr_length: int = 10
) -> pd.DataFrame:
    """
    Рассчитывает Keltner Channel (KC), основываясь на логике из TradingView.

    Args:
        high (pd.Series): Серия максимальных цен.
        low (pd.Series): Серия минимальных цен.
        close (pd.Series): Серия цен закрытия (Источник, 'src').
        length (int): Период для расчета MA.
        multiplier (float): Множитель для границ.
        use_exponential (bool): Использовать EMA (True) или SMA (False) для средней линии.
        bands_style (str): Стиль расчета границ. Опции: "ATR", "TR", "Range".
        atr_length (int): Отдельный период для ATR (если bands_style="ATR").

    Returns:
        pd.DataFrame: DataFrame с колонками kc_upper, kc_middle, kc_lower и kc_width.
    """
   
    # 1. Рассчитываем среднюю линию (MA)
    if use_exponential:
        middle_line = close.ewm(span=length, adjust=False).mean()
    else:
        middle_line = close.rolling(window=length).mean()

    # 2. Рассчитываем компонент для границ (rangema)
   
    # Рассчитываем True Range (TR) - он нужен для "ATR" и "TR"
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    if bands_style == "ATR":
        # "Average True Range" ? ta.atr(atrlength)
        # ta.atr() в TradingView - это сглаживание Уайлдера
        range_ma = _wilder_smooth(tr, atr_length)
    elif bands_style == "TR":
        # "True Range" ? ta.tr(true)
        range_ma = tr
    elif bands_style == "Range":
        # "Range" ? ta.rma(high - low, length)
        # ta.rma() - это сглаживание Уайлдера
        range_hl = high - low
        range_ma = _wilder_smooth(range_hl, length)
    else:
        # По умолчанию используем ATR, как наиболее стандартный
        range_ma = _wilder_smooth(tr, atr_length)

    # 3. Рассчитываем границы канала
    upper_line = middle_line + (range_ma * multiplier)
    lower_line = middle_line - (range_ma * multiplier)

    # 4. Рассчитываем ширину канала (аналогично BBandWidth)
    # Избегаем деления на ноль: если middle_line == 0, то kc_width = NaN
    denominator = middle_line.replace(0, np.nan)  # Заменяем 0 на NaN, чтобы избежать деления на 0
    kc_width = (upper_line - lower_line) / denominator

    # 5. Формируем итоговый DataFrame
    kc_df = pd.DataFrame({
        'kc_upper': upper_line,
        'kc_middle': middle_line,
        'kc_lower': lower_line,
        'kc_width': kc_width
    })

    return kc_df