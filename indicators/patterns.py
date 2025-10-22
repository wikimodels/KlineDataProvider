import pandas as pd

def recognize_patterns(df: pd.DataFrame, atr: pd.Series) -> pd.DataFrame:
    """
    Распознает базовые свечные паттерны Price Action.

    Args:
        df (pd.DataFrame): DataFrame с колонками 'openPrice', 'highPrice', 
                           'lowPrice', 'closePrice'.
        atr (pd.Series): Серия значений ATR для определения относительных размеров.

    Returns:
        pd.DataFrame: DataFrame с новыми булевыми колонками для каждого паттерна.
    """
    if df.empty:
        return pd.DataFrame()

    open_price = df['openPrice']
    high = df['highPrice']
    low = df['lowPrice']
    close = df['closePrice']

    # --- Вспомогательные расчеты ---
    body_abs = (close - open_price).abs()
    range = high - low
    
    # Предотвращаем деление на ноль, где range = 0
    range_safe = range.replace(0, 0.000001)

    upper_shadow = high - pd.concat([open_price, close], axis=1).max(axis=1)
    lower_shadow = pd.concat([open_price, close], axis=1).min(axis=1) - low

    # --- Логика распознавания паттернов ---

    # 1. Doji: Тело очень маленькое по сравнению со всем диапазоном
    is_doji = body_abs < (range * 0.1)

    # 2. Bullish Engulfing: Бычье поглощение
    prev_is_red = (open_price.shift(1) > close.shift(1))
    curr_is_green = (close > open_price)
    engulfs_body = (close > open_price.shift(1)) & (open_price < close.shift(1))
    is_bullish_engulfing = prev_is_red & curr_is_green & engulfs_body

    # 3. Bearish Engulfing: Медвежье поглощение
    prev_is_green = (close.shift(1) > open_price.shift(1))
    curr_is_red = (open_price > close)
    engulfs_body_bearish = (open_price > close.shift(1)) & (close < open_price.shift(1))
    is_bearish_engulfing = prev_is_green & curr_is_red & engulfs_body_bearish

    # 4. Hammer: Молот (длинная нижняя тень, маленькое тело наверху)
    is_hammer = (lower_shadow > body_abs * 2) & (upper_shadow < body_abs) & (body_abs > atr * 0.1)

    # 5. Pin Bar (Shooting Star): Пин-бар (длинная верхняя тень, маленькое тело внизу)
    is_pinbar = (upper_shadow > body_abs * 2) & (lower_shadow < body_abs) & (body_abs > atr * 0.1)

    # --- Формирование результата ---
    patterns_df = pd.DataFrame(index=df.index)
    patterns_df['is_doji'] = is_doji
    patterns_df['is_bullish_engulfing'] = is_bullish_engulfing
    patterns_df['is_bearish_engulfing'] = is_bearish_engulfing
    patterns_df['is_hammer'] = is_hammer
    patterns_df['is_pinbar'] = is_pinbar
    
    return patterns_df
