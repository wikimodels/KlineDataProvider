import pandas as pd
# --- ИСПРАВЛЕНИЕ: Используем правильную библиотеку ---
import pandas_ta_classic as ta
import logging
from typing import Dict, Any, List
import numpy as np

def add_indicators(market_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Принимает структуру данных, рассчитывает для каждой монеты технические индикаторы
    и возвращает обогащенную структуру данных.
    """
    if not market_data or not market_data.get('data'):
        logging.warning("Получены пустые данные, расчет индикаторов пропущен.")
        return market_data

    # Проходим по каждой монете в списке
    for coin_data in market_data['data']:
        symbol = coin_data.get('symbol', 'Unknown')
        candles = coin_data.get('data', [])

        if not candles or len(candles) < 50:
            logging.warning(f"Недостаточно данных для {symbol} ({len(candles)} свечей), расчет индикаторов пропущен.")
            continue

        try:
            # 1. Создаем DataFrame
            df = pd.DataFrame(candles)
            
            # В pandas-ta-classic нужно явно указывать колонки
            close = df['closePrice']
            high = df['highPrice']
            low = df['lowPrice']
            
            # 2. Рассчитываем индикаторы, передавая Series
            df['rsi_14'] = ta.rsi(close, length=14)
            
            adx_df = ta.adx(high, low, close, length=14)
            df['adx_14'] = adx_df[f'ADX_14']
            df['di_plus_14'] = adx_df[f'DMP_14']
            df['di_minus_14'] = adx_df[f'DMN_14']

            df['ema_50'] = ta.ema(close, length=50)
            df['ema_100'] = ta.ema(close, length=100)
            df['ema_150'] = ta.ema(close, length=150)
            
            # Округляем значения
            indicator_cols = ['rsi_14', 'adx_14', 'di_plus_14', 'di_minus_14', 'ema_50', 'ema_100', 'ema_150']
            for col in indicator_cols:
                if col in df.columns:
                    df[col] = df[col].round(4)

            # Заменяем NaN на None
            df.replace({np.nan: None}, inplace=True)
            
            # Обновляем исходные данные
            updated_candles = df.to_dict(orient='records')
            coin_data['data'] = updated_candles
            
        except Exception as e:
            logging.error(f"Ошибка при расчете индикаторов для {symbol}: {e}", exc_info=True)
            continue

    return market_data

