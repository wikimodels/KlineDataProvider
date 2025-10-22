import pandas as pd
import logging
from typing import Dict, Any, List
import numpy as np

# --- НОВЫЕ ИМПОРТЫ ИЗ ВАШЕГО ПАКЕТА 'indicators' ---
try:
    from indicators import (
        calculate_adx,
        calculate_anchored_vwap,
        calculate_atr,
        calculate_bollinger_bands,
        calculate_cmf,
        calculate_ema,
        calculate_highest,
        calculate_lowest,
        calculate_kama,
        calculate_keltner_channel,
        calculate_macd,
        calculate_obv,
        calculate_rsi,
        recognize_patterns,
        calculate_rvwap,
        calculate_slope,
        calculate_vzo,
        calculate_z_score
    )
except ImportError:
    logging.critical("НЕ УДАЛОСЬ ИМПОРТИРОВАТЬ ПАКЕТ 'indicators'. Убедитесь, что __init__.py и все модули индикаторов находятся в правильной папке.")
    # --- ПУСТЫШКИ ---
    def calculate_adx(*args, **kwargs):
        return pd.DataFrame(columns=['adx', 'di_plus', 'di_minus'])
    def calculate_anchored_vwap(*args, **kwargs):
        return pd.DataFrame(columns=['w_avwap', 'w_avwap_upper_band', 'w_avwap_lower_band',
                                     'm_avwap', 'm_avwap_upper_band', 'm_avwap_lower_band'])
    def calculate_atr(*args, **kwargs):
        return pd.Series(dtype='float64')
    def calculate_bollinger_bands(*args, **kwargs):
        return pd.DataFrame(columns=['bb_basis', 'bb_upper', 'bb_lower', 'bb_width'])
    def calculate_cmf(*args, **kwargs):
        return pd.DataFrame(columns=['cmf', 'cmf_ema'])
    def calculate_ema(*args, **kwargs):
        return pd.Series(dtype='float64')
    def calculate_highest(*args, **kwargs):
        return pd.Series(dtype='float64')
    def calculate_lowest(*args, **kwargs):
        return pd.Series(dtype='float64')
    def calculate_kama(*args, **kwargs):
        return (pd.Series(dtype='float64'), pd.Series(dtype='float64'))
    def calculate_keltner_channel(*args, **kwargs):
        return pd.DataFrame(columns=['kc_upper', 'kc_middle', 'kc_lower', 'kc_width'])
    def calculate_macd(*args, **kwargs):
        return pd.DataFrame(columns=['macd', 'macd_signal', 'macd_hist'])
    def calculate_obv(*args, **kwargs):
        ema_length = kwargs.get('ema_length')
        cols = ['obv']
        if ema_length and ema_length > 0:
            cols.append('obv_ema')
        return pd.DataFrame(columns=cols)
    def calculate_rsi(*args, **kwargs):
        return pd.Series(dtype='float64')
    def recognize_patterns(*args, **kwargs):
        return pd.DataFrame(columns=['is_doji', 'is_bullish_engulfing', 'is_bearish_engulfing', 'is_hammer', 'is_pinbar'])
    def calculate_rvwap(*args, **kwargs):
        stdev_mults = kwargs.get('stdev_mults', [1.0, 2.0])
        output_columns = ['rvwap']
        for mult in stdev_mults:
            mult_str = str(mult).replace('.', '_')
            output_columns.extend([
                f'rvwap_upper_band_{mult_str}',
                f'rvwap_lower_band_{mult_str}',
                f'rvwap_width_{mult_str}'
            ])
        return pd.DataFrame(columns=output_columns)
    def calculate_slope(*args, **kwargs):
        return pd.Series(dtype='float64')
    def calculate_vzo(*args, **kwargs):
        return pd.Series(dtype='float64')
    def calculate_z_score(*args, **kwargs):
        columns = kwargs.get('columns', [])
        output_cols = [f'{col}_z_score' for col in columns]
        return pd.DataFrame(columns=output_cols)
    # --- КОНЕЦ ПУСТЫШЕК ---


def add_indicators(market_data: Dict[str, Any]) -> Dict[str, Any]:  # <-- ✅ Исправлено
    """
    Принимает структуру данных, рассчитывает для каждой монеты технические индикаторы
    с использованием кастомного пакета 'indicators' и возвращает обогащенную структуру данных.
    """
    if not market_data or not market_data.get('data'):
        logging.warning("Получены пустые данные, расчет индикаторов пропущен.")
        return market_data

    for coin_data in market_data['data']:
        symbol = coin_data.get('symbol', 'Unknown')
        candles = coin_data.get('data', [])
        timeframe = coin_data.get('timeframe', '1h')

        if not candles or len(candles) < 200:
            logging.warning(f"Недостаточно данных для {symbol} (нужно > 200, получено {len(candles)}), расчет индикаторов пропущен.")
            continue

        try:
            df = pd.DataFrame(candles)

            df['closePrice'] = pd.to_numeric(df['closePrice'])
            df['highPrice'] = pd.to_numeric(df['highPrice'])
            df['lowPrice'] = pd.to_numeric(df['lowPrice'])
            df['openPrice'] = pd.to_numeric(df['openPrice'])  # <-- Добавить эту строку

            volume_key = 'volume'
            if volume_key not in df.columns:
                logging.warning(f"Колонка '{volume_key}' отсутствует для {symbol}. Индикаторы, требующие объем, будут пропущены.")
                df[volume_key] = 0.0
            else:
                df[volume_key] = pd.to_numeric(df[volume_key])

            close = df['closePrice']
            high = df['highPrice']
            low = df['lowPrice']
            volume = df[volume_key]

            has_volume = volume.sum() > 0

            # --- РАСЧЕТ ИНДИКАТОРОВ ---
            adx_df = calculate_adx(high, low, close, length=14)
            df = pd.concat([df, adx_df], axis=1)

            if has_volume:
                avwap_w_df = calculate_anchored_vwap(df, anchor='W', stdev_mult=1.0)
                avwap_m_df = calculate_anchored_vwap(df, anchor='M', stdev_mult=1.0)
                df = pd.concat([df, avwap_w_df, avwap_m_df], axis=1)
            else:
                logging.warning(f"Пропущен AVWAP для {symbol} - нет данных об объеме.")

            df['atr'] = calculate_atr(high, low, close, length=14)

            bb_df = calculate_bollinger_bands(close, length=20, mult=2.0)
            df = pd.concat([df, bb_df], axis=1)

            if has_volume:
                cmf_df = calculate_cmf(high, low, close, volume, length=20, ema_length=10)
                df = pd.concat([df, cmf_df], axis=1)

            df['ema_50'] = calculate_ema(close, length=50)
            df['ema_100'] = calculate_ema(close, length=100)
            df['ema_150'] = calculate_ema(close, length=150)

            # 7. Highest (Возвращает DataFrame)
            highest_df = calculate_highest(high, periods=[50, 100])
            df = pd.concat([df, highest_df], axis=1)

            # 8. Lowest (Возвращает DataFrame)
            lowest_df = calculate_lowest(low, periods=[50, 100])
            df = pd.concat([df, lowest_df], axis=1)

            kama_series, kama_sc = calculate_kama(close, length=10, fast_length=2, slow_length=30)
            df['kama'] = kama_series
            df['kama_sc'] = kama_sc

            kc_df = calculate_keltner_channel(high, low, close, length=20, atr_length=10, multiplier=2.0)
            df = pd.concat([df, kc_df], axis=1)

            macd_df = calculate_macd(close, fast=12, slow=26, signal=9)
            df = pd.concat([df, macd_df], axis=1)

            if has_volume:
                obv_df = calculate_obv(close, volume, ema_length=10)
                df = pd.concat([df, obv_df], axis=1)

            patterns_df = recognize_patterns(df, atr=df['atr'])
            df = pd.concat([df, patterns_df], axis=1)

            df['rsi'] = calculate_rsi(close, length=14)

            if has_volume:
                rvwap_df = calculate_rvwap(df, timeframe=timeframe, stdev_mults=[1.0, 2.0])
                df = pd.concat([df, rvwap_df], axis=1)
            else:
                logging.warning(f"Пропущен RVWAP для {symbol} - нет данных об объеме.")

            df['ema_50_slope'] = calculate_slope(df['ema_50'], period=5)
            df['ema_100_slope'] = calculate_slope(df['ema_100'], period=5)
            df['ema_150_slope'] = calculate_slope(df['ema_150'], period=5)

            if has_volume:
                df['vzo'] = calculate_vzo(close, volume, length=14)

            # --- НОВЫЙ БЛОК: РАСЧЁТ Z-SCORE ---
            z_score_cols = ['closePrice', 'bb_width', 'kc_width']
            z_score_cols.append('rvwap_width_1_0')
            if 'openInterest' in df.columns:
                z_score_cols.append('openInterest')
            if 'fundingRate' in df.columns:
                z_score_cols.append('fundingRate')

            df['ema_proximity'] = (abs(df['ema_50'] - df['ema_100']) +
                                   abs(df['ema_100'] - df['ema_150']) +
                                   abs(df['ema_50'] - df['ema_150'])) / 3
            z_score_cols.append('ema_proximity')

            z_score_df = calculate_z_score(df, columns=z_score_cols, window=50)
            df = pd.concat([df, z_score_df], axis=1)
            # --- КОНЕЦ НОВОГО БЛОКА ---

            # --- Округление и очистка ---
            indicator_cols = [
                'adx', 'di_plus', 'di_minus', 'openPrice',
                'w_avwap', 'w_avwap_upper_band', 'w_avwap_lower_band',
                'm_avwap', 'm_avwap_upper_band', 'm_avwap_lower_band',
                'atr', 'bb_basis', 'bb_upper', 'bb_lower', 'bb_width',
                'cmf', 'cmf_ema', 'ema_50', 'ema_100', 'ema_150', 'highest_50', 'lowest_50', 'highest_100', 'lowest_100', 'kama', 'kama_sc',
                'kc_upper', 'kc_middle', 'kc_lower', 'kc_width',
                'macd', 'macd_signal', 'macd_hist',
                'obv', 'obv_ema',
                'is_doji', 'is_bullish_engulfing', 'is_bearish_engulfing', 'is_hammer', 'is_pinbar',
                'rvwap',
                'rvwap_upper_band_1_0', 'rvwap_lower_band_1_0', 'rvwap_width_1_0',
                'rvwap_upper_band_2_0', 'rvwap_lower_band_2_0', 'rvwap_width_2_0',
                'rsi',
                'ema_50_slope', 'ema_100_slope', 'ema_150_slope',
                'vzo',
                'closePrice_z_score',
                'bb_width_z_score',
                'kc_width_z_score',
                'rvwap_width_1_0_z_score',
                'ema_proximity_z_score',
                'openInterest_z_score',
                'fundingRate_z_score',
            ]

            for col in indicator_cols:
                if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].round(6)

            df.replace([np.nan, np.inf, -np.inf], None, inplace=True)

            for col in ['is_doji', 'is_bullish_engulfing', 'is_bearish_engulfing', 'is_hammer', 'is_pinbar']:
                if col in df.columns:
                    df[col] = df[col].fillna(False)

            updated_candles = df.to_dict(orient='records')
            coin_data['data'] = updated_candles

        except Exception as e:
            logging.error(f"Ошибка при расчете кастомных индикаторов для {symbol}: {e}", exc_info=True)
            continue

    logging.info("Расчет всех индикаторов завершен.")
    return market_data