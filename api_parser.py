"""
Этот модуль отвечает за ПАРСИНГ сырых ответов API от бирж
и приведение их к ЕДИНОМУ внутреннему формату.

Единый формат:
Klines: [{'openTime': int, 'openPrice': float, 'highPrice': float, 'lowPrice': float, 'closePrice': float, 'volume': float, 'closeTime': int}]
OI:     [{'openTime': int, 'openInterest': float, 'closeTime': int}]
FR:     [{'openTime': int, 'fundingRate': float, 'closeTime': int}]
"""

import logging
from typing import List, Dict, Any, Optional

# --- Используем логгер из родительского пакета ---
try:
    from .logging_setup import logger, oi_fr_error_logger
except ImportError:
    # Фоллбэк для standalone запуска
    import logging
    logger = logging.getLogger(__name__)
    oi_fr_error_logger = logging.getLogger('oi_fr_errors')

# --- BINANCE Parsers ---

def parse_binance_klines(raw_data: List[List[Any]], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит Klines (свечи) от Binance.
    Формат Binance: [openTime, open, high, low, close, volume, closeTime, ...]
    """
    parsed_klines = []
    try:
        for kline in raw_data:
            if len(kline) < 7:
                logger.warning(f"BINANCE_PARSER (klines): Пропущена свеча, неполные данные: {kline}")
                continue
            
            parsed_klines.append({
                "openTime": int(kline[0]),
                "openPrice": float(kline[1]),
                "highPrice": float(kline[2]),
                "lowPrice": float(kline[3]),
                "closePrice": float(kline[4]),
                "volume": float(kline[5]),
                "closeTime": int(kline[6]),
            })
        return parsed_klines
    except (ValueError, TypeError, IndexError) as e:
        logger.error(f"BINANCE_PARSER (klines): Ошибка парсинга Klines: {e}. Raw data (sample): {str(raw_data)[:200]}...", exc_info=True)
        return []

def parse_binance_oi(raw_data: List[Dict[str, str]], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит Open Interest (OI) от Binance.
    Формат Binance: [{"symbol": "BTCUSDT", "sumOpenInterest": "10.0", "timestamp": 123...}, ...]
    """
    parsed_oi = []
    try:
        for item in raw_data:
            open_time = int(item["timestamp"])
            parsed_oi.append({
                "openTime": open_time,
                "openInterest": float(item["sumOpenInterest"]),
                # Добавляем closeTime для совместимости (хотя для OI это не так важно)
                "closeTime": open_time + 1 
            })
        return parsed_oi
    except (ValueError, TypeError, KeyError) as e:
        logger.error(f"BINANCE_PARSER (oi): Ошибка парсинга OI: {e}. Raw data (sample): {str(raw_data)[:200]}...", exc_info=True)
        oi_fr_error_logger.error(f"BINANCE_PARSER (oi): Ошибка парсинга OI: {e}.")
        return []

def parse_binance_fr(raw_data: List[Dict[str, str]], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит Funding Rate (FR) от Binance.
    Формат Binance: [{"symbol": "BTCUSDT", "fundingTime": 123..., "fundingRate": "0.0001"}, ...]
    """
    parsed_fr = []
    try:
        for item in raw_data:
            # --- ВАЖНО: Приводим 'fundingTime' к 'openTime' ---
            open_time = int(item["fundingTime"])
            # ------------------------------------------------
            parsed_fr.append({
                "openTime": open_time,
                "fundingRate": float(item["fundingRate"]),
                "closeTime": open_time + 1
            })
        return parsed_fr
    except (ValueError, TypeError, KeyError) as e:
        logger.error(f"BINANCE_PARSER (fr): Ошибка парсинга FR: {e}. Raw data (sample): {str(raw_data)[:200]}...", exc_info=True)
        oi_fr_error_logger.error(f"BINANCE_PARSER (fr): Ошибка парсинга FR: {e}.")
        return []

# --- BYBIT Parsers ---

def parse_bybit_klines(raw_data: List[List[str]], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит Klines (свечи) от Bybit V5.
    Формат Bybit: [openTime, open, high, low, close, volume, turnover]
    """
    parsed_klines = []
    try:
        for kline in raw_data:
            if len(kline) < 6:
                logger.warning(f"BYBIT_PARSER (klines): Пропущена свеча, неполные данные: {kline}")
                continue

            open_time = int(kline[0])
            parsed_klines.append({
                "openTime": open_time,
                "openPrice": float(kline[1]),
                "highPrice": float(kline[2]),
                "lowPrice": float(kline[3]),
                "closePrice": float(kline[4]),
                "volume": float(kline[5]),
                # Bybit не возвращает closeTime, рассчитываем его (хотя klines[0] уже openTime)
                "closeTime": open_time + 1 # Не используется, но для консистентности
            })
        # Bybit возвращает klines в обратном порядке (от новых к старым)
        return parsed_klines[::-1]
    except (ValueError, TypeError, IndexError) as e:
        logger.error(f"BYBIT_PARSER (klines): Ошибка парсинга Klines: {e}. Raw data (sample): {str(raw_data)[:200]}...", exc_info=True)
        return []

def parse_bybit_oi(raw_data: List[Dict[str, str]], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит Open Interest (OI) от Bybit V5.
    Формат Bybit: [{"timestamp": "123...", "openInterest": "10.0"}, ...]
    """
    parsed_oi = []
    try:
        for item in raw_data:
            open_time = int(item["timestamp"])
            parsed_oi.append({
                "openTime": open_time,
                "openInterest": float(item["openInterest"]),
                "closeTime": open_time + 1
            })
        # Bybit возвращает OI в обратном порядке (от новых к старым)
        return parsed_oi[::-1]
    except (ValueError, TypeError, KeyError) as e:
        logger.error(f"BYBIT_PARSER (oi): Ошибка парсинга OI: {e}. Raw data (sample): {str(raw_data)[:200]}...", exc_info=True)
        oi_fr_error_logger.error(f"BYBIT_PARSER (oi): Ошибка парсинга OI: {e}.")
        return []

def parse_bybit_fr(raw_data: List[Dict[str, str]], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит Funding Rate (FR) от Bybit V5.
    Формат Bybit: [{"symbol": "BTCUSDT", "fundingRateTime": "123...", "fundingRate": "0.0001"}, ...]
    """
    parsed_fr = []
    try:
        for item in raw_data:
            # --- ВАЖНО: Приводим 'fundingRateTime' к 'openTime' ---
            open_time = int(item["fundingRateTime"])
            # ----------------------------------------------------
            parsed_fr.append({
                "openTime": open_time,
                "fundingRate": float(item["fundingRate"]),
                "closeTime": open_time + 1
            })
        # Bybit возвращает FR в обратном порядке (от новых к старым)
        return parsed_fr[::-1]
    except (ValueError, TypeError, KeyError) as e:
        logger.error(f"BYBIT_PARSER (fr): Ошибка парсинга FR: {e}. Raw data (sample): {str(raw_data)[:200]}...", exc_info=True)
        oi_fr_error_logger.error(f"BYBIT_PARSER (fr): Ошибка парсинга FR: {e}.")
        return []
