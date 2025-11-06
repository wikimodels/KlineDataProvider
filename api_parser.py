"""
Этот модуль отвечает за ПАРСИНГ сырых ответов API от бирж
и приведение их к ЕДИНОМУ внутреннему формату.

Единый формат:
Klines: [{'openTime': int, 'openPrice': float, 'highPrice': float, 'lowPrice': float, 'closePrice': float, 'volume': float, 'closeTime': int, 'volumeDelta': float (optional)}]
OI:     [{'openTime': int, 'openInterest': float, 'closeTime': int}]
FR:     [{'openTime': int, 'fundingRate': float, 'closeTime': int}]
"""

import logging
from typing import List, Dict, Any, Optional

# --- Используем логгер из родительского пакета ---
try:
    # --- ИЗМЕНЕНИЕ: Убрали oi_fr_error_logger ---
    from .logging_setup import logger
except ImportError:
    # Фоллбэк для standalone запуска
    import logging
    logger = logging.getLogger(__name__)

# --- BINANCE Parsers ---

def parse_binance_klines(raw_data: List[List[Any]], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит Klines (свечи) от Binance.
    Формат Binance: [openTime, open, high, low, close, volume, closeTime, ..., takerBuyBaseAssetVolume (idx 9), ...]
    """
    parsed_klines = []
    try:
        for kline in raw_data:
            # --- ИЗМЕНЕНИЕ: Убедимся, что у нас есть 10 полей (для takerBuy... at index 9) ---
            if len(kline) < 10:
                logger.warning(f"BINANCE_PARSER (klines): Пропущена свеча, неполные данные (меньше 10 полей): {kline}")
                continue
            
            # --- ИЗМЕНЕНИЕ: Считаем volumeDelta ---
            try:
                total_volume = float(kline[5])
                buy_taker_volume = float(kline[9])
                
                # total_volume = buy_taker_volume + sell_taker_volume
                # sell_taker_volume = total_volume - buy_taker_volume
                # volume_delta = buy_taker_volume - sell_taker_volume
                # volume_delta = buy_taker_volume - (total_volume - buy_taker_volume)
                volume_delta = 2 * buy_taker_volume - total_volume
                
            except (ValueError, TypeError):
                # Если данные некорректны (например, '0' или пустая строка), ставим None
                volume_delta = None 
            # --- Конец ИЗМЕНЕНИЯ ---

            parsed_klines.append({
                "openTime": int(kline[0]),
                "openPrice": float(kline[1]),
                "highPrice": float(kline[2]),
                "lowPrice": float(kline[3]),
                "closePrice": float(kline[4]),
                "volume": float(kline[5]),
                "closeTime": int(kline[6]),
                "volumeDelta": volume_delta # <-- 1. ДОБАВЛЯЕМ НОВОЕ ПОЛЕ
            })
        return parsed_klines
    except (ValueError, TypeError, IndexError) as e:
        logger.error(f"BINANCE_PARSER (klines): Ошибка парсинга Klines: {e}. Raw data (sample): {str(raw_data)[:200]}...", exc_info=True)
        return []

def parse_binance_oi(raw_data: List[Dict[str, str]], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит Open Interest (OI) от Binance.
    (Код не изменен)
    """
    parsed_oi = []
    try:
        for item in raw_data:
            open_time = int(item["timestamp"])
            parsed_oi.append({
                "openTime": open_time,
                "openInterest": float(item["sumOpenInterest"]),
                "closeTime": open_time + 1 
            })
        return parsed_oi
    except (ValueError, TypeError, KeyError) as e:
        logger.error(f"BINANCE_PARSER (oi): Ошибка парсинга OI: {e}. Raw data (sample): {str(raw_data)[:200]}...", exc_info=True)
        return []

def parse_binance_fr(raw_data: List[Dict[str, str]], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит Funding Rate (FR) от Binance.
    (Код не изменен)
    """
    parsed_fr = []
    try:
        for item in raw_data:
            open_time = int(item["fundingTime"])
            parsed_fr.append({
                "openTime": open_time,
                "fundingRate": float(item["fundingRate"]),
                "closeTime": open_time + 1
            })
        return parsed_fr
    except (ValueError, TypeError, KeyError) as e:
        logger.error(f"BINANCE_PARSER (fr): Ошибка парсинга FR: {e}. Raw data (sample): {str(raw_data)[:200]}...", exc_info=True)
        return []

# --- BYBIT Parsers ---

def parse_bybit_klines(raw_data: List[List[str]], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит Klines (свечи) от Bybit V5.
    Bybit НЕ предоставляет taker volume, поэтому volumeDelta будет None (kline.get() вернет None).
    (Код не изменен)
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
                "closeTime": open_time + 1,
                # volumeDelta здесь не будет, что нормально
            })
        # Bybit возвращает klines в обратном порядке (от новых к старым)
        return parsed_klines[::-1]
    except (ValueError, TypeError, IndexError) as e:
        logger.error(f"BYBIT_PARSER (klines): Ошибка парсинга Klines: {e}. Raw data (sample): {str(raw_data)[:200]}...", exc_info=True)
        return []

def parse_bybit_oi(raw_data: List[Dict[str, str]], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит Open Interest (OI) от Bybit V5.
    (Код не изменен)
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
        return parsed_oi[::-1]
    except (ValueError, TypeError, KeyError) as e:
        logger.error(f"BYBIT_PARSER (oi): Ошибка парсинга OI: {e}. Raw data (sample): {str(raw_data)[:200]}...", exc_info=True)
        return []

def parse_bybit_fr(raw_data: List[Dict[str, str]], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит Funding Rate (FR) от Bybit V5.
    (Код не изменен)
    """
    parsed_fr = []
    try:
        for item in raw_data:
            open_time = int(item["fundingRateTimestamp"])
            parsed_fr.append({
                "openTime": open_time,
                "fundingRate": float(item["fundingRate"]),
                "closeTime": open_time + 1
            })
        return parsed_fr[::-1]
    except (ValueError, TypeError, KeyError) as e:
        logger.error(f"BYBIT_PARSER (fr): Ошибка парсинга FR: {e}. Raw data (sample): {str(raw_data)[:200]}...", exc_info=True)
        return []