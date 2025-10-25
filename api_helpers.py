"""
Этот модуль содержит вспомогательные функции для работы с API бирж.
"""

# --- НОВЫЕ ИМПОРТЫ ---
import asyncio
import aiohttp
from typing import Dict, Any, Tuple

# Импортируем центральные логгеры
# Предполагаем, что logging_setup.py находится в корне проекта
try:
     from logging_setup import logger, oi_fr_error_logger
except ImportError:
     # Фоллбэк, если logging_setup не найден
     import logging
     logger = logging.getLogger(__name__)
     oi_fr_error_logger = logging.getLogger('oi_fr_errors')

# --- Карты для преобразования таймфреймов в формат бирж ---
BINANCE_TIMEFRAME_MAP = {
    '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
    '1h': '1h', '4h': '4h', '8h': '8h', '12h': '12h', '1d': '1d'
}

BYBIT_TIMEFRAME_MAP = {
    '1m': '1', '5m': '5', '15m': '15', '30m': '30',
    '1h': '60', '4h': '240', '8h': '480', '12h': '720', '1d': 'D'
}

# --- Карта для вычисления длительности интервала в миллисекундах ---
TIMEFRAME_MS_MAP = {
    '1m': 60 * 1000,
    '5m': 5 * 60 * 1000,
    '15m': 15 * 60 * 1000,
    '30m': 30 * 60 * 1000,
    '1h': 60 * 60 * 1000,
    '4h': 4 * 60 * 60 * 1000,
    '8h': 8 * 60 * 60 * 1000,
    '12h': 12 * 60 * 60 * 1000,
    '1d': 24 * 60 * 60 * 1000
}

def get_interval_duration_ms(timeframe: str) -> int:
    """
    Возвращает длительность таймфрейма в миллисекундах.
    Эта функция необходима для расчета closeTime.
    """
    return TIMEFRAME_MS_MAP.get(timeframe, 4 * 60 * 60 * 1000) # 4h по умолчанию

# --- Существующие функции ---

def get_binance_interval(timeframe: str) -> str:
    """Возвращает таймфрейм в формате для Binance API."""
    return BINANCE_TIMEFRAME_MAP.get(timeframe, '4h')

def get_bybit_interval(timeframe: str) -> str:
    """Возвращает таймфрейм в формате для Bybit API."""
    return BYBIT_TIMEFRAME_MAP.get(timeframe, '240')


# --- НОВАЯ ФУНКЦИЯ (перенесена из data_collector.py) ---

async def fetch_url(session: aiohttp.ClientSession, task_info: Dict) -> Tuple[str, str, str, Any | None]:
    """
    Асинхронно запрашивает один URL и возвращает результат.
    Возвращает (symbol, data_type, exchange, json_data)
    """
    url = task_info['url']
    symbol = task_info['symbol']
    data_type = task_info['data_type']
    exchange = task_info['exchange']
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if exchange == 'binance':
        headers.update({
            "Origin": "https://www.binance.com",
            "Referer": "https://www.binance.com/"
        })

    try:
        # Логируем начало запроса
        if data_type in ['oi', 'fr']:
            # logger.info(f"FETCH: Запрашиваю {data_type} для {symbol} ({exchange}): {url}") # <-- УДАЛЕН ШУМ
            # oi_fr_error_logger.info(f"Запрашиваю {data_type} для {symbol} ({exchange}): {url}") # <-- УДАЛЕН ШУМ
            pass # Оставляем блок if, чтобы не менять отступы
            
        await asyncio.sleep(0.1) # Задержка
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status != 200:
                 status_msg = f"FETCH: {data_type} для {symbol} ({exchange}) вернул статус {response.status}. URL: {url}"
                 logger.warning(status_msg)
                 oi_fr_error_logger.warning(status_msg)
            response.raise_for_status()
            json_data = await response.json()
            
            if data_type in ['oi', 'fr']:
                # success_msg = f"FETCH: Успешно получен {data_type} для {symbol} ({exchange}), статус {response.status}." # <-- УДАЛЕН ШУМ
                # logger.info(success_msg) # <-- УДАЛЕН ШУМ
                # oi_fr_error_logger.info(success_msg) # <-- УДАЛЕН ШУМ
                pass # Оставляем блок if
                
            return (symbol, data_type, exchange, json_data)
            
    except Exception as e:
        error_msg = f"FETCH: Ошибка при запросе {data_type} для {symbol} ({exchange}): {e}. URL: {url}"
        logger.error(error_msg)
        oi_fr_error_logger.error(error_msg)
        return (symbol, data_type, exchange, None)
