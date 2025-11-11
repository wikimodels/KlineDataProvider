import asyncio
import aiohttp
from typing import Dict, Any, Tuple, Optional, List
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# --- ИЗМЕНЕНИЕ: Убираем oi_fr_error_logger ---
try:
    from .logging_setup import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# (Константы CONCURRENCY_LIMIT, REQUEST_TIMEOUT, REQUEST_HEADERS не изменились)
CONCURRENCY_LIMIT = 10
REQUEST_TIMEOUT = 15
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

async def fetch_simple(session: aiohttp.ClientSession, task_info: Dict[str, Any], semaphore: asyncio.Semaphore) -> Tuple[Dict[str, Any], Optional[List[Dict]]]:
    """
    (Код не изменен, кроме вызова логгера)
    """
    # ... (получение url, symbol, data_type ...)
    url = task_info.get("url")
    symbol = task_info.get("symbol", "N/A")
    data_type = task_info.get("data_type", "N/A")
    exchange = task_info.get("exchange", "N/A")
    log_prefix = f"[{task_info.get('original_timeframe', '?').upper()}]"
    
    if not url:
        logger.error(f"{log_prefix} FETCH_SIMPLE: Отсутствует URL в task_info для {symbol} ({data_type}, {exchange})")
        return task_info, None

    async with semaphore:
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS) as response:
                if response.status == 200:
                    data = await response.json()
                    return task_info, data
                elif response.status == 429:
                    logger.warning(f"{log_prefix} FETCH_SIMPLE: Получен 429 Too Many Requests для {url}. Повторная попытка не реализована.")
                    return task_info, None
                else:
                    response_text = await response.text()
                    msg = f"FETCH_SIMPLE: {data_type} для {symbol} ({exchange}) вернул статус {response.status}. Ответ: {response_text[:150]}. URL: {url}"
                    # --- ИЗМЕНЕНИЕ: Используем logger.warning ---
                    if data_type in ['oi', 'fr']:
                        logger.warning(f"{log_prefix} {msg}") # Логируем как warning
                    else:
                        logger.warning(f"{log_prefix} {msg}")
                    return task_info, None

        except asyncio.TimeoutError:
            msg = f"FETCH_SIMPLE: Таймаут запроса {data_type} для {symbol} ({exchange}). URL: {url}"
            # --- ИЗМ