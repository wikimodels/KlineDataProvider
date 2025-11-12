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
    Выполняет простой GET-запрос (для Binance).
    """
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
            # --- ИСПРАВЛЕНИЕ: Добавляем корректное логирование ---
            logger.warning(f"{log_prefix} {msg}")
            return task_info, None
            
        except aiohttp.ClientConnectorError as e:
            msg = f"FETCH_SIMPLE: Ошибка соединения {data_type} для {symbol} ({exchange}): {e}. URL: {url}"
            logger.error(f"{log_prefix} {msg}")
            return task_info, None
        
        except Exception as e:
             msg = f"FETCH_SIMPLE: Непредвиденная ошибка {data_type} для {symbol} ({exchange}): {e}. URL: {url}"
             logger.error(f"{log_prefix} {msg}", exc_info=True)
             return task_info, None
            

async def fetch_bybit_paginated(session: aiohttp.ClientSession, task_info: Dict[str, Any], semaphore: asyncio.Semaphore) -> Tuple[Dict[str, Any], Optional[List[Dict]]]:
    """
    Выполняет запросы к Bybit с учетом пагинации (для получения 2-х страниц, если нужно).
    (Код не изменен)
    """
    url = task_info.get("url")
    symbol = task_info.get("symbol", "N/A")
    data_type = task_info.get("data_type", "N/A")
    log_prefix = f"[{task_info.get('original_timeframe', '?').upper()}]"
    
    if not url:
        logger.error(f"{log_prefix} FETCH_BYBIT_PAGINATED: Отсутствует URL в task_info для {symbol} ({data_type})")
        return task_info, None

    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    
    # Bybit API limit
    limit = int(query_params.get('limit', ['1000'])[0])
    
    # 1. Запрос первой страницы
    async with semaphore:
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS) as response:
                if response.status != 200:
                    response_text = await response.text()
                    msg = f"FETCH_BYBIT_PAGINATED: {data_type} для {symbol} вернул статус {response.status}. Ответ: {response_text[:150]}. URL: {url}"
                    if data_type in ['oi', 'fr']:
                        logger.warning(f"{log_prefix} {msg}")
                    else:
                         logger.warning(f"{log_prefix} {msg}")
                    return task_info, None

                data = await response.json()
                result_list = data.get('result', {}).get('list', [])

                if len(result_list) < limit:
                    # 1 страница достаточна
                    return task_info, result_list
                
                # 2. Если требуется пагинация
                
                # Находим `endTime` последней свечи (которая является самой старой, т.к. Bybit возвращает от новых к старым)
                cursor = result_list[-1].get('startTime')
                
                # Проверяем, что cursor существует и что это не OI/FR (OI/FR не поддерживают пагинацию по `startTime`)
                if not cursor or data_type in ['oi', 'fr']:
                    return task_info, result_list
                
                # 3. Запрос второй страницы (с пагинацией)
                query_params['endTime'] = [str(cursor)]
                next_url = urlunparse(parsed_url._replace(query=urlencode(query_params, doseq=True)))

                async with session.get(next_url, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS) as next_response:
                    if next_response.status != 200:
                        logger.warning(f"{log_prefix} FETCH_BYBIT_PAGINATED: Не удалось получить вторую страницу {data_type} для {symbol}. Статус: {next_response.status}")
                        return task_info, result_list
                        
                    next_data = await next_response.json()
                    next_result_list = next_data.get('result', {}).get('list', [])
                    
                    # Объединяем результаты (уникальные, так как start/end time могут пересекаться)
                    # Используем множество openTime для дедупликации
                    unique_timestamps = {item[0] for item in result_list if isinstance(item, list) and len(item) > 0}
                    
                    for item in next_result_list:
                         if isinstance(item, list) and len(item) > 0 and item[0] not in unique_timestamps:
                            result_list.append(item)
                            
                    return task_info, result_list
                        
        except asyncio.TimeoutError:
            msg = f"FETCH_BYBIT_PAGINATED: Таймаут запроса {data_type} для {symbol}. URL: {url}"
            logger.warning(f"{log_prefix} {msg}")
            return task_info, None
            
        except aiohttp.ClientConnectorError as e:
            msg = f"FETCH_BYBIT_PAGINATED: Ошибка соединения {data_type} для {symbol}: {e}. URL: {url}"
            logger.error(f"{log_prefix} {msg}")
            return task_info, None
        
        except Exception as e:
            msg = f"FETCH_BYBIT_PAGINATED: Непредвиденная ошибка {data_type} для {symbol}: {e}. URL: {url}"
            logger.error(f"{log_prefix} {msg}", exc_info=True)
            return task_info, None