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
            # --- ИЗМЕНЕНИЕ: Используем logger.error ---
            logger.error(f"{log_prefix} {msg}")
            return task_info, None
        except aiohttp.ClientError as e:
            msg = f"FETCH_SIMPLE: Ошибка при запросе {data_type} для {symbol} ({exchange}): {e}. URL: {url}"
            # --- ИЗМЕНЕНИЕ: Используем logger.error ---
            logger.error(f"{log_prefix} {msg}")
            return task_info, None
        except Exception as e:
            msg = f"FETCH_SIMPLE: Непредвиденная ошибка при обработке {data_type} для {symbol} ({exchange}): {e}. URL: {url}"
            # --- ИЗМЕНЕНИЕ: Используем logger.error ---
            logger.error(f"{log_prefix} {msg}", exc_info=True)
            return task_info, None


async def fetch_bybit_paginated(session: aiohttp.ClientSession, task_info: Dict[str, Any], semaphore: asyncio.Semaphore) -> Tuple[Dict[str, Any], Optional[List[Dict]]]:
    """
    (Код не изменен, кроме вызова логгера)
    """
    # ... (получение url, symbol, data_type ...)
    url = task_info.get("url")
    symbol = task_info.get("symbol", "N/A")
    data_type = task_info.get("data_type", "N/A")
    exchange = task_info.get("exchange", "N/A")
    log_prefix = f"[{task_info.get('original_timeframe', '?').upper()}]"
    
    # ... (логика лимитов, target_records ...)
    bybit_limit_per_page = 100 if data_type == 'fr' else 200
    target_records = 1000
    
    if not url:
        logger.error(f"{log_prefix} FETCH_BYBIT: Отсутствует базовый URL в task_info для {symbol} ({data_type}, {exchange})")
        return task_info, None
    
    # ... (логика пагинации, current_url ...)
    all_results = []
    next_page_cursor = None
    page_count = 0
    max_pages = 10
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    query_params['limit'] = [str(bybit_limit_per_page)]
    if 'cursor' in query_params: del query_params['cursor']
    current_url = urlunparse(parsed_url._replace(query=urlencode(query_params, doseq=True)))
    
    while page_count < max_pages:
        page_count += 1
        url_to_fetch = current_url
        if next_page_cursor:
            # ... (добавление курсора) ...
            parsed_url = urlparse(current_url)
            query_params = parse_qs(parsed_url.query)
            query_params['cursor'] = [next_page_cursor]
            url_to_fetch = urlunparse(parsed_url._replace(query=urlencode(query_params, doseq=True)))

        async with semaphore:
            try:
                async with session.get(url_to_fetch, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS) as response:
                    if response.status == 200:
                        data = await response.json()
                        ret_code = data.get("retCode")
                        ret_msg = data.get("retMsg", "")

                        if ret_code == 0:
                            # ... (обработка list) ...
                            result_list = data.get("result", {}).get("list", [])
                            if result_list:
                                all_results.extend(result_list)
                            
                            next_page_cursor = data.get("result", {}).get("nextPageCursor")
                            if not next_page_cursor or len(all_results) >= target_records:
                                break # Выходим из цикла while
                        else:
                            msg = f"FETCH_BYBIT: API Bybit вернуло ошибку для {symbol} ({data_type}): Code={ret_code}, Msg='{ret_msg}'. URL: {url_to_fetch}"
                            # --- ИЗМЕНЕНИЕ: Используем logger.error ---
                            logger.error(f"{log_prefix} {msg}")
                            return task_info, None

                    elif response.status == 429:
                        logger.warning(f"{log_prefix} FETCH_BYBIT: Получен 429 Too Many Requests для {url_to_fetch}. Прерываю пагинацию.")
                        return task_info, None
                    else:
                        response_text = await response.text()
                        msg = f"FETCH_BYBIT: {data_type} для {symbol} ({exchange}) вернул статус {response.status}. Ответ: {response_text[:150]}. URL: {url_to_fetch}"
                        # --- ИЗМЕНЕНИЕ: Используем logger.warning ---
                        if data_type in ['oi', 'fr']:
                             logger.warning(f"{log_prefix} {msg}")
                        else:
                             logger.warning(f"{log_prefix} {msg}")
                        return task_info, None

            except asyncio.TimeoutError:
                msg = f"FETCH_BYBIT: Таймаут запроса (Page {page_count}) {data_type} для {symbol} ({exchange}). URL: {url_to_fetch}"
                # --- ИЗМЕНЕНИЕ: Используем logger.error ---
                logger.error(f"{log_prefix} {msg}")
                return task_info, None
            except aiohttp.ClientError as e:
                msg = f"FETCH_BYBIT: Ошибка при запросе (Page {page_count}) {data_type} для {symbol} ({exchange}): {e}. URL: {url_to_fetch}"
                # --- ИЗМЕНЕНИЕ: Используем logger.error ---
                logger.error(f"{log_prefix} {msg}")
                return task_info, None
            except Exception as e:
                msg = f"FETCH_BYBIT: Непредвиденная ошибка (Page {page_count}) при обработке {data_type} для {symbol} ({exchange}): {e}. URL: {url_to_fetch}"
                # --- ИЗМЕНЕНИЕ: Используем logger.error ---
                logger.error(f"{log_prefix} {msg}", exc_info=True)
                return task_info, None

        await asyncio.sleep(0.1)

    # ... (остальная часть) ...
    if page_count >= max_pages:
         logger.warning(f"{log_prefix} FETCH_BYBIT: Достигнут лимит страниц ({max_pages}) для {symbol} ({data_type}). Собрано {len(all_results)} записей.")
    return task_info, all_results