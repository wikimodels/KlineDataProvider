import asyncio
import aiohttp
from typing import Dict, Any, Tuple, Optional, List
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# --- Используем логгер из родительского пакета ---
try:
    from .logging_setup import logger, oi_fr_error_logger
except ImportError:
    # Фоллбэк для standalone запуска
    import logging
    logger = logging.getLogger(__name__)
    oi_fr_error_logger = logging.getLogger('oi_fr_errors')


# Лимит одновременных запросов к ОДНОЙ бирже
CONCURRENCY_LIMIT = 3

# Таймаут для каждого запроса
REQUEST_TIMEOUT = 20 # секунд

# --- Изменение №1: Добавляем стандартный User-Agent ---
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
# ---------------------------------------------------

async def fetch_simple(session: aiohttp.ClientSession, task_info: Dict[str, Any], semaphore: asyncio.Semaphore) -> Tuple[Dict[str, Any], Optional[List[Dict]]]:
    """
    Простая стратегия: делает один GET запрос.
    Возвращает (task_info, result_list | None в случае ошибки).
    """
    url = task_info.get("url")
    symbol = task_info.get("symbol", "N/A")
    data_type = task_info.get("data_type", "N/A")
    exchange = task_info.get("exchange", "N/A")
    log_prefix = f"[{task_info.get('original_timeframe', '?').upper()}]" # Используем original_timeframe если есть

    if not url:
        logger.error(f"{log_prefix} FETCH_SIMPLE: Отсутствует URL в task_info для {symbol} ({data_type}, {exchange})")
        return task_info, None

    async with semaphore:
        try:
            # --- Изменение №1: Добавляем headers ---
            async with session.get(url, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS) as response:
            # ------------------------------------
                # logger.debug(f"{log_prefix} FETCH_SIMPLE: Запрос {url} со статусом {response.status}")
                if response.status == 200:
                    data = await response.json()
                    return task_info, data
                elif response.status == 429:
                    logger.warning(f"{log_prefix} FETCH_SIMPLE: Получен 429 Too Many Requests для {url}. Повторная попытка не реализована.")
                    # В будущем можно добавить логику повтора с задержкой
                    return task_info, None
                else:
                    # Логируем другие ошибки, но возвращаем None
                    response_text = await response.text()
                    msg = f"FETCH_SIMPLE: {data_type} для {symbol} ({exchange}) вернул статус {response.status}. Ответ: {response_text[:150]}. URL: {url}"
                    logger.warning(f"{log_prefix} {msg}")
                    if data_type in ['oi', 'fr']:
                        oi_fr_error_logger.warning(f"{log_prefix} {msg}")
                    return task_info, None

        except asyncio.TimeoutError:
            msg = f"FETCH_SIMPLE: Таймаут запроса {data_type} для {symbol} ({exchange}). URL: {url}"
            logger.error(f"{log_prefix} {msg}")
            if data_type in ['oi', 'fr']:
                oi_fr_error_logger.error(f"{log_prefix} {msg}")
            return task_info, None
        except aiohttp.ClientError as e:
            # Обработка других ошибок aiohttp (включая ошибки соединения)
            msg = f"FETCH_SIMPLE: Ошибка при запросе {data_type} для {symbol} ({exchange}): {e}. URL: {url}"
            logger.error(f"{log_prefix} {msg}")
            if data_type in ['oi', 'fr']:
                oi_fr_error_logger.error(f"{log_prefix} {msg}")
            return task_info, None
        except Exception as e:
            # Обработка непредвиденных ошибок (например, при парсинге JSON, хотя он должен быть внутри try)
            msg = f"FETCH_SIMPLE: Непредвиденная ошибка при обработке {data_type} для {symbol} ({exchange}): {e}. URL: {url}"
            logger.error(f"{log_prefix} {msg}", exc_info=True)
            if data_type in ['oi', 'fr']:
                oi_fr_error_logger.error(f"{log_prefix} {msg}")
            return task_info, None


async def fetch_bybit_paginated(session: aiohttp.ClientSession, task_info: Dict[str, Any], semaphore: asyncio.Semaphore) -> Tuple[Dict[str, Any], Optional[List[Dict]]]:
    """
    Стратегия для Bybit V5 API с пагинацией через nextPageCursor.
    Собирает данные со всех страниц.
    Возвращает (task_info, combined_result_list | None в случае ошибки).
    """
    url = task_info.get("url")
    symbol = task_info.get("symbol", "N/A")
    data_type = task_info.get("data_type", "N/A")
    exchange = task_info.get("exchange", "N/A")
    log_prefix = f"[{task_info.get('original_timeframe', '?').upper()}]" # Используем original_timeframe если есть

    # Лимит для Bybit V5 Klines/OI/FR (по документации)
    # Klines/OI - макс 200, FR - макс 100
    bybit_limit_per_page = 100 if data_type == 'fr' else 200

    # Целевое количество записей (может быть больше base_limit из task_builder)
    # Пытаемся собрать ~1000 свечей/записей (5 страниц по 200 или 10 по 100)
    # Этого должно хватить для агрегации 4h -> 8h (нужно 800 свечей 4h для 400 свечей 8h)
    target_records = 1000

    if not url:
        logger.error(f"{log_prefix} FETCH_BYBIT: Отсутствует базовый URL в task_info для {symbol} ({data_type}, {exchange})")
        return task_info, None

    all_results = []
    next_page_cursor = None
    page_count = 0
    max_pages = 10 # Ограничение, чтобы избежать бесконечного цикла

    # Обновляем URL, чтобы включить правильный лимит
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    query_params['limit'] = [str(bybit_limit_per_page)]
    # Собираем URL обратно без пустого cursor
    if 'cursor' in query_params: del query_params['cursor']
    current_url = urlunparse(parsed_url._replace(query=urlencode(query_params, doseq=True)))


    while page_count < max_pages:
        page_count += 1
        url_to_fetch = current_url

        # Добавляем курсор, если он есть (для страниц > 1)
        if next_page_cursor:
            parsed_url = urlparse(current_url)
            query_params = parse_qs(parsed_url.query)
            query_params['cursor'] = [next_page_cursor]
            url_to_fetch = urlunparse(parsed_url._replace(query=urlencode(query_params, doseq=True)))

        async with semaphore:
            try:
                # --- Изменение №1: Добавляем headers ---
                async with session.get(url_to_fetch, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS) as response:
                # ------------------------------------
                    # logger.debug(f"{log_prefix} FETCH_BYBIT (Page {page_count}): Запрос {url_to_fetch} со статусом {response.status}")

                    if response.status == 200:
                        data = await response.json()
                        ret_code = data.get("retCode")
                        ret_msg = data.get("retMsg", "")

                        if ret_code == 0:
                            result_list = data.get("result", {}).get("list", [])
                            if result_list:
                                all_results.extend(result_list)
                                # logger.debug(f"{log_prefix} FETCH_BYBIT (Page {page_count}): Получено {len(result_list)} записей для {symbol} ({data_type}). Всего: {len(all_results)}")
                            else:
                                logger.debug(f"{log_prefix} FETCH_BYBIT (Page {page_count}): Пустой список 'list' в ответе для {symbol} ({data_type}).")

                            # Проверяем наличие следующей страницы
                            next_page_cursor = data.get("result", {}).get("nextPageCursor")
                            if not next_page_cursor or len(all_results) >= target_records:
                                # logger.debug(f"{log_prefix} FETCH_BYBIT: Пагинация завершена для {symbol} ({data_type}). Собрано {len(all_results)} записей.")
                                break # Выходим из цикла while
                        else:
                            msg = f"FETCH_BYBIT: API Bybit вернуло ошибку для {symbol} ({data_type}): Code={ret_code}, Msg='{ret_msg}'. URL: {url_to_fetch}"
                            logger.error(f"{log_prefix} {msg}")
                            if data_type in ['oi', 'fr']:
                                oi_fr_error_logger.error(f"{log_prefix} {msg}")
                            return task_info, None # Прерываем пагинацию при ошибке API

                    elif response.status == 429:
                        logger.warning(f"{log_prefix} FETCH_BYBIT: Получен 429 Too Many Requests для {url_to_fetch}. Прерываю пагинацию.")
                        return task_info, None # Прерываем
                    else:
                        response_text = await response.text()
                        msg = f"FETCH_BYBIT: {data_type} для {symbol} ({exchange}) вернул статус {response.status}. Ответ: {response_text[:150]}. URL: {url_to_fetch}"
                        logger.warning(f"{log_prefix} {msg}")
                        if data_type in ['oi', 'fr']:
                            oi_fr_error_logger.warning(f"{log_prefix} {msg}")
                        return task_info, None # Прерываем

            except asyncio.TimeoutError:
                msg = f"FETCH_BYBIT: Таймаут запроса (Page {page_count}) {data_type} для {symbol} ({exchange}). URL: {url_to_fetch}"
                logger.error(f"{log_prefix} {msg}")
                if data_type in ['oi', 'fr']:
                    oi_fr_error_logger.error(f"{log_prefix} {msg}")
                return task_info, None # Прерываем
            except aiohttp.ClientError as e:
                msg = f"FETCH_BYBIT: Ошибка при запросе (Page {page_count}) {data_type} для {symbol} ({exchange}): {e}. URL: {url_to_fetch}"
                logger.error(f"{log_prefix} {msg}")
                if data_type in ['oi', 'fr']:
                    oi_fr_error_logger.error(f"{log_prefix} {msg}")
                return task_info, None # Прерываем
            except Exception as e:
                msg = f"FETCH_BYBIT: Непредвиденная ошибка (Page {page_count}) при обработке {data_type} для {symbol} ({exchange}): {e}. URL: {url_to_fetch}"
                logger.error(f"{log_prefix} {msg}", exc_info=True)
                if data_type in ['oi', 'fr']:
                    oi_fr_error_logger.error(f"{log_prefix} {msg}")
                return task_info, None # Прерываем

        # Небольшая задержка между страницами, чтобы не нагружать API
        await asyncio.sleep(0.1)

    if page_count >= max_pages:
         logger.warning(f"{log_prefix} FETCH_BYBIT: Достигнут лимит страниц ({max_pages}) для {symbol} ({data_type}). Собрано {len(all_results)} записей.")

    # logger.info(f"{log_prefix} FETCH_BYBIT: Успешно собрано {len(all_results)} записей для {symbol} ({data_type}) за {page_count} стр.")
    return task_info, all_results
