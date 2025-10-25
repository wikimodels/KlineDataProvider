import asyncio
import aiohttp
import urllib.parse
from typing import Dict, Any, List, Tuple

# --- ИСПРАВЛЕНИЕ 1: Импорты ---
# Импортируем из родительской директории (..), т.к. api_helpers.py лежит снаружи
from api_helpers import fetch_url 
# Импортируем из текущей директории (.), т.к. logging_setup.py лежит здесь же
from .logging_setup import logger, oi_fr_error_logger


CONCURRENCY_LIMIT = 10 # Ограничение одновременных запросов

async def fetch_simple(
    session: aiohttp.ClientSession, 
    task_info: Dict, 
    semaphore: asyncio.Semaphore
) -> Tuple[Dict, Any]:
    """
    Простая стратегия: запрашивает один URL.
    Возвращает (task_info, json_data)
    """
    async with semaphore:
        # --- ИЗМЕНЕНИЕ: Принимаем 4 значения от fetch_url ---
        # Нам нужен только json_data, так как task_info у нас уже есть.
        _, _, _, json_data = await fetch_url(session, task_info)
    
    # Возвращаем исходный task_info и необработанный json_data
    return task_info, json_data

async def fetch_bybit_paginated(
    session: aiohttp.ClientSession, 
    task_info: Dict, 
    semaphore: asyncio.Semaphore
) -> Tuple[Dict, List[Dict]]:
    """
    Стратегия пагинации для Bybit (OI и FR).
    Собирает данные со всех страниц и возвращает единый список.
    Возвращает (task_info, combined_data_list)
    """
    all_paginated_data = []
    cursor = None
    max_pages_per_symbol = 5 # Ограничение
    page_count = 0
    
    # Извлекаем базовый URL из task_info
    base_url = task_info['url']
    symbol = task_info['symbol']
    data_type = task_info['data_type']

    while page_count < max_pages_per_symbol:
        page_count += 1
        
        # Модифицируем URL для пагинации
        parsed_url = urllib.parse.urlparse(base_url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        
        # Устанавливаем лимит (Bybit рекомендует 200 для OI, 100 для FR)
        limit = 200 if data_type == 'oi' else 100
        query_params['limit'] = [str(limit)]

        if cursor:
            query_params['cursor'] = [cursor]
            # logger.info(f"FETCH: Запрашиваю Bybit {data_type} для {symbol}, страница {page_count}, cursor: {cursor}") # <-- УДАЛЕН ШУМ
        else:
            # logger.info(f"FETCH: Запрашиваю Bybit {data_type} для {symbol}, первая страница.") # <-- УДАЛЕН ШУМ
            pass # Оставляем для сохранения структуры

        new_query_string = urllib.parse.urlencode(query_params, doseq=True)
        url_with_cursor = parsed_url._replace(query=new_query_string).geturl()
        
        # Создаем временный task_info для fetch_url
        page_task_info = task_info.copy()
        page_task_info['url'] = url_with_cursor

        async with semaphore:
            # --- ИЗМЕНЕНИЕ: Принимаем 4 значения от fetch_url ---
            _, _, _, json_data = await fetch_url(session, page_task_info)

        if not json_data:
            logger.warning(f"FETCH: json_data пуст для Bybit {data_type} {symbol}, страница {page_count}. Прерываю пагинацию.")
            break

        result_data = json_data.get('result', {})
        data_list = result_data.get('list', [])
        
        if not data_list:
            # logger.info(f"FETCH: Список данных Bybit {data_type} для {symbol} пуст на странице {page_count}. Прерываю пагинацию.") # <-- УДАЛЕН ШУМ
            break

        all_paginated_data.extend(data_list)
        
        cursor = result_data.get('nextPageCursor') or result_data.get('cursor')
        if not cursor:
            # logger.info(f"FETCH: Cursor отсутствует для Bybit {data_type} {symbol} на странице {page_count}. Пагинация завершена.") # <-- УДАЛЕН ШУМ
            break
            
    if all_paginated_data:
        # logger.info(f"FETCH: Собрано {len(all_paginated_data)} записей Bybit {data_type} для {symbol} (с пагинацией).") # <-- УДАЛЕН ШУМ
        pass # Оставляем для сохранения структуры
    
    # Возвращаем исходный task_info и *объединенный* список данных
    return task_info, all_paginated_data

