# data_collector/__init__.py

"""
Этот пакет отвечает за полный цикл сбора, обработки и
форматирования рыночных данных с бирж.

Основная функция - fetch_market_data()
"""
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional 
from collections import defaultdict
import random
import time 

# Импортируем из модулей этого пакета
from . import task_builder
from . import data_processing

# --- ИСПРАВЛЕНИЕ: Добавляем прямой импорт get_global_fr_data для worker.py ---
from .fr_fetcher import get_global_fr_data
# --------------------------------------------------------------------------

from . import fr_fetcher # Оставляем импорт модуля, если он используется внутри
from .fetch_strategies import CONCURRENCY_LIMIT
# --- Используем logger напрямую ---
import logging
logger = logging.getLogger(__name__)
# ---------------------------------

# --- Определяем публичный API пакета ---
__all__ = [
    'fetch_market_data',
    'get_global_fr_data', # Теперь доступна напрямую
]
# --------------------------------------


async def fetch_market_data(
    coins: List[Dict], 
    timeframe: str, 
    prefetched_fr_data: Optional[Dict[str, List[Dict]]] = None, 
    skip_formatting: bool = False
) -> Dict[str, Any]:
    """
    Основная функция-оркестратор.
    ...
    """
    log_prefix = f"[{timeframe.upper()}] DATA_COLLECTOR:"
    start_total_time = time.time()
    
    if not coins:
        logger.warning(f"{log_prefix} Список монет пуст. Возвращаю пустые данные.")
        return {"data": [], "audit": {"symbols": 0}}
        
    logger.info(f"{log_prefix} Начинаю цикл сбора данных для {len(coins)} монет.")

    # 1. Готовим задачи
    logger.info(f"{log_prefix} 1/6: Подготовка задач (Klines/OI/FR)...")
    tasks_to_run = task_builder.prepare_tasks(
        coins, 
        timeframe, 
        prefetched_fr_data
    )
    
    if not tasks_to_run:
        logger.error(f"{log_prefix} 1/6: Не удалось создать задачи. Прерывание.")
        return {"data": [], "audit": {"symbols": 0}}

    logger.info(f"{log_prefix} 1/6: Готово. Всего {len(tasks_to_run)} задач.")


    # 2. Выполняем задачи параллельно
    logger.info(f"{log_prefix} 2/6: Запуск асинхронного сбора данных (Лимит: {CONCURRENCY_LIMIT})...")
    start_fetch_time = time.time()
    
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async_tasks = []
    
    # --- ИСПРАВЛЕНИЕ УТЕЧКИ: Создаем ОДНУ ClientSession в контекстном менеджере ---
    
    # --- ИЗМЕНЕНИЕ: Логика сопоставления задач и результатов ---
    tasks_in_gather_order = [] 
    
    async with aiohttp.ClientSession() as session:
        
        # Распределяем Klines, OI, FR задачи
        klines_oi_tasks = [t for t in tasks_to_run if t['task_info']['data_type'] in ['klines', 'oi']]
        fr_tasks = [t for t in tasks_to_run if t['task_info']['data_type'] == 'fr']

        # Перемешиваем Klines/OI задачи для лучшего распределения нагрузки
        random.shuffle(klines_oi_tasks)
        
        # Собираем задачи В ТОМ ПОРЯДКЕ, в котором они будут запущены
        tasks_in_gather_order = klines_oi_tasks + fr_tasks
        
        # Собираем все задачи
        for task in tasks_in_gather_order:
            strategy_func = task["fetch_strategy"]
            task_info = task["task_info"]
            # Передаем ОДИН ClientSession всем задачам
            async_tasks.append(strategy_func(session, task_info, semaphore)) 
            
        try:
            results = await asyncio.gather(*async_tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"{log_prefix} 2/6: Критическая ошибка во время asyncio.gather: {e}", exc_info=True)
            return {"data": [], "audit": {"symbols": 0}}
    # --- КОНЕЦ ИСПРАВЛЕНИЯ УТЕЧКИ ---

    end_fetch_time = time.time()
    logger.info(f"{log_prefix} 2/6: Сбор данных завершен за {end_fetch_time - start_fetch_time:.2f} сек.")
    
# ... (Остальная часть функции fetch_market_data остается без изменений) ...

    # 3. Добавляем prefetched FR (если они были)
    if prefetched_fr_data:
        logger.info(f"{log_prefix} 3/6: Добавляю предварительно собранные {len(prefetched_fr_data)} FR...")
        # (FR данные добавляются на шаге парсинга/обработки)
    else:
        logger.info(f"{log_prefix} 3/6: Предварительно собранные FR отсутствуют.")

    
    # 4. Парсинг и обработка результатов
    logger.info(f"{log_prefix} 4/6: Начинаю парсинг Klines/OI/FR...")
    start_parse_time = time.time()
    
    processed_data = defaultdict(lambda: defaultdict(list))
    
    # --- ИЗМЕНЕНИЕ: Используем tasks_in_gather_order для zip ---
    for task, result in zip(tasks_in_gather_order, results):
    # ---------------------------------------------------------
        task_info = task['task_info']
        symbol = task_info['symbol']
        data_type = task_info['data_type']
        
        try:
            if isinstance(result, Exception):
                logger.error(f"{log_prefix} Ошибка запроса для {symbol} ({data_type}): {result}")
                continue
                
            task_info_result, raw_data = result
            
            if not raw_data:
                continue
                
            parser_func = task['parser']
            timeframe_arg = task['timeframe']
            
            # --- Логика для FR из prefetched_fr_data ---
            if data_type == 'fr' and prefetched_fr_data:
                fr_list = prefetched_fr_data.get(symbol, [])
                if fr_list:
                    processed_data[symbol][data_type].extend(fr_list)
                continue
            # ---------------------------------------------

            # Основной парсинг
            parsed_result = parser_func(raw_data, timeframe_arg)

            if parsed_result:
                processed_data[symbol][data_type].extend(parsed_result)
            
        except Exception as e:
            logger.error(f"{log_prefix} Ошибка парсинга для {symbol} ({data_type}): {e}", exc_info=True)
            
    end_parse_time = time.time()
    logger.info(f"{log_prefix} 4/6: Парсинг завершен за {end_parse_time - start_parse_time:.2f} сек.")


    # 5. Объединение данных (Klines + OI + FR)
    logger.info(f"{log_prefix} 5/6: Начинаю объединение данных Klines/OI/FR...")
    start_merge_time = time.time()
    merged_data = data_processing.merge_data(processed_data)
    end_merge_time = time.time()
    logger.info(f"{log_prefix} 5/6: Объединение данных завершено за {end_merge_time - start_merge_time:.2f} сек.")

    
    # --- ИЗМЕНЕНИЕ №1: Переносим 'skip_formatting' ДО шага 6 ---
    # (Это нужно, чтобы worker.py мог получить ПОЛНЫЕ (800 свечей) merged_data
    # для '4h' ПЕРЕД тем, как 'format_final_structure' их обрежет)
    if skip_formatting:
        logger.info(f"{log_prefix} 6/6: Пропускаю финальное форматирование (skip_formatting=True). Возвращаю 'merged_data'.")
        end_total_time = time.time()
        logger.info(f"{log_prefix} --- Цикл сбора данных (raw) завершен за {end_total_time - start_total_time:.2f} сек. ---")
        return merged_data 
    # --- КОНЕЦ ИЗМЕНЕНИЯ №1 ---


    # 6. Финальное форматирование
    logger.info(f"{log_prefix} 6/6: Начинаю финальное форматирование...")
    start_format_time = time.time()
    final_structured_data = data_processing.format_final_structure(merged_data, coins, timeframe)
    end_format_time = time.time()
    logger.info(f"{log_prefix} 6/6: Финальное форматирование завершено за {end_format_time - start_format_time:.2f} сек.")

    end_total_time = time.time()
    logger.info(f"{log_prefix} --- Полный цикл сбора данных завершен за {end_total_time - start_total_time:.2f} сек. ---")
    
    return final_structured_data
