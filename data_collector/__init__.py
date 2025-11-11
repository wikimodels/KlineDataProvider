"""
Этот пакет отвечает за полный цикл сбора, обработки и
форматирования рыночных данных с бирж.

Основная функция - fetch_market_data()
"""
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional # Добавляем Optional
from collections import defaultdict
import random
import time # Добавляем time для замеров

# Импортируем из модулей этого пакета
from . import task_builder
from . import data_processing
from .fetch_strategies import CONCURRENCY_LIMIT
# --- Используем logger напрямую ---
import logging
logger = logging.getLogger(__name__)
# --- ИЗМЕНЕНИЕ: oi_fr_error_logger УДАЛЕН ---
# ---------------------------------


# --- ИЗМЕНЕНИЕ: Добавлен аргумент skip_formatting ---
async def fetch_market_data(coins: List[Dict], timeframe: str, prefetched_fr_data: Optional[Dict[str, List[Dict]]] = None, skip_formatting: bool = False) -> Dict[str, Any]:
# -----------------------------------------------------------
    """
    Основная функция-оркестратор.
    1. Готовит задачи (Klines/OI, пропускает FR если есть prefetched)
    2. Выполняет их параллельно
    3. Добавляет prefetched FR
    4. Парсит результаты Klines/OI
    5. Объединяет данные
    6. Форматирует финальный ответ
    (Код этой функции не изменен)
    """
    log_prefix = f"[{timeframe.upper()}]"
    logger.info(f"{log_prefix} --- Начинаю цикл сбора данных Klines/OI ---")
    start_total_time = time.time() # Засекаем общее время

    # 1. Подготовка задач (Klines/OI)
    # --- Передаем prefetched_fr_data ---
    tasks_to_run = task_builder.prepare_tasks(coins, timeframe, prefetched_fr_data)
    # -----------------------------------------------
    if not tasks_to_run:
        # Если нет задач Klines/OI, но есть FR, их нужно обработать
        if prefetched_fr_data:
             logger.warning(f"{log_prefix} Нет задач Klines/OI, но есть предзагруженные FR. Попытка обработки только FR.")
             processed_data = defaultdict(dict)
             # --- Добавляем FR к пустому processed_data ---
             logger.info(f"{log_prefix} Добавляю предзагруженные FR данные...")
             fr_added_count = 0
             for symbol, fr_list in prefetched_fr_data.items():
                 if fr_list: # Добавляем только если есть данные
                     processed_data[symbol]['fr'] = fr_list
                     fr_added_count += 1
             logger.info(f"{log_prefix} Добавлено {fr_added_count} записей FR.")
             # ----------------------------------------------------
             # Переходим сразу к объединению (которое ничего не сделает без Klines) и форматированию
             merged_data = data_processing.merge_data(processed_data) # Вернет {}
             
             # --- ИЗМЕНЕНИЕ: Пропускаем форматирование, если skip_formatting=True ---
             if skip_formatting:
                 logger.info(f"{log_prefix} Пропускаю финальное форматирование (skip_formatting=True). Возвращаю 'merged_data'.")
                 end_total_time = time.time()
                 logger.info(f"{log_prefix} --- Цикл сбора данных (raw) завершен за {end_total_time - start_total_time:.2f} сек. ---")
                 return merged_data
             # ------------------------------------------------------------------
             
             final_structured_data = data_processing.format_final_structure(merged_data, coins, timeframe)
             end_total_time = time.time()
             logger.info(f"{log_prefix} --- Цикл сбора данных завершен (только FR) за {end_total_time - start_total_time:.2f} сек. ---")
             return final_structured_data
        else:
            logger.warning(f"{log_prefix} Нет задач Klines/OI и нет предзагруженных FR.")
            
            # --- ИЗМЕНЕНИЕ: Пропускаем форматирование, если skip_formatting=True ---
            if skip_formatting:
                logger.info(f"{log_prefix} Пропускаю финальное форматирование (skip_formatting=True). Возвращаю {{}}.")
                return {} # Возвращаем пустой dict
            # ------------------------------------------------------------------
                
            return data_processing.format_final_structure({}, coins, timeframe) # Возвращаем пустую структуру


    # Логирование примеров задач (без изменений)
    logger.debug(f"{log_prefix} Примеры задач для выполнения (Klines/OI, до 5 шт):")
    sample_tasks = random.sample(tasks_to_run, min(5, len(tasks_to_run)))
    for i, task in enumerate(sample_tasks):
        ti = task['task_info']
        parser_name = task['parser'].__name__ if task.get('parser') else 'N/A'
        strategy_name = task['fetch_strategy'].__name__ if task.get('fetch_strategy') else 'N/A'
        logger.debug(f"{log_prefix}   {i+1}. {ti.get('symbol')} ({ti.get('data_type')}, {ti.get('exchange')}) | URL: {ti.get('url', 'N/A')[:100]}... | Parser: {parser_name} | Strategy: {strategy_name}")


    processed_data = defaultdict(dict)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    # 2. Выполнение задач (Klines/OI)
    logger.info(f"{log_prefix} Запускаю {len(tasks_to_run)} задач (Klines/OI) с лимитом {CONCURRENCY_LIMIT}...")
    start_gather_time = time.time()

    async with aiohttp.ClientSession() as session:
        async_tasks = []
        for task in tasks_to_run:
            strategy_func = task["fetch_strategy"]
            task_info = task["task_info"]
            async_tasks.append(strategy_func(session, task_info, semaphore))

        try:
            results = await asyncio.gather(*async_tasks, return_exceptions=True)
        except Exception as e:
            logger.critical(f"{log_prefix} Критическая ошибка во время asyncio.gather (Klines/OI): {e}", exc_info=True)
            results = [e] * len(tasks_to_run) # Заполняем результатами-ошибками

    end_gather_time = time.time()
    logger.info(f"{log_prefix} Запросы Klines/OI завершены за {end_gather_time - start_gather_time:.2f} сек.")

    # 3. Парсинг результатов (Klines/OI)
    logger.info(f"{log_prefix} Начинаю парсинг {len(results)} результатов (Klines/OI)...")
    failed_tasks_count = 0
    success_tasks_count = 0
    parsed_non_empty_count = 0
    start_parse_time = time.time()

    for task, result in zip(tasks_to_run, results):
        task_info = task['task_info']
        symbol = task_info['symbol']
        data_type = task_info['data_type'] # Будет 'klines' или 'oi'
        exchange = task_info['exchange']
        url = task_info.get('url', 'N/A')

        try:
            if isinstance(result, Exception):
                failed_tasks_count += 1
                msg = f"FETCH_GATHER: Запрос провален: {symbol} ({data_type}, {exchange}). Ошибка: {result}. URL: {url}"
                # --- ИЗМЕНЕНИЕ: Используем logger.error ---
                logger.error(f"{log_prefix} {msg}")
                if data_type == 'oi': # Логируем только ошибки OI отдельно
                    logger.warning(f"{log_prefix} {msg}") # Используем warning, т.к. это ожидаемо
                continue

            _, raw_data = result
            success_tasks_count +=1
            raw_data_size = len(raw_data) if isinstance(raw_data, list) else 1 if raw_data else 0
            # logger.debug(f"{log_prefix} FETCH_GATHER: Получено {raw_data_size} raw_data для {symbol} ({data_type}, {exchange})") # Уменьшаем шум

            if not raw_data:
                msg = f"PARSER: Нет raw_data для {symbol} ({data_type}, {exchange}). Запрос вернул пустой ответ. URL: {url}"
                # --- ИЗМЕНЕНИЕ: Используем logger.warning ---
                logger.warning(f"{log_prefix} {msg}")
                if data_type == 'oi':
                    logger.warning(f"{log_prefix} {msg}")
                continue

            parser_func = task['parser']
            timeframe_arg = task['timeframe']
            parsed_result = parser_func(raw_data, timeframe_arg)
            parsed_result_size = len(parsed_result) if isinstance(parsed_result, list) else 0
            # logger.debug(f"{log_prefix} PARSER: Распарсено {parsed_result_size} для {symbol} ({data_type}, {exchange})") # Уменьшаем шум

            if not parsed_result:
                # logger.warning(f"{log_prefix} PARSER: Парсер вернул пустой список для {symbol} ({data_type}, {exchange}). Raw data size: {raw_data_size}") # Уменьшаем шум
                pass # Не логируем пустые парсеры как warning
            else:
                parsed_non_empty_count += 1
                # Сохраняем Klines или OI
                processed_data[symbol][data_type] = parsed_result

        except Exception as e:
            failed_tasks_count += 1 # Считаем ошибку парсинга как провал задачи
            msg = f"PARSER: Ошибка парсинга {symbol} ({data_type}, {exchange}): {e}. URL: {url}"
            # --- ИЗМЕНЕНИЕ: Используем logger.error ---
            logger.error(f"{log_prefix} {msg}", exc_info=True)
            if data_type == 'oi':
                logger.error(f"{log_prefix} {msg}")

    end_parse_time = time.time()
    logger.info(f"{log_prefix} Парсинг Klines/OI завершен за {end_parse_time - start_parse_time:.2f} сек.")

    # --- Добавляем предзагруженные FR данные ---
    if prefetched_fr_data:
        logger.info(f"{log_prefix} Добавляю предзагруженные FR данные к результатам Klines/OI...")
        fr_added_count = 0
        for symbol, fr_list in prefetched_fr_data.items():
            if fr_list: # Добавляем только если есть данные
                # Добавляем FR к словарю для этого символа
                processed_data[symbol]['fr'] = fr_list
                fr_added_count += 1
            # else: # Не логируем отсутствие FR здесь, аудит сделает это позже
            #     logger.debug(f"{log_prefix} Предзагруженные FR для {symbol} пусты.")
        logger.info(f"{log_prefix} Добавлено {fr_added_count} записей FR из предзагруженных данных.")
    else:
        logger.warning(f"{log_prefix} Предзагруженные FR данные отсутствуют (prefetched_fr_data is None).")
    # ----------------------------------------------------

    # Итоги сбора (без изменений)
    total_tasks = len(tasks_to_run) # Только Klines/OI
    logger.info(f"{log_prefix} FETCH_GATHER Итог (Klines/OI): Успешных запросов: {success_tasks_count}/{total_tasks}. Провалено запросов: {failed_tasks_count}/{total_tasks}.")
    logger.info(f"{log_prefix} PARSER Итог (Klines/OI): Успешно распарсено (непустых): {parsed_non_empty_count} из {success_tasks_count} успешных запросов.")


    # 4. Объединение данных (Klines + OI + FR)
    logger.info(f"{log_prefix} Начинаю объединение данных Klines/OI/FR...")
    start_merge_time = time.time()
    merged_data = data_processing.merge_data(processed_data)
    end_merge_time = time.time()
    logger.info(f"{log_prefix} Объединение данных завершено за {end_merge_time - start_merge_time:.2f} сек.")

    
    # --- ИЗМЕНЕНИЕ: Пропускаем форматирование, если skip_formatting=True ---
    if skip_formatting:
        logger.info(f"{log_prefix} Пропускаю финальное форматирование (skip_formatting=True). Возвращаю 'merged_data'.")
        end_total_time = time.time()
        logger.info(f"{log_prefix} --- Цикл сбора данных (raw) завершен за {end_total_time - start_total_time:.2f} сек. ---")
        return merged_data # Возвращаем сырые, но слитые данные
    # ------------------------------------------------------------------


    # 5. Финальное форматирование
    logger.info(f"{log_prefix} Начинаю финальное форматирование...")
    start_format_time = time.time()
    final_structured_data = data_processing.format_final_structure(merged_data, coins, timeframe)
    end_format_time = time.time()
    logger.info(f"{log_prefix} Финальное форматирование завершено за {end_format_time - start_format_time:.2f} сек.")


    end_total_time = time.time()
    logger.info(f"{log_prefix} --- Цикл сбора данных завершен за {end_total_time - start_total_time:.2f} сек. ---")
    return final_structured_data
