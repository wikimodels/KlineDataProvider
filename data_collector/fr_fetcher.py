"""
Этот модуль отвечает за ОДНУ задачу:
Обновление ГЛОБАЛЬНОГО КЭША Funding Rate (cache:global_fr).

Вызывается по расписанию (через cron-job эндпоинт в main.py).
"""
import asyncio
import logging
import time
from typing import Dict, Any, List, Optional
from collections import defaultdict
import aiohttp

# --- Импорты из пакета/родителя ---
try:
    # Относительные импорты из пакета data_collector
    from . import task_builder
    from .logging_setup import logger
    
    # --- ИЗМЕНЕНИЕ: Импортируем новый coin_source ---
    # (Удален импорт database)
    from coin_source import get_coins_from_api
    # ------------------------------------------------
    
    # Импорты из корневого уровня
    from cache_manager import save_to_cache
except ImportError:
    # Фоллбэки
    import task_builder
    import logging
    logger = logging.getLogger(__name__)
    # Фоллбэк для coin_source
    async def get_coins_from_api(): return None 
    def save_to_cache(key, data): pass

# --- Константы (скопированы из worker.py) ---
FR_CONCURRENCY_LIMIT = 5
ERROR_RETRY_DELAY = 10


async def fetch_funding_rates(coins_from_api: List[Dict]) -> Optional[Dict[str, List[Dict]]]:
    """
    (Перенесено из worker.py)
    Выполняет сбор данных Funding Rate для всех монет один раз.
    """
    logger.info("[GLOBAL_FR_FETCH] Начинаю предварительный сбор Funding Rates...")
    start_time = time.time()

    # --- Используем coins_from_api ---
    fr_tasks_to_run = task_builder.prepare_fr_tasks(coins_from_api)
    if not fr_tasks_to_run:
        logger.warning("[GLOBAL_FR_FETCH] Нет задач для сбора FR.")
        return {}

    fr_semaphore = asyncio.Semaphore(FR_CONCURRENCY_LIMIT)
    processed_fr_data = defaultdict(list)

    async with aiohttp.ClientSession() as session:
        async_tasks = []
        for task in fr_tasks_to_run:
            strategy_func = task["fetch_strategy"]
            task_info = task["task_info"]
            async_tasks.append(strategy_func(session, task_info, fr_semaphore))

        try:
            results = await asyncio.gather(*async_tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"[GLOBAL_FR_FETCH] Критическая ошибка во время asyncio.gather для FR: {e}", exc_info=True)
            return None

    failed_fr_count = 0
    parsed_fr_count = 0
    for task, result in zip(fr_tasks_to_run, results):
        task_info = task['task_info']
        symbol = task_info['symbol']

        try:
            if isinstance(result, Exception):
                failed_fr_count += 1
                logger.error(f"[GLOBAL_FR_FETCH] Ошибка запроса FR для {symbol}: {result}")
                continue

            _, raw_data = result
            if not raw_data:
                continue

            parser_func = task['parser']
            timeframe_arg = task['timeframe']
            parsed_result = parser_func(raw_data, timeframe_arg)

            if parsed_result:
                processed_fr_data[symbol].extend(parsed_result)
                parsed_fr_count += 1

        except Exception as e:
            failed_fr_count += 1
            logger.error(f"[GLOBAL_FR_FETCH] Ошибка парсинга FR для {symbol}: {e}", exc_info=True)

    end_time = time.time()
    # Эта переменная используется в логе ниже
    total_fr_tasks = len(fr_tasks_to_run)
    
    # --- ИСПРАВЛЕНИЕ: (было 'total_tasks', стало 'total_fr_tasks') ---
    logger.info(f"[GLOBAL_FR_FETCH] Сбор FR завершен за {end_time - start_time:.2f} сек. "
                f"Успешно: {parsed_fr_count}/{total_fr_tasks}. Ошибок: {failed_fr_count}/{total_fr_tasks}.")
    # -----------------------------------------------------------------

    if failed_fr_count > 0:
        logger.warning(f"[GLOBAL_FR_FETCH] Были ошибки при сборе {failed_fr_count} FR. Данные могут быть неполными.")

    return dict(processed_fr_data)


async def run_fr_update_process():
    """
    Главная функция, запускаемая эндпоинтом.
    1. Получает ВСЕ монеты из API (coin_source).
    2. Собирает для них FR.
    3. Сохраняет в 'cache:global_fr'.
    """
    logger.info("[CRON_JOB] Запущена задача обновления cache:global_fr...")
    start_time = time.time()
    try:
        # --- Получаем монеты из API ---
        logger.info("[CRON_JOB] 1/3: Запрос *всех* монет из API (coin-sifter)...")
        all_coins = await get_coins_from_api()
        if not all_coins:
             logger.error("[CRON_JOB] 1/3: Не удалось получить монеты из API (None или []). Обновление FR отменено.")
             return
        logger.info(f"[CRON_JOB] 1/3: Получено {len(all_coins)} монет.")
        # -----------------------------

        # 2. Собираем FR
        logger.info("[CRON_JOB] 2/3: Запуск сбора FR...")
        fr_data = await fetch_funding_rates(all_coins)
        
        if fr_data is None:
            logger.error("[CRON_JOB] 2/3: Сбор FR вернул None (критическая ошибка). Обновление FR отменено.")
            return
        if not fr_data:
            logger.warning("[CRON_JOB] 2/3: Сбор FR вернул пустой словарь. Кэш FR не будет обновлен.")
            return
        
        logger.info("[CRON_JOB] 2/3: Сбор FR успешно завершен.")

        # 3. Сохраняем в кэш
        logger.info("[CRON_JOB] 3/3: Сохранение данных в 'cache:global_fr'...")
        save_to_cache('global_fr', fr_data)
        
        end_time = time.time()
        logger.info(f"[CRON_JOB] Успех! Обновление 'cache:global_fr' завершено за {end_time - start_time:.2f} сек.")

    except Exception as e:
        logger.critical(f"[CRON_JOB] КРИТИЧЕСКАЯ ОШИБКА в run_fr_update_process: {e}", exc_info=True)