# fr_fetcher.py
"""
Этот модуль отвечает за ОДНУ задачу:
Обновление ГЛОБАЛЬНОГО КЭША Funding Rate (cache:global_fr).
"""
import asyncio
import logging
import time
from typing import Dict, Any, List, Optional
from collections import defaultdict
import aiohttp

# --- Импорты из cache_manager (внешние) ---
from cache_manager import save_to_cache, get_redis_connection 
# ----------------------------------------------------

# --- Импорты из пакета data_collector (внутренние) ---
try:
    from . import task_builder
    from .logging_setup import logger
    
    # --- ИСПРАВЛЕНИЕ: Используем относительный импорт, так как coin_source находится в этом же пакете ---
    from .coin_source import get_coins as get_coins_func 
    # ----------------------------------------------------
    
except ImportError:
    # Фоллбэки для standalone запуска
    import task_builder
    import logging
    logger = logging.getLogger(__name__)
    async def get_coins_func(): return [] # <-- ФОЛЛБЭК
    async def save_to_cache(redis_conn, key, data): pass
    async def get_redis_connection(): return None


# --- Константы (скопированы из worker.py) ---
FR_CONCURRENCY_LIMIT = 5
ERROR_RETRY_DELAY = 10


async def fetch_funding_rates(coins_from_api: List[Dict[str, Any]]) -> Optional[Dict[str, List[Dict]]]:
    """
    Выполняет асинхронный сбор Funding Rates для заданного списка монет.
    """
    logger.info(f"[GLOBAL_FR_FETCH] Начинаю предварительный сбор Funding Rates...")
    
    tasks_to_run = task_builder.prepare_fr_tasks(coins_from_api)
    if not tasks_to_run:
        logger.warning("[GLOBAL_FR_FETCH] Не удалось создать задачи FR. Возвращаю None.")
        return None
        
    start_fetch_time = time.time()
    semaphore = asyncio.Semaphore(FR_CONCURRENCY_LIMIT)
    async_tasks = []
    
    # --- ИСПРАВЛЕНИЕ УТЕЧКИ: Создаем ОДНУ ClientSession в контекстном менеджере ---
    async with aiohttp.ClientSession() as session:
        
        for task in tasks_to_run:
            strategy_func = task["fetch_strategy"]
            task_info = task["task_info"]
            # Передаем ОДИН ClientSession всем задачам
            async_tasks.append(strategy_func(session, task_info, semaphore))
            
        try:
            results = await asyncio.gather(*async_tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"[GLOBAL_FR_FETCH] Критическая ошибка во время asyncio.gather: {e}", exc_info=True)
            return None

    # --- КОНЕЦ ИСПРАВЛЕНИЯ УТЕЧКИ ---
    
    end_fetch_time = time.time()
    
    # --- Обработка результатов ---
    processed_fr_data = defaultdict(list)
    success_count = 0
    error_count = 0
    
    for task, result in zip(tasks_to_run, results):
        task_info = task['task_info']
        symbol = task_info['symbol']
        
        try:
            if isinstance(result, Exception):
                logger.error(f"[GLOBAL_FR_FETCH] Ошибка запроса для {symbol}: {result}")
                error_count += 1
                continue
                
            task_info_result, raw_data = result
            
            if not raw_data:
                error_count += 1
                continue
                
            parser_func = task['parser']
            timeframe_arg = task['timeframe']
            
            # Парсинг
            parsed_result = parser_func(raw_data, timeframe_arg)

            if parsed_result:
                processed_fr_data[symbol].extend(parsed_result)
                success_count += 1
            else:
                error_count += 1
            
        except Exception as e:
            logger.error(f"[GLOBAL_FR_FETCH] Ошибка парсинга FR для {symbol}: {e}", exc_info=True)
            error_count += 1
    
    logger.info(f"[GLOBAL_FR_FETCH] Сбор FR завершен за {end_fetch_time - start_fetch_time:.2f} сек. Успешно: {success_count}/{len(coins_from_api)}. Ошибок: {error_count}/{len(coins_from_api)}.")
    
    return dict(processed_fr_data) if processed_fr_data else None


async def run_fr_update_process():
    """
    Основной оркестратор для сбора и сохранения Global Funding Rate.
    Эта функция вызывается воркером при обработке задачи 'global_fr'.
    """
    logger.info("[CRON_JOB] Запущена задача обновления cache:global_fr...")
    start_time = time.time()
    redis_conn = await get_redis_connection()
    
    if not redis_conn:
        logger.error("[CRON_JOB] Не удалось подключиться к Redis. Обновление FR отменено.")
        return

    try:
        # 1. Получаем монеты
        logger.info("[CRON_JOB] 1/3: Запрос *всех* монет из API (coin-sifter)...")
        # --- ИЗМЕНЕНИЕ: Вызываем унифицированную функцию ---
        all_coins = await get_coins_func()
        # ---------------------------------------------------
        if not all_coins:
             logger.error("[CRON_JOB] 1/3: Не удалось получить монеты из API (None или []). Обновление FR отменено.")
             return
        logger.info(f"[CRON_JOB] 1/3: Получено {len(all_coins)} монет.")
        
        # 2. Собираем FR
        logger.info("[CRON_JOB] 2/3: Запуск сбора FR...")
        fr_data_dict = await fetch_funding_rates(all_coins)
        
        if not fr_data_dict:
             logger.warning("[CRON_JOB] 2/3: Сбор FR не вернул данных. Обновление FR завершено с предупреждением.")
             return
        
        logger.info("[CRON_JOB] 2/3: Сбор FR успешно завершен.")

        # 3. Сохранение
        logger.info("[CRON_JOB] 3/3: Сохранение данных в 'cache:global_fr'...")
        data_to_save = {
            "data": fr_data_dict, 
            "timeframe": "global_fr",
            "openTime": int(time.time() * 1000) # Текущий момент
        }
        await save_to_cache(redis_conn, 'global_fr', data_to_save)
        logger.info(f"[CRON_JOB] Успех! Обновление 'cache:global_fr' завершено за {time.time() - start_time:.2f} сек.")

    except Exception as e:
        logger.critical(f"[CRON_JOB] КРИТИЧЕСКАЯ ОШИБКА во время обновления FR: {e}", exc_info=True)
        
# --- Экспорт для __init__.py ---
async def get_global_fr_data():
    """
    Алиас для run_fr_update_process, который используется в __init__.py
    (Эта функция нужна, чтобы worker мог запустить CRON-Job)
    """
    await run_fr_update_process()
# -------------------------------