import asyncio
import logging
import time
import copy # <-- ЭТОТ ИМПОРТ БОЛЬШЕ НЕ НУЖЕН
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict

# --- Импорты ---
# --- ИЗМЕНЕНИЕ: Заменяем database на coin_source ---
from coin_source import get_coins_from_api
# (Удален 'from database import get_coins_from_db')
# ----------------------------------------------------

import data_collector

# --- ИЗМЕНЕНИЕ: Убираем импорт _enrich_coin_metadata ---
# (from data_collector.data_processing import _enrich_coin_metadata)
# --------------------------------------------------------

# (Импорт aggregation_8h остается)
from data_collector.aggregation_8h import generate_and_save_8h_cache
from cache_manager import save_to_cache, redis_client, get_from_cache

# (Импорт config)
try:
    from config import REDIS_TASK_QUEUE_KEY, WORKER_LOCK_KEY, WORKER_LOCK_TIMEOUT_SECONDS
except ImportError:
    # Фоллбэки
    REDIS_TASK_QUEUE_KEY = "data_collector_task_queue"
    WORKER_LOCK_KEY = "data_collector_lock"
    WORKER_LOCK_TIMEOUT_SECONDS = 1800

# Инициализация логгера
logger = logging.getLogger(__name__)

# --- Константы ---
LOCK_RETRY_DELAY = 15
ERROR_RETRY_DELAY = 10

# --- (Функция _load_global_fr_cache не изменена) ---
def _load_global_fr_cache() -> Optional[Dict[str, List[Dict]]]:
    """
    Загружает и декодирует 'cache:global_fr' из Redis.
    """
    logger.info("[WORKER] Загружаю 'cache:global_fr' из Redis...")
    try:
        fr_data = get_from_cache('global_fr')
        
        if not fr_data:
            logger.error("[WORKER] КРИТИЧЕСКАЯ ОШИБКА: 'cache:global_fr' не найден или пуст. "
                         "Klines/OI будут собраны БЕЗ ФАНДИНГА.")
            return None 

        logger.info(f"[WORKER] 'cache:global_fr' успешно загружен. (Записей: {len(fr_data)})")
        return fr_data

    except Exception as e:
        logger.error(f"[WORKER] Не удалось загрузить/распарсить 'cache:global_fr': {e}", exc_info=True)
        return None

# --- (Функция _process_single_timeframe_task ИСПРАВЛЕНА) ---
async def _process_single_timeframe_task(timeframe: str, global_fr_data: Optional[Dict]):
    """
    Обрабатывает стандартную задачу (1h, 12h, 1d).
    Запрашивает ВСЕ монеты из API.
    """
    log_prefix = f"[{timeframe.upper()}]"
    logger.info(f"{log_prefix} WORKER: Начинаю стандартную задачу...")
    
    # 1. Get coins (из API)
    logger.info(f"{log_prefix} WORKER: Запрашиваю ВСЕ монеты из API (coin_source)...")
    coins_from_api = await get_coins_from_api()
    if not coins_from_api:
         logger.warning(f"{log_prefix} WORKER: Из API не получено ни одной монеты. Задача завершена.")
         return
    logger.info(f"{log_prefix} WORKER: Получено {len(coins_from_api)} монет.")

    # 2. FR (без изменений)
    if global_fr_data is None:
         logger.warning(f"{log_prefix} WORKER: 'cache:global_fr' не загружен. Klines/OI будут собраны без FR.")

    # 3. Fetch Klines/OI
    market_data = await data_collector.fetch_market_data(
        coins_from_api, # <-- Передаем монеты из API
        timeframe,
        prefetched_fr_data=global_fr_data 
    )
    if not market_data or ("data" not in market_data):
        logger.error(f"{log_prefix} WORKER: Сборщик Klines/OI вернул некорректный ответ.")
        return

    # 4. --- ОБОГАЩЕНИЕ УДАЛЕНО ---
    # logger.info(f"{log_prefix} WORKER: Начинаю обогащение метаданных...")
    # final_enriched_data = _enrich_coin_metadata(market_data, coins_from_api)
    # logger.info(f"{log_prefix} WORKER: Обогащение метаданных завершено.")
    # -----------------------------

    # 5. Save (сохраняем 'market_data' напрямую)
    save_to_cache(timeframe, market_data)
    logger.info(f"{log_prefix} WORKER: Задача {timeframe} успешно завершена.")


# --- (Функция _process_4h_and_8h_task ИСПРАВЛЕНА) ---
async def _process_4h_and_8h_task(global_fr_data: Optional[Dict]):
    """
    (ИСПРАВЛЕНО)
    Обрабатывает специальную задачу 4h, которая также генерирует 8h.
    Запрашивает ВСЕ монеты ОДИН РАЗ из API.
    """
    log_prefix = "[4H/8H]"
    logger.info(f"{log_prefix} WORKER: Начинаю специальную задачу (4h + 8h)...")

    # 1. Get coins (ОДИН РАЗ из API)
    logger.info(f"{log_prefix} WORKER: Запрашиваю ВСЕ монеты из API (coin_source)...")
    coins_from_api = await get_coins_from_api()
    
    if not coins_from_api:
        logger.warning(f"{log_prefix} WORKER: Из API не получено ни одной монеты. Задача (4h+8h) завершена.")
        return
    logger.info(f"{log_prefix} WORKER: Получено {len(coins_from_api)} монет.")
    
    # 2. FR (без изменений)
    if global_fr_data is None:
         logger.warning(f"{log_prefix} WORKER: 'cache:global_fr' не загружен. Klines/OI будут собраны без FR.")

    # 3. Fetch Klines/OI (собираем данные 4h для всех монет)
    master_market_data = await data_collector.fetch_market_data(
        coins_from_api, # Используем единый список
        '4h', 
        prefetched_fr_data=global_fr_data 
    )
    if not master_market_data or ("data" not in master_market_data):
        logger.error(f"{log_prefix} WORKER: Сборщик Klines/OI (master) вернул некорректный ответ.")
        return
    
    logger.info(f"{log_prefix} WORKER: Мастер-данные (Klines/OI/FR 4h) собраны. Начинаю 'раскидывать'...")

    # 4. "Раскидываем"
    try:
        # --- Процесс 4h ---
        logger.info("[4H] WORKER: Обрабатываю данные для '4h'...")
        # --- ОБОГАЩЕНИЕ УДАЛЕНО ---
        # enriched_data_4h = _enrich_coin_metadata(master_market_data, coins_from_api)
        # save_to_cache('4h', enriched_data_4h)
        # ---------------------------
        save_to_cache('4h', master_market_data) # Сохраняем "как есть"
        logger.info("[4H] WORKER: Данные 4h (для всех монет) успешно сохранены в cache:4h.")

        # --- Процесс 8h ---
        logger.info("[8H] WORKER: Обрабатываю данные для '8h'...")
        # (Передаем master_market_data (НЕ обогащенные) и список монет)
        await generate_and_save_8h_cache(master_market_data, coins_from_api)
        
        logger.info(f"{log_prefix} WORKER: Задача (4h + 8h) успешно завершена.")

    except Exception as e_split:
        logger.error(f"{log_prefix} WORKER: Ошибка на этапе 'раскидывания' данных: {e_split}", exc_info=True)


# --- (Роутер _process_task не изменен) ---
async def _process_task(timeframe: str):
    """
    Главный роутер задач. Вызывается из background_worker.
    """
    log_prefix = f"[{timeframe.upper()}]"
    
    global_fr_data = _load_global_fr_cache()
    
    if timeframe == '4h':
        await _process_4h_and_8h_task(global_fr_data)
    elif timeframe == '8h':
        logger.info(f"{log_prefix} WORKER: Задача 8h получена, но будет пропущена (генерируется задачей 4h).")
        return
    else:
        await _process_single_timeframe_task(timeframe, global_fr_data)
            
            
async def background_worker():
    """
    Основной цикл воркера.
    Берет задачу из Redis (LPOP), получает блокировку, вызывает роутер _process_task.
    (Использует константы из config)
    """
    timeframe: str = ""
    log_prefix = "[WORKER]"
    lock_acquired = False
    
    while True:
        try:
            task_data = redis_client.lpop(REDIS_TASK_QUEUE_KEY)

            if not task_data:
                log_prefix = "[WORKER]"
                logger.info(f"{log_prefix} Ожидаю новую задачу из Redis-очереди ('{REDIS_TASK_QUEUE_KEY}')...")
                await asyncio.sleep(LOCK_RETRY_DELAY)
                continue
            
            timeframe = task_data
            log_prefix = f"[{timeframe.upper()}]"
            lock_acquired = False
            start_time = time.time()

            # --- Блокировка ---
            if not redis_client:
                logger.warning(f"{log_prefix} WORKER: Redis недоступен...")
                await asyncio.sleep(ERROR_RETRY_DELAY)
                redis_client.rpush(REDIS_TASK_QUEUE_KEY, timeframe)
                continue

            lock_acquired = redis_client.set(
                WORKER_LOCK_KEY,
                f"busy_by_{timeframe}_at_{int(start_time)}",
                nx=True,
                ex=WORKER_LOCK_TIMEOUT_SECONDS # Используем константу из config
            )

            if not lock_acquired:
                logger.warning(f"{log_prefix} WORKER: Сборщик занят (lock). Возвращаю задачу в очередь...")
                await asyncio.sleep(LOCK_RETRY_DELAY)
                redis_client.rpush(REDIS_TASK_QUEUE_KEY, timeframe)
                continue

            logger.info(f"{log_prefix} WORKER: Блокировка получена. Начинаю задачу.")

            # --- Вызов Роутера Задач ---
            await _process_task(timeframe)
            # -------------------------
            
            end_time = time.time()
            logger.info(f"{log_prefix} WORKER: Весь процесс для задачи {timeframe} занял {end_time - start_time:.2f} сек.")


        except Exception as e:
            logger.error(f"{log_prefix} WORKER: КРИТИЧЕСКАЯ ОШИБКА в цикле: {e}", exc_info=True)
            if not timeframe:
                await asyncio.sleep(ERROR_RETRY_DELAY)


        finally:
            # --- Освобождение блокировки ---
            if lock_acquired and redis_client:
                 logger.info(f"{log_prefix} WORKER: Освобождаю блокировку.")
                 try:
                     redis_client.delete(WORKER_LOCK_KEY)
                 except Exception as redis_err:
                      logger.error(f"{log_prefix} WORKER: Ошибка при удалении блокировки Redis: {redis_err}")
            
            timeframe = ""
            log_prefix = "[WORKER]"
            lock_acquired = False