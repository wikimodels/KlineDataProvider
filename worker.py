import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict

# --- Импорты ---
from coin_source import get_coins_from_api
import data_collector
from data_collector.aggregation_8h import generate_and_save_8h_cache
from cache_manager import save_to_cache, redis_client, get_from_cache

# --- ИЗМЕНЕНИЕ: Импортируем 'run_fr_update_process' ---
try:
    from data_collector.fr_fetcher import run_fr_update_process
except ImportError:
    logging.critical("WORKER: Не удалось импортировать run_fr_update_process.")
    async def run_fr_update_process(): 
        logging.error("WORKER: Заглушка run_fr_update_process вызвана.")
# ----------------------------------------------------

# --- ИЗМЕНЕНИЕ: Импортируем data_processing для форматирования 4h ---
try:
    from data_collector import data_processing
except ImportError:
    logging.critical("WORKER: Не удалось импортировать data_processing.")
    # Фоллбэк
    class data_processing:
        @staticmethod
        def format_final_structure(data, coins, tf): return {}
# -----------------------------------------------------------------


# (Импорт config)
try:
    from config import REDIS_TASK_QUEUE_KEY, WORKER_LOCK_KEY, WORKER_LOCK_TIMEOUT_SECONDS
except ImportError:
    REDIS_TASK_QUEUE_KEY = "data_collector_task_queue"
    WORKER_LOCK_KEY = "data_collector_lock"
    WORKER_LOCK_TIMEOUT_SECONDS = 1800

# Инициализация логгера
logger = logging.getLogger(__name__)

# --- Константы ---
LOCK_RETRY_DELAY = 15
ERROR_RETRY_DELAY = 10

def _load_global_fr_cache() -> Optional[Dict[str, List[Dict]]]:
    """
    (Код не изменен)
    """
    logger.info("[WORKER] Загружаю 'cache:global_fr' из Redis...")
    try:
        fr_data = get_from_cache('global_fr')
        if not fr_data:
            logger.error("[WORKER] КРИТИЧЕСКАЯ ОШИБКА: 'cache:global_fr' не найден или пуст.")
            return None 
        logger.info(f"[WORKER] 'cache:global_fr' успешно загружен. (Записей: {len(fr_data)})")
        return fr_data
    except Exception as e:
        logger.error(f"[WORKER] Не удалось загрузить/распарсить 'cache:global_fr': {e}", exc_info=True)
        return None

async def _process_single_timeframe_task(timeframe: str, global_fr_data: Optional[Dict]):
    """
    (Код не изменен - он по-прежнему получает данные С форматированием)
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

    # 2. FR
    if global_fr_data is None:
         logger.warning(f"{log_prefix} WORKER: 'cache:global_fr' не загружен. Klines/OI будут собраны без FR.")

    # 3. Fetch Klines/OI (БЕЗ skip_formatting=True - форматирование включено)
    market_data = await data_collector.fetch_market_data(
        coins_from_api,
        timeframe,
        prefetched_fr_data=global_fr_data 
    )
    if not market_data or ("data" not in market_data):
        logger.error(f"{log_prefix} WORKER: Сборщик Klines/OI вернул некорректный ответ.")
        return

    # 4. (Обогащение удалено)

    # 5. Save
    save_to_cache(timeframe, market_data)
    logger.info(f"{log_prefix} WORKER: Задача {timeframe} успешно завершена.")


async def _process_4h_and_8h_task(global_fr_data: Optional[Dict]):
    """
    (Код ИЗМЕНЕН - разделяем поток 4h и 8h)
    """
    log_prefix = "[4H/8H]"
    logger.info(f"{log_prefix} WORKER: Начинаю специальную задачу (4h + 8h)...")

    # 1. Get coins
    logger.info(f"{log_prefix} WORKER: Запрашиваю ВСЕ монеты из API (coin_source)...")
    coins_from_api = await get_coins_from_api()
    if not coins_from_api:
        logger.warning(f"{log_prefix} WORKER: Из API не получено ни одной монеты. Задача (4h+8h) завершена.")
        return
    logger.info(f"{log_prefix} WORKER: Получено {len(coins_from_api)} монет.")
    
    # 2. FR
    if global_fr_data is None:
         logger.warning(f"{log_prefix} WORKER: 'cache:global_fr' не загружен. Klines/OI будут собраны без FR.")

    # --- ИЗМЕНЕНИЕ: 3. Fetch Klines/OI (СЫРЫЕ данные) ---
    master_market_data = await data_collector.fetch_market_data(
        coins_from_api, 
        '4h', 
        prefetched_fr_data=global_fr_data,
        skip_formatting=True # <-- 1. Получаем СЫРЫЕ (только слитые) данные
    )
    if not master_market_data: # (Проверяем на пустой dict, т.к. 'data' еще нет)
        logger.error(f"{log_prefix} WORKER: Сборщик Klines/OI (master) вернул некорректный ответ (None или {{}}).")
        return
    
    logger.info(f"{log_prefix} WORKER: Мастер-данные (Klines/OI/FR 4h, СЫРЫЕ) собраны. Начинаю 'раскидывать'...")

    # 4. "Раскидываем"
    try:
        # --- Процесс 8h (ПЕРВЫМ, использует СЫРЫЕ данные) ---
        logger.info("[8H] WORKER: Обрабатываю данные для '8h' (из сырых)...")
        # 2. Передаем СЫРЫЕ данные в 8h-агрегатор
        await generate_and_save_8h_cache(master_market_data, coins_from_api) 
        
        # --- Процесс 4h (ВТОРЫМ, форматируем и сохраняем) ---
        logger.info("[4H] WORKER: Обрабатываю данные для '4h' (форматирование)...")
        # 3. Форматируем 4h данные (здесь происходит обрезка неполной свечи)
        formatted_4h_data = data_processing.format_final_structure(
            master_market_data, coins_from_api, '4h'
        )
        
        # 4. Сохраняем 4h
        save_to_cache('4h', formatted_4h_data) 
        logger.info("[4H] WORKER: Данные 4h (для всех монет) успешно сохранены в cache:4h.")
        
        logger.info(f"{log_prefix} WORKER: Задача (4h + 8h) успешно завершена.")

    except Exception as e_split:
        logger.error(f"{log_prefix} WORKER: Ошибка на этапе 'раскидывания' данных: {e_split}", exc_info=True)
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---


# --- ИЗМЕНЕНИЕ: Обновляем роутер задач _process_task ---
async def _process_task(task_name: str):
    """
    Главный роутер задач. Вызывается из background_worker.
    Распознает 'fr' или таймфреймы.
    """
    log_prefix = f"[{task_name.upper()}]"
    
    if task_name == 'fr':
        # 1. Это задача сбора Фандинга
        logger.info(f"{log_prefix} WORKER: Получена задача обновления 'cache:global_fr'.")
        # (Эта функция сама все делает - собирает и сохраняет в кэш)
        await run_fr_update_process()
        logger.info(f"{log_prefix} WORKER: Задача 'fr' успешно завершена.")
        return

    # 2. Это задача сбора Klines/OI
    # (Загружаем FR кэш ТОЛЬКО для Klines/OI задач)
    global_fr_data = _load_global_fr_cache()
    
    if task_name == '4h':
        await _process_4h_and_8h_task(global_fr_data)
    elif task_name == '8h':
        logger.info(f"{log_prefix} WORKER: Задача 8h получена, но будет пропущена (генерируется задачей 4h).")
        return
    else:
        # (Обработает '1h', '12h', '1d')
        await _process_single_timeframe_task(task_name, global_fr_data)
# ----------------------------------------------------
            
            
async def background_worker():
    """
    Основной цикл воркера.
    Берет задачу (timeframe или 'fr') из Redis (LPOP), получает блокировку, 
    вызывает роутер _process_task.
    (Код этой функции не изменен)
    """
    task_name: str = "" # (Переименовал timeframe в task_name)
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
            
            task_name = task_data
            log_prefix = f"[{task_name.upper()}]"
            lock_acquired = False
            start_time = time.time()

            # --- Блокировка ---
            if not redis_client:
                logger.warning(f"{log_prefix} WORKER: Redis недоступен...")
                await asyncio.sleep(ERROR_RETRY_DELAY)
                redis_client.rpush(REDIS_TASK_QUEUE_KEY, task_name)
                continue

            lock_acquired = redis_client.set(
                WORKER_LOCK_KEY,
                f"busy_by_{task_name}_at_{int(start_time)}",
                nx=True,
                ex=WORKER_LOCK_TIMEOUT_SECONDS
            )

            if not lock_acquired:
                logger.warning(f"{log_prefix} WORKER: Сборщик занят (lock). Возвращаю задачу в очередь...")
                await asyncio.sleep(LOCK_RETRY_DELAY)
                redis_client.rpush(REDIS_TASK_QUEUE_KEY, task_name)
                continue

            logger.info(f"{log_prefix} WORKER: Блокировка получена. Начинаю задачу.")

            # --- Вызов Роутера Задач ---
            await _process_task(task_name) # <-- Передаем task_name
            # -------------------------
            
            end_time = time.time()
            logger.info(f"{log_prefix} WORKER: Весь процесс для задачи {task_name} занял {end_time - start_time:.2f} сек.")

        except Exception as e:
            logger.error(f"{log_prefix} WORKER: КРИТИЧЕСКАЯ ОШИБКА в цикле: {e}", exc_info=True)
            if not task_name:
                await asyncio.sleep(ERROR_RETRY_DELAY)

        finally:
            # --- Освобождение блокировки ---
            if lock_acquired and redis_client:
                 logger.info(f"{log_prefix} WORKER: Освобождаю блокировку.")
                 try:
                     redis_client.delete(WORKER_LOCK_KEY)
                 except Exception as redis_err:
                      logger.error(f"{log_prefix} WORKER: Ошибка при удалении блокировки Redis: {redis_err}")
            
            task_name = ""
            log_prefix = "[WORKER]"
            lock_acquired = False