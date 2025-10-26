import asyncio
import logging
import time
import copy
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict

# --- Импорты ---
from database import get_coins_from_db
import data_collector
# (data_collector.task_builder больше не нужен в этом файле)
from data_collector.data_processing import generate_and_save_8h_cache, _enrich_coin_metadata
# --- Изменение №1: Импортируем get_from_cache ---
from cache_manager import save_to_cache, redis_client, get_from_cache
# -----------------------------------------------

# Инициализация логгера
logger = logging.getLogger(__name__)

# --- Очередь Задач ---
task_queue = asyncio.Queue()

# --- Константы ---
WORKER_LOCK_KEY = "data_collector_lock"
WORKER_LOCK_TIMEOUT = 1800 # 30 минут
LOCK_RETRY_DELAY = 15
ERROR_RETRY_DELAY = 10
# (FR_CONCURRENCY_LIMIT удален)

# --- Изменение №1: Функция fetch_funding_rates() ПОЛНОСТЬЮ УДАЛЕНА ---
# (Она теперь находится в data_collector/fr_fetcher.py)
# -------------------------------------------------------------------


# --- Изменение №1: Новая helper-функция для загрузки кэша FR ---
def _load_global_fr_cache() -> Optional[Dict[str, List[Dict]]]:
    """
    Загружает и декодирует 'cache:global_fr' из Redis.
    """
    logger.info("[WORKER] Загружаю 'cache:global_fr' из Redis...")
    try:
        # get_from_cache (из cache_manager) выполняет всю работу
        # (получение, base64_decode, gzip_decompress, json_loads)
        fr_data = get_from_cache('global_fr')
        
        if not fr_data:
            logger.error("[WORKER] КРИТИЧЕСКАЯ ОШИБКА: 'cache:global_fr' не найден или пуст. "
                         "Klines/OI будут собраны БЕЗ ФАНДИНГА. "
                         "Убедитесь, что cron-job (/update-fr) работает или сервер был перезапущен.")
            return None # Возвращаем None, если кэш пуст

        logger.info(f"[WORKER] 'cache:global_fr' успешно загружен. (Записей: {len(fr_data)})")
        return fr_data

    except Exception as e:
        logger.error(f"[WORKER] Не удалось загрузить/распарсить 'cache:global_fr': {e}", exc_info=True)
        return None
# -------------------------------------------------------------------


# --- (Helper-функция _filter_market_data не изменена) ---
def _filter_market_data(master_data: Dict[str, Any], coin_list: List[Dict]) -> Dict[str, Any]:
    """
    Фильтрует 'data' в market_data, оставляя только монеты из coin_list.
    Использует deepcopy, чтобы не изменять master_data.
    """
    if not master_data.get('data') or not coin_list:
        return master_data

    symbols_to_keep = {c['symbol'].split(':')[0] for c in coin_list}
    
    filtered_data = copy.deepcopy(master_data) 
    
    filtered_data['data'] = [
        coin_data for coin_data in filtered_data['data'] 
        if coin_data.get('symbol') in symbols_to_keep
    ]
    return filtered_data


# --- (Функция _process_single_timeframe_task ОБНОВЛЕНА) ---
async def _process_single_timeframe_task(timeframe: str, global_fr_data: Optional[Dict]):
    """
    Обрабатывает стандартную, "одиночную" задачу (1h, 12h, 1d).
    (Теперь ПРИНИМАЕТ global_fr_data и НЕ вызывает fetch_funding_rates)
    """
    log_prefix = f"[{timeframe.upper()}]"
    logger.info(f"{log_prefix} WORKER: Начинаю стандартную задачу...")
    
    # 1. Get coins (без изменений)
    logger.info(f"{log_prefix} WORKER: Запрашиваю метаданные из БД для '{timeframe}'...")
    coins_from_db = get_coins_from_db(timeframe)
    if not coins_from_db:
         logger.warning(f"{log_prefix} WORKER: Из БД не получено ни одной монеты для '{timeframe}'. Задача завершена.")
         return
    logger.info(f"{log_prefix} WORKER: Получено {len(coins_from_db)} монет.")

    # 2. Fetch FR (УДАЛЕНО)
    # (global_fr_data уже получен)
    if global_fr_data is None:
         logger.warning(f"{log_prefix} WORKER: 'cache:global_fr' не загружен. Klines/OI будут собраны без FR.")
         # (Продолжаем, а не возвращаем None)

    # 3. Fetch Klines/OI (без изменений, но передаем prefetched_fr_data)
    market_data = await data_collector.fetch_market_data(
        coins_from_db,
        timeframe,
        prefetched_fr_data=global_fr_data # --- Изменение №1 ---
    )
    if not market_data or ("data" not in market_data):
        logger.error(f"{log_prefix} WORKER: Сборщик Klines/OI вернул некорректный ответ.")
        return

    # 4. Enrich (без изменений)
    logger.info(f"{log_prefix} WORKER: Начинаю обогащение метаданных...")
    final_enriched_data = _enrich_coin_metadata(market_data, coins_from_db)
    logger.info(f"{log_prefix} WORKER: Обогащение метаданных завершено.")

    # 5. Save (без изменений)
    save_to_cache(timeframe, final_enriched_data)
    logger.info(f"{log_prefix} WORKER: Задача {timeframe} успешно завершена.")


# --- (Функция _process_4h_and_8h_task ОБНОВЛЕНА) ---
async def _process_4h_and_8h_task(global_fr_data: Optional[Dict]):
    """
    Обрабатывает специальную задачу 4h, которая также генерирует 8h.
    (Теперь ПРИНИМАЕТ global_fr_data и НЕ вызывает fetch_funding_rates)
    """
    log_prefix = "[4H/8H]"
    logger.info(f"{log_prefix} WORKER: Начинаю специальную задачу (4h + 8h)...")

    # 1. Get coins 4h & 8h (без изменений)
    logger.info(f"{log_prefix} WORKER: Запрашиваю список монет '4h'...")
    coins_4h = get_coins_from_db('4h')
    logger.info(f"{log_prefix} WORKER: Запрашиваю список монет '8h'...")
    coins_8h = get_coins_from_db('8h')

    if not coins_4h and not coins_8h:
        logger.warning(f"{log_prefix} WORKER: Из БД не получено ни одной монеты (ни 4h, ни 8h). Задача завершена.")
        return

    # 2. Combine lists (без изменений)
    list_4h = coins_4h or []
    list_8h = coins_8h or []
    combined_map = {c['symbol'].split(':')[0]: c for c in list_4h}
    combined_map.update({c['symbol'].split(':')[0]: c for c in list_8h})
    combined_coin_list = list(combined_map.values())
    
    logger.info(f"{log_prefix} WORKER: Списки монет объединены. 4h: {len(list_4h)}, 8h: {len(list_8h)}, Итого: {len(combined_coin_list)}.")
    if not combined_coin_list:
         logger.warning(f"{log_prefix} WORKER: Итоговый список монет пуст. Задача завершена.")
         return

    # 3. Fetch FR (УДАЛЕНО)
    if global_fr_data is None:
         logger.warning(f"{log_prefix} WORKER: 'cache:global_fr' не загружен. Klines/OI будут собраны без FR.")
         # (Продолжаем)

    # 4. Fetch Klines/OI (без изменений, но передаем prefetched_fr_data)
    master_market_data = await data_collector.fetch_market_data(
        combined_coin_list,
        '4h', 
        prefetched_fr_data=global_fr_data # --- Изменение №1 ---
    )
    if not master_market_data or ("data" not in master_market_data):
        logger.error(f"{log_prefix} WORKER: Сборщик Klines/OI (master) вернул некорректный ответ.")
        return
    
    logger.info(f"{log_prefix} WORKER: Мастер-данные (Klines/OI/FR 4h) собраны. Начинаю 'раскидывать'...")

    # 5. "Раскидываем" (без изменений)
    try:
        # --- Процесс 4h ---
        if list_4h:
            logger.info("[4H] WORKER: Обрабатываю данные для '4h'...")
            data_for_4h = _filter_market_data(master_market_data, list_4h)
            enriched_data_4h = _enrich_coin_metadata(data_for_4h, list_4h)
            save_to_cache('4h', enriched_data_4h)
            logger.info("[4H] WORKER: Данные 4h успешно сохранены в cache:4h.")
        else:
            logger.warning("[4H] WORKER: Список монет 4h пуст, 'cache:4h' не будет создан.")

        # --- Процесс 8h ---
        if list_8h:
            logger.info("[8H] WORKER: Обрабатываю данные для '8h'...")
            data_for_8h_as_4h = _filter_market_data(master_market_data, list_8h)
            enriched_data_for_8h_as_4h = _enrich_coin_metadata(data_for_8h_as_4h, list_8h)
            # --- Изменение №1: Передаем list_8h в generate_and_save_8h_cache ---
            await generate_and_save_8h_cache(enriched_data_for_8h_as_4h, list_8h)
            # -----------------------------------------------------------------
        else:
            logger.warning("[8H] WORKER: Список монет 8h пуст, 'cache:8h' не будет создан.")
        
        logger.info(f"{log_prefix} WORKER: Задача (4h + 8h) успешно завершена.")

    except Exception as e_split:
        logger.error(f"{log_prefix} WORKER: Ошибка на этапе 'раскидывания' данных: {e_split}", exc_info=True)


# --- (Роутер _process_task ОБНОВЛЕН) ---
async def _process_task(timeframe: str):
    """
    Главный роутер задач. Вызывается из background_worker.
    (Теперь СНАЧАЛА загружает global_fr_data)
    """
    log_prefix = f"[{timeframe.upper()}]"
    
    # --- Изменение №1: Загружаем FR кэш ПЕРЕД обработкой задачи ---
    global_fr_data = _load_global_fr_cache()
    # -------------------------------------------------------------
    
    if timeframe == '4h':
        await _process_4h_and_8h_task(global_fr_data)
    elif timeframe == '8h':
        logger.info(f"{log_prefix} WORKER: Задача 8h получена, но будет пропущена (генерируется задачей 4h).")
        return
    else:
        await _process_single_timeframe_task(timeframe, global_fr_data)
            
            
# --- (Цикл background_worker не изменен) ---
async def background_worker():
    """
    Основной цикл воркера.
    Берет задачу, получает блокировку, вызывает роутер _process_task.
    (Код этой функции не изменен)
    """
    while True:
        timeframe: str = await task_queue.get()
        log_prefix = f"[{timeframe.upper()}]"
        lock_acquired = False
        start_time = time.time()

        try:
            # --- Блокировка ---
            if not redis_client:
                logger.warning(f"{log_prefix} WORKER: Redis недоступен...")
                await asyncio.sleep(ERROR_RETRY_DELAY)
                await task_queue.put(timeframe)
                task_queue.task_done()
                continue

            lock_acquired = redis_client.set(
                WORKER_LOCK_KEY,
                f"busy_by_{timeframe}_at_{int(start_time)}",
                nx=True,
                ex=WORKER_LOCK_TIMEOUT
            )

            if not lock_acquired:
                await asyncio.sleep(LOCK_RETRY_DELAY)
                await task_queue.put(timeframe)
                task_queue.task_done()
                continue

            logger.info(f"{log_prefix} WORKER: Блокировка получена. Начинаю задачу.")

            # --- Вызов Роутера Задач ---
            await _process_task(timeframe)
            # -------------------------
            
            end_time = time.time()
            logger.info(f"{log_prefix} WORKER: Весь процесс для задачи {timeframe} занял {end_time - start_time:.2f} сек.")


        except Exception as e:
            logger.error(f"{log_prefix} WORKER: КРИТИЧЕСКАЯ ОШИБКА в цикле (lock/route): {e}", exc_info=True)

        finally:
            # --- Освобождение блокировки ---
            if lock_acquired and redis_client:
                 logger.info(f"{log_prefix} WORKER: Освобождаю блокировку.")
                 try:
                     redis_client.delete(WORKER_LOCK_KEY)
                 except Exception as redis_err:
                      logger.error(f"{log_prefix} WORKER: Ошибка при удалении блокировки Redis: {redis_err}")

            task_queue.task_done()
            await asyncio.sleep(0.1)

