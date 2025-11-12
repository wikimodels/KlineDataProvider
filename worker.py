import logging
from typing import Dict, Any, Optional, List
from collections import defaultdict
import asyncio
import aiohttp
import json
import time
import os 
from redis.asyncio import Redis as AsyncRedis 

# --- Импорты из config ---
try:
    from config import (
        REDIS_TASK_QUEUE_KEY,
        WORKER_LOCK_KEY,
        WORKER_LOCK_TIMEOUT_SECONDS,
        WORKER_LOCK_VALUE, 
        ALLOWED_CACHE_KEYS,
        TG_BOT_TOKEN_KEY,
        TG_USER_KEY,
    )
except ImportError:
    # Фоллбэки
    REDIS_TASK_QUEUE_KEY = "data_collector_task_queue"
    WORKER_LOCK_KEY = "data_collector_lock"
    WORKER_LOCK_TIMEOUT_SECONDS = 1800
    WORKER_LOCK_VALUE = "processing"
    ALLOWED_CACHE_KEYS = ['1h', '4h', '8h', '12h', '1d', 'global_fr']
    TG_BOT_TOKEN_KEY = os.environ.get("TG_BOT_TOKEN")
    TG_USER_KEY = os.environ.get("TG_USER")


# --- Импорты из cache_manager ---
from cache_manager import (
    check_redis_health,
    clear_queue,
    get_redis_connection,
    load_from_cache,
    save_to_cache, 
)

# --- Импорты других модулей проекта ---
try:
    # --- ИЗМЕНЕНИЕ №1: Используем абсолютные импорты от корня ---
    from data_collector import fetch_market_data
    from data_collector.aggregation_8h import generate_and_save_8h_cache
    from data_collector.logging_setup import logger
    from data_collector.coin_source import get_coins as get_all_symbols
    
    # --- ИЗМЕНЕНИЕ №1: Исправляем импорт FR ---
    from data_collector import get_global_fr_data 
    
    # --- ИЗМЕНЕНИЕ №1: Импорт Alert Manager (абсолютный) ---
    from alert_manager.storage import AlertStorage
    from alert_manager.checker import run_alert_checks
    # --- КОНЕЦ ИЗМЕНЕНИЯ №1 ---
    
except ImportError as e: # --- Добавил 'e' для дебага ---
    logger = logging.getLogger(__name__)
    # --- ОБНОВЛЕНО: Логгируем саму ошибку импорта ---
    logger.error(f"Не удалось импортировать зависимости: {e}", exc_info=True)
    
    async def fetch_market_data(coins, timeframe): 
        logger.error("Mock: Не удалось запустить fetch_market_data.")
        return {}
    async def generate_and_save_8h_cache(data_4h, coins): 
        logger.error("Mock: Не удалось запустить generate_and_save_8h_cache.")
        pass
    async def get_all_symbols(): 
        logger.error("Mock: Не удалось запустить get_all_symbols.")
        return []
            
    # Заглушка для fr_fetcher
    async def get_global_fr_data(): # --- ИЗМЕНЕНИЕ №1 (Заглушка) ---
        logger.error("Mock: Не удалось запустить get_global_fr_data. Зависимость fr_fetcher недоступна.")
    
    # Заглушка для Alert Manager
    class AlertStorage:
        def __init__(self, r): pass
    async def run_alert_checks(data, storage):
        logger.error("Mock: Не удалось запустить run_alert_checks.")
        pass


# --- КОНСТАНТЫ ВОЗВРАТА ---
WORKER_RETRY_DELAY = 2  # Проверка очереди каждые 2 секунды
FR_UPDATE_FREQUENCY_SECONDS = 1800 # 30 минут


async def _get_and_process_task_from_queue(redis_conn: AsyncRedis) -> bool:
    """
    Вынимает и обрабатывает одну задачу из очереди.
    """
    
    # 1. Получаем задачу
    logger.info(f"[TASK_PROCESSOR] >>> Попытка получить задачу из очереди '{REDIS_TASK_QUEUE_KEY}'...")
    task_json = await redis_conn.lpop(REDIS_TASK_QUEUE_KEY)
    
    if not task_json:
        logger.info(f"[TASK_PROCESSOR] <<< Очередь пуста (lpop вернул None).")
        return False 
    
    if isinstance(task_json, bytes):
        task_json = task_json.decode('utf-8')

    logger.info(f"[TASK_PROCESSOR] Получена задача (JSON): {task_json}")
        
    try:
        task_payload = json.loads(task_json)
        logger.info(f"[TASK_PROCESSOR] Задача декодирована: {task_payload}")
    except json.JSONDecodeError as e:
        logger.error(f"[TASK_PROCESSOR] ❌ Не удалось декодировать JSON задачи: {task_json}. Ошибка: {e}")
        return True 

    timeframe = task_payload.get("timeframe")
    
    if not timeframe:
        logger.error(f"[TASK_PROCESSOR] ❌ Задача не содержит 'timeframe': {task_payload}")
        return True

    log_prefix = f"[WORKER:{timeframe.upper()}]"
    logger.info(f"{log_prefix} 🔥 Начинаю обработку задачи: {task_payload}")

    # 2. Обрабатываем задачу FR
    if timeframe == 'global_fr':
        try:
            logger.info(f"{log_prefix} Запуск обновления 'cache:global_fr' через get_global_fr_data()...")
            # --- ИЗМЕНЕНИЕ №2: Исправляем вызов ---
            await get_global_fr_data()
        except Exception as e:
            logger.error(f"{log_prefix} ❌ Ошибка при обновлении FR: {e}", exc_info=True)
        return True

    # --- ИЗМЕНЕНИЕ №3: Инициализируем AlertStorage ---
    # (Он нужен для `run_alert_checks`, который вызывается для '1h')
    storage = AlertStorage(redis_conn)
    # --- КОНЕЦ ИЗМЕНЕНИЯ №3 ---

    # 3. Обрабатываем Klines/OI
    
    final_data: Optional[Dict[str, Any]] = None
    
    try:
        # Получаем список монет
        logger.info(f"{log_prefix} Запрашиваю список монет через get_all_symbols()...")
        all_coins = await get_all_symbols()
        logger.info(f"{log_prefix} Получено монет: {len(all_coins) if all_coins else 0}")
        
        if not all_coins:
            logger.error(f"{log_prefix} ❌ Не удалось получить список монет. all_coins = {all_coins}")
            return True
        
        # --- (ОРИГИНАЛЬНАЯ ЛОГИКА 8h (399 -> 199) - СОХРАНЕНА) ---
        if timeframe == '8h':
            logger.info(f"{log_prefix} Проверка зависимости: загрузка 'cache:4h'...")
            data_4h = await load_from_cache('4h', redis_conn=redis_conn)
            
            if not data_4h or not data_4h.get('data'):
                logger.warning(f"{log_prefix} ⚠️ Зависимость: Отсутствуют или пусты данные 'cache:4h'. Агрегация 8h невозможна.")
                logger.info(f"{log_prefix} Возвращаю задачу '8h' обратно в очередь (конец)...")
                await redis_conn.rpush(REDIS_TASK_QUEUE_KEY, json.dumps(task_payload)) 
                return True
            
            logger.info(f"{log_prefix} Запуск агрегации 4h->8h...")
            
            # (Передаем 399 свечей 4h, чтобы получить ~199 свечей 8h)
            await generate_and_save_8h_cache(data_4h.get('data'), all_coins)
            
            logger.info(f"{log_prefix} Агрегация 4h->8h завершена.")
        else:
            # (Обычный путь для 1h, 4h, 12h, 1d)
            logger.info(f"{log_prefix} Запуск fetch_market_data()...")
            klines_data = await fetch_market_data(all_coins, timeframe)
            logger.info(f"{log_prefix} fetch_market_data() завершён.")
            
            if not klines_data:
                logger.warning(f"{log_prefix} ⚠️ Не получено данных Klines для {timeframe}.")
                return True
                
            final_data = klines_data
            
    except Exception as e:
        logger.error(f"{log_prefix} ❌ Критическая ошибка при сборе данных: {e}", exc_info=True)
        logger.info(f"{log_prefix} Возвращаю задачу обратно в очередь (из-за ошибки)...")
        await redis_conn.rpush(REDIS_TASK_QUEUE_KEY, json.dumps(task_payload))
        return True


    # 4. Сохранение в кэш
    if final_data: # (Для 8h это будет False, что корректно)
        try:
            logger.info(f"{log_prefix} Сохранение данных в 'cache:{timeframe}'...")
            await save_to_cache(redis_conn, timeframe, final_data)
            logger.info(f"{log_prefix} ✅ Данные успешно сохранены в кэш.")
            
            # --- ИЗМЕНЕНИЕ №3: "Включаем" проверку алертов (только для 1h) ---
            if timeframe == '1h':
                try:
                    logger.info(f"{log_prefix} 🚀 Запуск проверки алертов (Line/VWAP)...")
                    # Передаем 'final_data' (это 'cache_data') и 'storage'
                    await run_alert_checks(final_data, storage)
                except Exception as e:
                    # (Ловим ошибку здесь, чтобы она не сломала основной цикл воркера)
                    logger.error(f"{log_prefix} 💥 Ошибка во время проверки алертов: {e}", exc_info=True)
            # --- КОНЕЦ ИЗМЕНЕНИЯ №3 ---
            
        except Exception as e:
            logger.error(f"{log_prefix} ❌ Ошибка при СОХРАНЕНИИ в кэш: {e}", exc_info=True)
            logger.info(f"{log_prefix} Возвращаю задачу обратно в очередь (ошибка сохранения)...")
            await redis_conn.rpush(REDIS_TASK_QUEUE_KEY, json.dumps(task_payload))
            return True
            
    else:
        if timeframe != '8h':
            logger.warning(f"{log_prefix} ⚠️ final_data пустой. Ничего не сохранено в кэш.")
        else:
            logger.info(f"{log_prefix} ✅ Кэш '8h' сохранен внутри generate_and_save_8h_cache. Пропускаю дублирующее сохранение.")


    logger.info(f"{log_prefix} 🎉 Задача '{timeframe}' полностью обработана.")
    return True


async def background_worker():
    """
    Основной асинхронный цикл для обработки очереди задач Redis.
    """
    logger.info("[MAIN_WORKER] 🚀 Запускаю главный цикл воркера...")
    
    # 1. Проверка здоровья Redis
    logger.info("[MAIN_WORKER] Проверка доступности Redis...")
    if not await check_redis_health():
        logger.critical("[MAIN_WORKER] ❌ Redis недоступен. Воркер не запущен.")
        return
    logger.info("[MAIN_WORKER] ✅ Redis доступен.")

    # 2. Получение соединения 
    logger.info("[MAIN_WORKER] Получение соединения с Redis...")
    redis_conn = await get_redis_connection()
    if not redis_conn:
        logger.critical("[MAIN_WORKER] ❌ Не удалось получить соединение с Redis.")
        return
    logger.info("[MAIN_WORKER] ✅ Соединение с Redis установлено.")
    
    logger.info(f"[MAIN_WORKER] ✅ Воркер готов к работе. Начинаю мониторинг очереди '{REDIS_TASK_QUEUE_KEY}'...")
    logger.info(f"[MAIN_WORKER] 🔑 Lock Key: '{WORKER_LOCK_KEY}', Lock Value: '{WORKER_LOCK_VALUE}'")
    logger.info(f"[MAIN_WORKER] ⏱️  Lock Timeout: {WORKER_LOCK_TIMEOUT_SECONDS} сек, Retry Delay: {WORKER_RETRY_DELAY} сек")
        
    iteration = 0
    while True:
        iteration += 1
        logger.info(f"\n[MAIN_WORKER] ==================== ИТЕРАЦИЯ #{iteration} ====================")
        
        try:
            # 4. Проверка и установка блокировки
            logger.info(f"[MAIN_WORKER] 🔍 Проверяю статус блокировки '{WORKER_LOCK_KEY}'...")
            lock_status_bytes = await redis_conn.get(WORKER_LOCK_KEY)
            
            lock_status = lock_status_bytes.decode('utf-8') if lock_status_bytes else None
            
            logger.info(f"[MAIN_WORKER] 🔑 Lock Status: {lock_status} (ожидаемое значение: '{WORKER_LOCK_VALUE}')")
            
            if lock_status and lock_status == WORKER_LOCK_VALUE:
                logger.info(f"[MAIN_WORKER] ⏸️  Воркер занят (Lock установлен). Жду {WORKER_RETRY_DELAY} сек...")
            else:
                logger.info(f"[MAIN_WORKER] 🟢 Lock свободен. Попытка установить блокировку...")
                
                # 5. Установка блокировки и обработка задач
                lock_set = await redis_conn.set(
                    WORKER_LOCK_KEY, 
                    WORKER_LOCK_VALUE, 
                    ex=WORKER_LOCK_TIMEOUT_SECONDS, 
                    nx=True
                )
                logger.info(f"[MAIN_WORKER] 🔐 Результат установки блокировки (nx=True): {lock_set}")
                
                if lock_set:
                    logger.info("[MAIN_WORKER] ✅ Блокировка установлена успешно! Начинаю обработку очереди...")
                    
                    task_processed = await _get_and_process_task_from_queue(redis_conn)
                    
                    if not task_processed:
                        logger.info("[MAIN_WORKER] 📭 Очередь пуста. Удаляю блокировку...")
                        await redis_conn.delete(WORKER_LOCK_KEY)
                        logger.info("[MAIN_WORKER] 🔓 Блокировка снята. Ожидаю новых задач...")
                    else:
                        logger.info("[MAIN_WORKER] ✅ Задача обработана. Удаляю блокировку...")
                        await redis_conn.delete(WORKER_LOCK_KEY)
                        logger.info("[MAIN_WORKER] 🔓 Блокировка снята. Проверяю очередь снова...")
                        continue 

                else:
                    logger.info("[MAIN_WORKER] ⚠️  Не удалось установить блокировку (другой процесс успел раньше или она уже существует). Ожидаю.")

        except Exception as e:
            logger.critical(f"[MAIN_WORKER] 💥 КРИТИЧЕСКАЯ ОШИБКА в цикле воркера: {e}", exc_info=True)
            
        logger.info(f"[MAIN_WORKER] 💤 Сон {WORKER_RETRY_DELAY} сек перед следующей итерацией...")
        await asyncio.sleep(WORKER_RETRY_DELAY)


async def main():
    """
    Основная функция для запуска воркера. 
    """
    await background_worker()