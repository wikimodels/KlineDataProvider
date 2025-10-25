import asyncio
import logging
from typing import Dict, Any, List
import json

# --- Импорт всех необходимых модулей для выполнения задачи ---
from database import get_coins_from_db
from data_collector import fetch_market_data
# from indicator_calculator import add_indicators  <-- УДАЛЕНО
from cache_manager import save_to_cache
# --- ИЗМЕНЕНИЕ: Импортируем redis_client ---
from cache_manager import redis_client 

# --- Очередь Задач ---
task_queue = asyncio.Queue()

# --- ИЗМЕНЕНИЕ: Константы для глобальной блокировки ---
WORKER_LOCK_KEY = "data_collector_lock"
# Таймаут блокировки (30 минут) на случай, если воркер умрет,
# не освободив блокировку.
WORKER_LOCK_TIMEOUT = 1800 


def _enrich_coin_metadata(market_data: Dict[str, Any], coins_from_db: List[Dict]) -> Dict[str, Any]:
    """
    Обогащает метаданные каждой монеты, добавляя общие и специфичные для
    таймфрейма аналитические данные из БД.
    """
    if not market_data.get('data') or not coins_from_db:
        return market_data

    coins_map = {c['symbol'].split(':')[0]: c for c in coins_from_db}
    timeframe = market_data.get('timeframe')

    for coin_data in market_data['data']:
        symbol = coin_data['symbol']
        full_coin_details = coins_map.get(symbol)

        if full_coin_details:
            # Итерируемся по всем полям, полученным из БД
            for key, value in full_coin_details.items():
                # Пропускаем ключи, которые уже есть (symbol, exchanges) или не нужны
                if key in ['symbol', 'exchanges']:
                    continue

                # Для полей, специфичных для таймфрейма, убираем суффикс
                if key.endswith(f"_{timeframe}"):
                    clean_key = key.replace(f"_{timeframe}", '')
                    coin_data[clean_key] = value
                else: # Общие поля добавляем как есть
                    coin_data[key] = value
    
    return market_data

# --- ФУНКЦИЯ АУДИТА (_audit_final_data) УДАЛЕНА ---
# (Логика перенесена в data_processing.py)


async def background_worker():
    """
    Основной рабочий процесс, выполняющий задачи из очереди с детальным логированием
    И С ГЛОБАЛЬНОЙ БЛОКИРОВКОЙ.
    """
    while True:
        # --- ИЗМЕНЕНИЕ: Вся логика обернута в try/finally для task_done() ---
        # --- НО блокировка проверяется ДО основной работы ---
        
        timeframe: str = await task_queue.get()
        lock_acquired = False
        
        try:
            # --- ИЗМЕНЕНИЕ: Логика проверки и установки блокировки ---
            if not redis_client:
                logging.warning(f"WORKER: Redis недоступен, не могу проверить блокировку. Возвращаю '{timeframe}' в очередь.")
                await asyncio.sleep(10) # Пауза перед возвратом
                await task_queue.put(timeframe)
                continue # Переход к finally -> task_done()

            # Пытаемся захватить блокировку
            # nx=True: установить, только если не существует
            # ex=WORKER_LOCK_TIMEOUT: установить время жизни
            lock_acquired = redis_client.set(
                WORKER_LOCK_KEY, 
                f"busy_by_{timeframe}", 
                nx=True, 
                ex=WORKER_LOCK_TIMEOUT
            )

            if not lock_acquired:
                # Блокировка уже установлена другим воркером
                logging.info(f"WORKER: Другой процесс уже выполняет сбор данных. Возвращаю '{timeframe}' в очередь.")
                await asyncio.sleep(15) # Пауза, чтобы не спамить проверками
                await task_queue.put(timeframe)
                continue # Переход к finally -> task_done()
            
            # --- Блокировка получена, выполняем основную логику ---
            
            logging.info(f"WORKER: Блокировка получена. Начинаю задачу для '{timeframe}'.")

            # --- Шаг 1: Получение данных из БД ---
            coins_from_db = get_coins_from_db(timeframe)
            if not coins_from_db:
                logging.error(f"WORKER: Не удалось получить монеты из БД для '{timeframe}'. Задача пропущена.")
                continue # Переход к finally
            
            # --- Шаг 2: Сбор рыночных данных ---
            market_data = await fetch_market_data(coins_from_db, timeframe)
            if not market_data:
                logging.error(f"WORKER: Сборщик рыночных данных вернул пустой ответ для '{timeframe}'. Задача пропущена.")
                continue # Переход к finally
            
            processed_coins_count = len(market_data.get('data', []))

            # --- Шаг 3: Расчет индикаторов (УДАЛЕН) ---
            
            # --- Шаг 4: Обогащение метаданных ---
            final_enriched_data = _enrich_coin_metadata(market_data, coins_from_db) 
            
            # --- Шаг 5: Сохранение в кэш (ПЕРЕИМЕНОВАН В ШАГ 4) ---
            save_to_cache(timeframe, final_enriched_data)
            
            logging.info(f"WORKER: Задача для таймфрейма '{timeframe}' успешно выполнена.")

        except Exception as e:
            logging.error(f"WORKER: Критическая ошибка при обработке задачи '{timeframe}': {e}", exc_info=True)
        
        finally:
            # --- ИЗМЕНЕНИЕ: Освобождение блокировки ---
            if lock_acquired:
                if redis_client:
                    logging.info(f"WORKER: Освобождаю блокировку (задача: {timeframe}).")
                    redis_client.delete(WORKER_LOCK_KEY)
                else:
                    # Редкий случай: Redis упал МЕЖДУ установкой и снятием
                    logging.critical(f"WORKER: REDIS НЕДОСТУПЕН! НЕ МОГУ ОСВОБОДИТЬ БЛОКИРОВКУ! Истечет через {WORKER_LOCK_TIMEOUT}с.")
            
            # Этот вызов теперь ЕДИНСТВЕННЫЙ и гарантированно выполняется 1 раз
            # Он отмечает, что *попытка* обработки (включая возврат в очередь) завершена.
            task_queue.task_done()