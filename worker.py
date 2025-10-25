import asyncio
import logging
from typing import Dict, Any, List
import json

# --- Импорт всех необходимых модулей для выполнения задачи ---
from database import get_coins_from_db
from data_collector import fetch_market_data
# from indicator_calculator import add_indicators  <-- УДАЛЕНО
from cache_manager import save_to_cache

# --- Очередь Задач ---
task_queue = asyncio.Queue()

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
    Основной рабочий процесс, выполняющий задачи из очереди с детальным логированием.
    """
    while True:
        try:
            # Логика получения одного таймфрейма из очереди СОХРАНЕНА
            timeframe: str = await task_queue.get()
            # logging.info(f"WORKER: Взял задачу для '{timeframe}'. Задач в очереди: {task_queue.qsize()}") # <-- УДАЛЕН ШУМ

            # --- Шаг 1: Получение данных из БД ---
            coins_from_db = get_coins_from_db(timeframe)
            if not coins_from_db:
                logging.error(f"WORKER: Не удалось получить монеты из БД для '{timeframe}'. Задача пропущена.")
                continue
            # logging.info(f"WORKER: Шаг 1/4 | Получено {len(coins_from_db)} монет из БД.") # <-- УДАЛЕН ШУМ

            # --- Шаг 2: Сбор рыночных данных ---
            market_data = await fetch_market_data(coins_from_db, timeframe)
            # --- ИЗМЕНЕНИЕ: Проверяем market_data (т.к. klines могут отсутствовать, но audit_report будет) ---
            if not market_data:
                logging.error(f"WORKER: Сборщик рыночных данных вернул пустой ответ для '{timeframe}'. Задача пропущена.")
                continue
            
            # Логгируем кол-во, даже если 0
            processed_coins_count = len(market_data.get('data', []))
            # logging.info(f"WORKER: Шаг 2/4 | Собраны рыночные данные для {processed_coins_count} монет.") # <-- УДАЛЕН ШУМ

            # --- Шаг 3: Расчет индикаторов (УДАЛЕН) ---
            # market_data_with_indicators = add_indicators(market_data) <-- УДАЛЕНО
            # logging.info(f"WORKER: Шаг 3/4 | Расчет индикаторов пропущен.") # <-- УДАЛЕН ШУМ
            
            # --- Шаг 4: Обогащение метаданных ---
            # Изменена переменная на 'market_data'
            final_enriched_data = _enrich_coin_metadata(market_data, coins_from_db) 
            # logging.info(f"WORKER: Шаг 4/4 | Метаданные обогащены.") # <-- УДАЛЕН ШУМ
            
            # --- ДЕБАГГИНГ ЛОГ (ЛОГИКА СОХРАНЕНА) ---
            if final_enriched_data.get('data'):
                example_coin = final_enriched_data['data'][0]
                example_meta = {k: v for k, v in example_coin.items() if k != 'data'}
                # logging.info(f"WORKER: Пример обогащенных метаданных для {example_meta.get('symbol')}: {json.dumps(example_meta, indent=2)}") # <-- УДАЛЕН ШУМ

            # --- ВЫЗОВ АУДИТА УДАЛЕН ---
            # (Логика теперь в data_processing.py)

            # --- Шаг 5: Сохранение в кэш (ПЕРЕИМЕНОВАН В ШАГ 4) ---
            # Эта логика была Шагом 5, теперь она часть Шага 4
            save_to_cache(timeframe, final_enriched_data)
            # logging.info(f"WORKER: Шаг 4/4 | Обогащенные данные сохранены в кэш.") # <-- УДАЛЕН ШУМ
            
            # logging.info(f"WORKER: Задача для таймфрейма '{timeframe}' успешно выполнена.") # <-- УДАЛЕН ШУМ

        except Exception as e:
            logging.error(f"WORKER: Критическая ошибка при обработке задачи: {e}", exc_info=True)
        finally:
            # Этот вызов теперь ЕДИНСТВЕННЫЙ и гарантированно выполняется 1 раз
            task_queue.task_done()

