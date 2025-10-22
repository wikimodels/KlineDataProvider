import asyncio
import logging
from typing import Dict, Any, List
import json

# --- Импорт всех необходимых модулей для выполнения задачи ---
from database import get_coins_from_db
from data_collector import fetch_market_data
from indicator_calculator import add_indicators
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


async def background_worker():
    """
    Основной рабочий процесс, выполняющий задачи из очереди с детальным логированием.
    """
    while True:
        try:
            timeframe: str = await task_queue.get()
            logging.info(f"WORKER: Взял задачу для '{timeframe}'. Задач в очереди: {task_queue.qsize()}")

            # --- Шаг 1: Получение данных из БД ---
            # Передаем таймфрейм, чтобы получить нужные поля
            coins_from_db = get_coins_from_db(timeframe)
            if not coins_from_db:
                logging.error(f"WORKER: Не удалось получить монеты из БД для '{timeframe}'. Задача пропущена.")
                task_queue.task_done()
                continue
            logging.info(f"WORKER: Шаг 1/5 | Получено {len(coins_from_db)} монет из БД.")

            # --- Шаг 2: Сбор рыночных данных ---
            market_data = await fetch_market_data(coins_from_db, timeframe)
            if not market_data or not market_data.get('data'):
                logging.error(f"WORKER: Не удалось собрать рыночные данные для '{timeframe}'. Задача пропущена.")
                task_queue.task_done()
                continue
            processed_coins_count = len(market_data.get('data', []))
            logging.info(f"WORKER: Шаг 2/5 | Собраны рыночные данные для {processed_coins_count} монет.")

            # --- Шаг 3: Расчет индикаторов ---
            market_data_with_indicators = add_indicators(market_data)
            logging.info(f"WORKER: Шаг 3/5 | Технические индикаторы рассчитаны.")
            
            # --- Шаг 4: Обогащение метаданных ---
            final_enriched_data = _enrich_coin_metadata(market_data_with_indicators, coins_from_db)
            logging.info(f"WORKER: Шаг 4/5 | Метаданные обогащены.")
            
            # --- ДЕБАГГИНГ ЛОГ: Показываем пример обогащенных данных ---
            if final_enriched_data.get('data'):
                example_coin = final_enriched_data['data'][0]
                # Убираем kline данные для краткости лога
                example_meta = {k: v for k, v in example_coin.items() if k != 'data'}
                logging.info(f"WORKER: Пример обогащенных метаданных для {example_meta.get('symbol')}: {json.dumps(example_meta, indent=2)}")

            # --- Шаг 5: Сохранение в кэш ---
            save_to_cache(timeframe, final_enriched_data)
            logging.info(f"WORKER: Шаг 5/5 | Обогащенные данные сохранены в кэш.")
            
            logging.info(f"WORKER: Задача для таймфрейма '{timeframe}' успешно выполнена.")

        except Exception as e:
            logging.error(f"WORKER: Критическая ошибка при обработке задачи: {e}", exc_info=True)
        finally:
            task_queue.task_done()

