import asyncio
import logging
from typing import Dict, Any

# --- Импорт всех необходимых модулей для выполнения задачи ---
from database import get_coins_from_db
from data_collector import fetch_market_data
from indicator_calculator import add_indicators
from cache_manager import save_to_cache

# --- Очередь Задач ---
# Очередь определяется здесь и импортируется в main.py
task_queue = asyncio.Queue()

# --- Фоновый Обработчик (Worker) ---
async def background_worker():
    """
    Бесконечно работающий процесс, который берет задачи из очереди
    и выполняет их последовательно, одну за другой.
    """
    while True:
        try:
            # Ждем, пока в очереди не появится задача
            timeframe: str = await task_queue.get()
            logging.info(f"WORKER: Взял задачу для таймфрейма '{timeframe}'. Задач в очереди: {task_queue.qsize()}")

            # --- Начало выполнения тяжелой задачи ---
            # 1. Получаем список монет из БД
            coins = get_coins_from_db()
            if not coins:
                logging.error(f"WORKER: Не удалось получить монеты из БД для '{timeframe}'. Задача пропущена.")
                task_queue.task_done()
                continue

            # 2. Собираем данные с бирж
            market_data = await fetch_market_data(coins, timeframe)
            if not market_data or not market_data.get('data'):
                logging.error(f"WORKER: Не удалось собрать данные для '{timeframe}'. Задача пропущена.")
                task_queue.task_done()
                continue

            # 3. Рассчитываем индикаторы
            market_data_with_indicators = add_indicators(market_data)

            # 4. Сохраняем в кэш
            save_to_cache(timeframe, market_data_with_indicators)
            # --- Конец выполнения тяжелой задачи ---

            logging.info(f"WORKER: Задача для таймфрейма '{timeframe}' успешно выполнена.")

        except Exception as e:
            logging.error(f"WORKER: Критическая ошибка при обработке задачи: {e}", exc_info=True)
        finally:
            # Сообщаем очереди, что задача завершена, чтобы .join() мог работать
            task_queue.task_done()
