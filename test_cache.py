import requests
import logging
import time
import asyncio
from typing import Dict, Any

# --- НАСТРОЙКА ---
BASE_URL = "http://127.0.0.1:8000"
ALL_TIMEFRAMES = ['1h', '4h', '8h', '12h', '1d']

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_data_with_indicators(data: Dict[str, Any]) -> bool:
    """
    Проверяет, что данные из кэша имеют правильную структуру, включая индикаторы.
    """
    try:
        if not all(k in data for k in ["openTime", "closeTime", "timeframe", "data"]):
            logging.error("  -> ПРОВАЛ ВАЛИДАЦИИ: Отсутствуют ключи верхнего уровня.")
            return False

        data_list = data.get('data', [])
        if not data_list:
            logging.warning("  -> ВАЛИДАЦИЯ: Список 'data' пуст, но структура верна.")
            return True

        first_coin_data = data_list[0]
        if not all(k in first_coin_data for k in ["symbol", "exchanges", "data"]):
            logging.error("  -> ПРОВАЛ ВАЛИДАЦИИ: В объекте монеты отсутствуют ключи.")
            return False

        candle_list = first_coin_data.get('data', [])
        if not candle_list:
             logging.warning(f"  -> ВАЛИДАЦИЯ: Список свечей для {first_coin_data['symbol']} пуст.")
             return True
        
        last_candle = candle_list[-1]
        candle_keys = ["openTime", "closeTime", "highPrice", "lowPrice", "closePrice"]
        indicator_keys = ["rsi_14", "adx_14", "ema_50"]
        
        if not all(k in last_candle for k in candle_keys + indicator_keys):
            logging.error(f"  -> ПРОВАЛ ВАЛИДАЦИИ: В свече отсутствуют обязательные поля или индикаторы.")
            return False
            
        return True
    except Exception as e:
        logging.error(f"  -> КРИТИЧЕСКАЯ ОШИБКА ВАЛИДАЦИИ: {e}", exc_info=True)
        return False

async def main_test():
    """
    Основная асинхронная функция для проведения стресс-теста.
    """
    start_time = time.time()
    
    # --- Шаг 1: "Вмазываем" все 5 таймфреймов в очередь ---
    logging.info(f"--- Шаг 1: Одновременная отправка запросов для ВСЕХ таймфреймов ---")
    post_url = f"{BASE_URL}/get-market-data"
    for timeframe in ALL_TIMEFRAMES:
        try:
            response = requests.post(post_url, json={"timeframe": timeframe})
            if response.status_code == 202:
                logging.info(f"  -> УСПЕХ: Задача для '{timeframe}' принята в обработку (202 Accepted).")
            else:
                logging.error(f"  -> ОШИБКА: Сервер не принял задачу для '{timeframe}'. Статус: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.critical(f"Не удалось отправить запрос для '{timeframe}'. Сервер запущен? Ошибка: {e}")
            return

    # --- Шаг 2: Мониторим очередь, пока она не опустеет ---
    logging.info("\n--- Шаг 2: Мониторинг выполнения задач в очереди ---")
    status_url = f"{BASE_URL}/queue-status"
    while True:
        try:
            response = requests.get(status_url)
            tasks_in_queue = response.json().get('tasks_in_queue', -1)
            
            if tasks_in_queue == 0:
                logging.info("  -> УСПЕХ: Очередь пуста. Все задачи выполнены.")
                break
            elif tasks_in_queue > 0:
                logging.info(f"  -> ВЫПОЛНЕНИЕ: В очереди {tasks_in_queue} задач. Ждем 10 секунд...")
                await asyncio.sleep(10)
            else:
                logging.error("  -> ОШИБКА: Не удалось получить статус очереди.")
                break
        except requests.exceptions.RequestException:
            logging.error("  -> ОШИБКА: Не удалось подключиться к серверу для проверки статуса.")
            break

    total_time = time.time() - start_time
    logging.info(f"\n--- Общее время выполнения всех задач: {total_time:.2f} секунд ---")

    # --- Шаг 3: Проверяем и валидируем данные во всех кэшах ---
    logging.info("\n--- Шаг 3: Финальная проверка и валидация всех кэшей ---")
    for timeframe in ALL_TIMEFRAMES:
        cache_url = f"{BASE_URL}/cache/{timeframe}"
        logging.info(f"Проверка кэша для '{timeframe}'...")
        try:
            response = requests.get(cache_url)
            if response.status_code == 200:
                data = response.json()
                coin_count = len(data.get('data', []))
                logging.info(f"  -> УСПЕХ: Данные найдены. Получено {coin_count} монет.")
                if not validate_data_with_indicators(data):
                     logging.error(f"  -> ПРОВАЛ: Данные для '{timeframe}' не прошли валидацию!")
                else:
                     logging.info(f"  -> УСПЕХ: Данные для '{timeframe}' прошли валидацию.")
            else:
                logging.error(f"  -> ПРОВАЛ: Кэш для '{timeframe}' должен быть заполнен, но сервер ответил {response.status_code}.")
        except requests.exceptions.RequestException:
            logging.error(f"  -> КРИТИЧЕСКАЯ ОШИБКА при проверке кэша для '{timeframe}'.")

if __name__ == "__main__":
    asyncio.run(main_test())

