import requests
import logging
import time
import asyncio
from typing import Dict, Any

# --- НАСТРОЙКА ---
BASE_URL = "http://127.0.0.1:8000"
TIMEFRAME_TO_TEST = '4h'
CACHE_WAIT_TIMEOUT = 300 # 5 минут

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_final_data(data: Dict[str, Any]) -> bool:
    """
    Максимально подробно проверяет финальную структуру данных, включая все метаданные и индикаторы.
    """
    try:
        # --- БАЗОВАЯ ПРОВЕРКА СТРУКТУРЫ ---
        if not all(k in data for k in ["openTime", "closeTime", "timeframe", "data"]):
            logging.error("  -> ПРОВАЛ: Отсутствуют ключи верхнего уровня (openTime, closeTime, timeframe, data).")
            return False

        data_list = data.get('data', [])
        if not data_list:
            logging.warning("  -> ВАЛИДАЦИЯ: Список 'data' пуст, но структура верна.")
            return True

        first_coin_data = data_list[0]
        symbol = first_coin_data.get("symbol", "Неизвестный символ")

        # --- НОВАЯ ПРОВЕРКА №1: ПОЛНАЯ ВАЛИДАЦИЯ МЕТАДАННЫХ ---
        # Список всех ожидаемых общих полей
        expected_common_meta_keys = {
            "symbol", "exchanges", "logoUrl", "category", "volatility_index",
            "efficiency_index", "trend_harmony_index", "btc_correlation",
            "returns_skewness", "avg_wick_ratio", "relative_strength_vs_btc",
            "max_drawdown_percent", "data"
        }
        # Список специфичных для таймфрейма полей (без суффикса)
        expected_tf_meta_keys = {"hurst", "entropy"}
        
        all_expected_keys = expected_common_meta_keys.union(expected_tf_meta_keys)

        missing_keys = [key for key in all_expected_keys if key not in first_coin_data]
        if missing_keys:
            logging.error(f"  -> ПРОВАЛ: У монеты {symbol} отсутствуют мета-поля: {missing_keys}")
            return False
        logging.info(f"  -> УСПЕХ: Все {len(all_expected_keys)} мета-полей для {symbol} на месте.")

        # --- ПРОВЕРКА KLINE ДАННЫХ И ИНДИКАТОРОВ ---
        candle_list = first_coin_data.get('data', [])
        if not candle_list:
             logging.warning(f"  -> ВАЛИДАЦИЯ: Список свечей для {symbol} пуст.")
             return True
        
        last_candle = candle_list[-1]
        expected_candle_keys = {
            "openTime", "closeTime", "highPrice", "lowPrice", "closePrice",
            "rsi_14", "adx_14", "di_plus_14", "di_minus_14",
            "ema_50", "ema_100", "ema_150"
        }
        missing_candle_keys = [key for key in expected_candle_keys if key not in last_candle]
        if missing_candle_keys:
            logging.error(f"  -> ПРОВАЛ: В последней свече для {symbol} отсутствуют поля: {missing_candle_keys}")
            return False
        logging.info(f"  -> УСПЕХ: Все поля в свечах для {symbol} на месте.")

        logging.info(f"  -> УСПЕХ ВАЛИДАЦИИ: Финальный формат данных для {symbol} полностью корректен.")
        return True

    except Exception as e:
        logging.error(f"  -> КРИТИЧЕСКАЯ ОШИБКА ВАЛИДАЦИИ: {e}", exc_info=True)
        return False


async def main_test():
    """
    Основная асинхронная функция для проведения теста.
    """
    start_time = time.time()
    
    logging.info(f"--- Шаг 1: Запуск задачи для таймфрейма '{TIMEFRAME_TO_TEST}' ---")
    post_url = f"{BASE_URL}/get-market-data"
    try:
        response = requests.post(post_url, json={"timeframe": TIMEFRAME_TO_TEST})
        if response.status_code != 202:
            logging.critical(f"Не удалось запустить задачу. Сервер ответил {response.status_code}. Тест остановлен.")
            return
        logging.info(f"  -> УСПЕХ: Задача для '{TIMEFRAME_TO_TEST}' принята в обработку.")
    except requests.exceptions.RequestException as e:
        logging.critical(f"Не удалось отправить запрос. Сервер запущен? Ошибка: {e}. Тест остановлен.")
        return

    logging.info(f"\n--- Шаг 2: Ожидание результата в кэше (таймаут {CACHE_WAIT_TIMEOUT} сек) ---")
    cache_url = f"{BASE_URL}/cache/{TIMEFRAME_TO_TEST}"
    
    wait_start_time = time.time()
    while time.time() - wait_start_time < CACHE_WAIT_TIMEOUT:
        try:
            response = requests.get(cache_url)
            if response.status_code == 200:
                logging.info(f"  -> УСПЕХ: Данные появились в кэше!")
                
                logging.info(f"\n--- Шаг 3: Финальная проверка и валидация кэша ---")
                validate_final_data(response.json())
                
                total_time = time.time() - start_time
                logging.info(f"\n--- Общее время выполнения задачи: {total_time:.2f} секунд ---")
                return
                
            logging.info(f"  -> ОЖИДАНИЕ: Кэш пока пуст (404). Ждем 5 секунд...")
            await asyncio.sleep(5)
            
        except requests.exceptions.RequestException as e:
            logging.error(f"  -> ОШИБКА: Не удалось подключиться к серверу для проверки кэша. {e}")
            await asyncio.sleep(5)

    logging.critical(f"--- ПРОВАЛ: Данные не появились в кэше за {CACHE_WAIT_TIMEOUT} секунд. ---")


if __name__ == "__main__":
    asyncio.run(main_test())

