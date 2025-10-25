import requests
import logging
import time
import asyncio
from typing import Dict, Any, List
import json

# --- НАСТРОЙКА ---
BASE_URL = "http://127.0.0.1:8000"
TIMEFRAME_TO_TEST = '4h'
CACHE_WAIT_TIMEOUT = 600  # 10 минут

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_data_collection(data: Dict[str, Any]) -> bool:
    """
    Проверяет, что базовая структура данных (Klines + OI + FR)
    была корректно собрана, ИСПОЛЬЗУЯ 'audit_report'.
    
    Эта функция НЕ проверяет метаданные (logoUrl, hurst, entropy и т.д.),
    а только результат работы data_collector.py (data_processing.py).
    """
    try:
        # --- БАЗОВАЯ ПРОВЕРКА СТРУКТУРЫ ---
        # Проверяем наличие всех ключей, включая audit_report
        expected_keys = ["openTime", "closeTime", "timeframe", "data", "audit_report"]
        missing_keys = [k for k in expected_keys if k not in data]
        if missing_keys:
            logging.error(f"  -> ПРОВАЛ: Отсутствуют ключи верхнего уровня: {missing_keys}.")
            return False

        # --- ПРОВЕРКА АУДИТА ---
        audit_report = data.get('audit_report')
        if not isinstance(audit_report, dict):
            logging.error(f"  -> ПРОВАЛ: 'audit_report' не является словарем. Структура ответа неверна.")
            return False
            
        logging.info("--- Проверка 'audit_report' ---")
        
        missing_klines = audit_report.get("missing_klines", [])
        missing_oi = audit_report.get("missing_oi", [])
        missing_fr = audit_report.get("missing_fr", [])

        overall_success = True
        
        # 1. Проверка Klines
        if missing_klines:
            logging.error(f"  -> ПРОВАЛ АУДИТА [KLINES]: Отсутствуют Klines для {len(missing_klines)} монет: {missing_klines}")
            overall_success = False
        else:
            logging.info("  -> УСПЕХ АУДИТА [KLINES]: Все Klines на месте.")
        
        # 2. Проверка OI
        if missing_oi:
            logging.error(f"  -> ПРОВАЛ АУДИТА [OI]: Отсутствует Open Interest для {len(missing_oi)} монет: {missing_oi}")
            overall_success = False
        else:
            logging.info("  -> УСПЕХ АУДИТА [OI]: Весь Open Interest на месте.")

        # 3. Проверка FR
        if missing_fr:
            logging.error(f"  -> ПРОВАЛ АУДИТА [FR]: Отсутствует Funding Rate для {len(missing_fr)} монет: {missing_fr}")
            overall_success = False
        else:
            logging.info("  -> УСПЕХ АУДИТА [FR]: Весь Funding Rate на месте.")

        # --- ИТОГ ---
        if overall_success:
            total_coins = len(data.get("data", [])) # Кол-во монет, которые *были* собраны
            logging.info(f"  -> УСПЕХ ВАЛИДАЦИИ: 'audit_report' пуст. Все данные для {total_coins} монет собраны корректно.")
        else:
            logging.error("  -> ПРОВАЛ ВАЛИДАЦИИ: 'audit_report' содержит ошибки.")
        
        return overall_success

    except Exception as e:
        logging.error(f"  -> КРИТИЧЕСКАЯ ОШИБКА ВАЛИДАЦИИ: {e}", exc_info=True)
        return False


async def main_test():
    """
    Основная асинхронная функция для проведения теста сбора данных.
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
                
                logging.info(f"\n--- Шаг 3: Проверка собранных данных (Klines + OI + FR) ---")
                
                try:
                    json_data = response.json()
                except requests.exceptions.JSONDecodeError:
                    logging.critical(f"\n--- ТЕСТ ПРОВАЛЕН: Сервер вернул не-JSON ответ. ---")
                    return

                is_valid = validate_data_collection(json_data)
                
                if is_valid:
                    total_time = time.time() - start_time
                    logging.info(f"\n--- ТЕСТ УСПЕШЕН: 'audit_report' чист. Все данные присутствуют! Общее время: {total_time:.2f} сек. ---")
                else:
                    logging.critical(f"\n--- ТЕСТ ПРОВАЛЕН: 'audit_report' сообщил об ошибках. (См. логи выше) ---")
                
                return
                
            logging.info(f"  -> ОЖИДАНИЕ: Кэш пока пуст (404). Ждем 10 секунд...")
            await asyncio.sleep(10)
            
        except requests.exceptions.RequestException as e:
            logging.error(f"  -> ОШИКА: Не удалось подключиться к серверу для проверки кэша. {e}")
            await asyncio.sleep(10)

    logging.critical(f"--- ПРОВАЛ: Данные не появились в кэше за {CACHE_WAIT_TIMEOUT} секунд. ---")


if __name__ == "__main__":
    asyncio.run(main_test())
