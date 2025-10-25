import asyncio
import aiohttp
import logging
import os
import time
import json
from dotenv import load_dotenv
from typing import Tuple, Dict, Any

# --- НОВЫЙ КЛАСС-ФИЛЬТР ---
class TaskNameFilter(logging.Filter):
    """
    Этот фильтр 'внедряет' имя текущей задачи asyncio в запись лога.
    """
    def filter(self, record):
        try:
            # Пытаемся получить имя задачи
            task = asyncio.current_task()
            if task:
                # Если у задачи есть имя, используем его
                record.task_name = task.get_name()
            else:
                # Если задачи нет (например, выполняется в main)
                record.task_name = 'main'
        except RuntimeError:
            # Случается, если логгирование происходит вне asyncio-цикла
            record.task_name = 'main'
        return True

# --- Настройка ---
# Загружаем переменные из .env файла (должен лежать рядом)
load_dotenv()

# URL вашего сервиса на Render (берется из .env)
BASE_URL = os.environ.get("BASE_URL")

# Таймфреймы для одновременного тестирования
TIMEFRAMES_TO_TEST = ['4h']

# Таймауты (у Render могут быть долгие "холодные" старты)
# 20 минут = 1200 секунд
CACHE_WAIT_TIMEOUT = int(os.environ.get("CACHE_WAIT_TIMEOUT", 1200)) 
# Интервал проверки кэша
CACHE_POLL_INTERVAL = int(os.environ.get("CACHE_POLL_INTERVAL", 15))

# Настройка логирования (ФОРМАТ СОХРАНЕН)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(task_name)s] - %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger()

# --- НОВОЕ: ДОБАВЛЯЕМ ФИЛЬТР ---
# Это свяжет 'task_name' из asyncio с 'task_name' в логгере
task_filter = TaskNameFilter()
for handler in log.handlers:
    handler.addFilter(task_filter)


def validate_audit_report(data: Dict[str, Any], timeframe: str) -> bool:
    """
    Проверяет, что 'audit_report' в ответе пустой.
    """
    try:
        audit_report = data.get("audit_report")
        if not isinstance(audit_report, dict):
            log.error(f"'audit_report' отсутствует или не является словарем.")
            return False

        missing_klines = audit_report.get("missing_klines", [])
        missing_oi = audit_report.get("missing_oi", [])
        missing_fr = audit_report.get("missing_fr", [])

        is_success = not missing_klines and not missing_oi and not missing_fr

        if is_success:
            total_coins = len(data.get("data", []))
            log.info(f"ВАЛИДАЦИЯ УСПЕШНА: 'audit_report' пуст. Собрано {total_coins} монет.")
            return True
        else:
            log.error(f"ВАЛИДАЦИЯ ПРОВАЛЕНА: 'audit_report' содержит ошибки:")
            if missing_klines:
                log.error(f"  -> Отсутствуют Klines для {len(missing_klines)} монет.")
            if missing_oi:
                log.error(f"  -> Отсутствует OI для {len(missing_oi)} монет.")
            if missing_fr:
                log.error(f"  -> Отсутствует FR для {len(missing_fr)} монет.")
            # Печатаем полный отчет для диагностики
            log.error(f"Полный audit_report: {json.dumps(audit_report, indent=2)}")
            return False
            
    except Exception as e:
        log.error(f"Критическая ошибка валидации JSON: {e}", exc_info=True)
        return False


async def run_single_test(session: aiohttp.ClientSession, timeframe: str) -> Tuple[str, bool]:
    """
    Выполняет полный цикл теста для одного таймфрейма:
    1. Запускает задачу (POST)
    2. Ждет кэш (GET)
    3. Валидирует 'audit_report'
    Возвращает (timeframe, success_boolean)
    """
    # Устанавливаем имя задачи для логгера
    # Эта строка по-прежнему важна!
    asyncio.current_task().set_name(f"TF-{timeframe}")
    
    post_url = f"{BASE_URL}/get-market-data"
    cache_url = f"{BASE_URL}/cache/{timeframe}"
    start_time = time.time()

    try:
        # --- Шаг 1: Запуск задачи (POST) ---
        log.info(f"Шаг 1: Отправка POST-запроса на /get-market-data...")
        async with session.post(post_url, json={"timeframe": timeframe}, timeout=30) as response:
            
            if response.status == 202:
                log.info(f"Шаг 1 УСПЕХ: Задача принята (Статус 202).")
            elif response.status == 409:
                log.warning(f"Шаг 1 ПРЕДУПРЕЖДЕНИЕ: Задача уже выполняется (Статус 409). Продолжаем...")
            else:
                response_text = await response.text()
                log.error(f"Шаг 1 ПРОВАЛ: Сервер вернул {response.status}. Ответ: {response_text[:150]}")
                return timeframe, False

    except asyncio.TimeoutError:
        log.error(f"Шаг 1 ПРОВАЛ: Таймаут POST-запроса (сервер не ответил за 30с).")
        return timeframe, False
    except aiohttp.ClientError as e:
        log.error(f"Шаг 1 ПРОВАЛ: Ошибка POST-запроса: {e}")
        return timeframe, False

    # --- Шаг 2: Ожидание кэша (GET) ---
    log.info(f"Шаг 2: Ожидание кэша (Таймаут {CACHE_WAIT_TIMEOUT}с)...")
    wait_start_time = time.time()
    
    while time.time() - wait_start_time < CACHE_WAIT_TIMEOUT:
        try:
            async with session.get(cache_url, timeout=30) as response:
                
                if response.status == 200:
                    log.info(f"Шаг 2 УСПЕХ: Кэш получен (Статус 200)!")
                    
                    # --- Шаг 3: Валидация ---
                    try:
                        json_data = await response.json()
                        log.info(f"Шаг 3: Валидация 'audit_report'...")
                        is_valid = validate_audit_report(json_data, timeframe)
                        
                        total_time = time.time() - start_time
                        log.info(f"Тест завершен за {total_time:.2f}с. Результат: {'УСПЕХ' if is_valid else 'ПРОВАЛ'}")
                        return timeframe, is_valid

                    except json.JSONDecodeError:
                        log.error(f"Шаг 3 ПРОВАЛ: Сервер вернул не-JSON ответ: {await response.text()[:150]}...")
                        return timeframe, False
                    
                elif response.status == 404:
                    log.info(f"Шаг 2 ОЖИДАНИЕ: Кэш пока пуст (404). Проверка через {CACHE_POLL_INTERVAL}с...")
                else:
                    log.warning(f"Шаг 2 ОШИБКА ОЖИДАНИЯ: Сервер вернул {response.status}. Повтор...")

        except asyncio.TimeoutError:
            log.warning(f"Шаг 2 ОШИБКА ОЖИДАНИЯ: Таймаут GET-запроса (сервер не ответил за 30с). Повтор...")
        except aiohttp.ClientError as e:
            log.warning(f"Шаг 2 ОШИБКА ОЖИДАНИЯ: Ошибка GET-запроса: {e}. Повтор...")

        await asyncio.sleep(CACHE_POLL_INTERVAL)

    # Если мы вышли из цикла по таймауту
    log.error(f"Шаг 2 ПРОВАЛ: Таймаут! Кэш не появился за {CACHE_WAIT_TIMEOUT} секунд.")
    return timeframe, False


async def main():
    """
    Главная функция: запускает все тесты параллельно и собирает результаты.
    """
    if not BASE_URL:
        log.critical("ПРОВАЛ: Переменная BASE_URL не найдена в .env файле.")
        log.critical("Пожалуйста, создайте .env файл и добавьте: BASE_URL=https://ваш-адрес.onrender.com")
        return

    log.info(f"--- ЗАПУСК СТРЕСС-ТЕСТА ---")
    log.info(f"Цель: {BASE_URL}")
    log.info(f"Таймфреймы: {TIMEFRAMES_TO_TEST}")
    log.info(f"Таймаут на задачу: {CACHE_WAIT_TIMEOUT}с")
    
    start_time = time.time()

    # Устанавливаем общий таймаут на *все* операции, включая таймауты подключения
    session_timeout = aiohttp.ClientTimeout(total=CACHE_WAIT_TIMEOUT + 60)
    
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        # Создаем список задач
        tasks = [run_single_test(session, tf) for tf in TIMEFRAMES_TO_TEST]
        
        # Запускаем все параллельно
        results = await asyncio.gather(*tasks)

    total_duration = time.time() - start_time
    log.info(f"--- СТРЕСС-ТЕСТ ЗАВЕРШЕН (Общее время: {total_duration:.2f}с) ---")
    
    failed_tests = []
    for timeframe, success in results:
        if success:
            log.info(f"  [+] УСПЕХ: {timeframe}")
        else:
            log.error(f"  [-] ПРОВАЛ: {timeframe}")
            failed_tests.append(timeframe)

    if failed_tests:
        log.error(f"\nИтог: ПРОВАЛЕНО {len(failed_tests)} из {len(TIMEFRAMES_TO_TEST)} тестов.")
        exit(1) # Выход с кодом ошибки для CI/CD
    else:
        log.info(f"\nИтог: УСПЕХ. Все {len(TIMEFRAMES_TO_TEST)} тестов пройдены.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("\nТест прерван пользователем.")

