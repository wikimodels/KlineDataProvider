import aiohttp
import logging
import time
import asyncio
from typing import Dict, Any, List, Tuple
import json

# --- НАСТРОЙКА ---
BASE_URL = "http://127.0.0.1:8000" # Используйте URL вашего сервера
# --- ИЗМЕНЕНИЕ: Список таймфреймов ---
# Теперь можно добавлять '1h', '12h', '1d'.
# '8h' будет запущен автоматически, если '4h' есть в списке.
TIMEFRAMES_TO_TEST = [ '1h' ]
CACHE_WAIT_TIMEOUT = 1200  # 20 минут
CACHE_POLL_INTERVAL = 15 # Интервал проверки кэша

# --- Настройка логирования (без изменений) ---
class TaskNameFilter(logging.Filter):
    """Добавляет имя задачи asyncio в лог."""
    def filter(self, record):
        try:
            task = asyncio.current_task()
            record.task_name = task.get_name() if task else 'main'
        except RuntimeError:
            record.task_name = 'main'
        return True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(task_name)s] - %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger()
task_filter = TaskNameFilter()
for handler in log.handlers:
    handler.addFilter(task_filter)
# --- Конец настройки логирования ---


def validate_data_collection(data: Dict[str, Any], timeframe: str) -> bool:
    """
    Проверяет данные в 2 ЭТАПА.
    (Код этой функции не изменен, включает вывод кол-ва свечей и примера)
    """
    try:
        # --- БАЗОВАЯ ПРОВЕРКА СТРУКТКРЫ ---
        expected_keys = ["openTime", "closeTime", "timeframe", "data", "audit_report"]
        missing_keys = [k for k in expected_keys if k not in data]
        if missing_keys:
            log.error(f"  -> ПРОВАЛ: Отсутствуют ключи верхнего уровня: {missing_keys}.")
            return False

        # --- ЭТАП 1: Проверка 'audit_report' (Самоотчет сервера) ---
        audit_report = data.get('audit_report')
        if not isinstance(audit_report, dict):
            log.error(f"  -> ПРОВАЛ: 'audit_report' не является словарем. Структура ответа неверна.")
            return False

        log.info("--- Этап 1: Проверка 'audit_report' (отчет сервера) ---")

        missing_klines = audit_report.get("missing_klines", [])
        missing_oi = audit_report.get("missing_oi", [])
        missing_fr = audit_report.get("missing_fr", [])

        audit_report_success = True

        if missing_klines:
            log.error(f"  -> ПРОВАЛ АУДИТА [KLINES]: Сервер сообщает об отсутствующих Klines для {len(missing_klines)} монет: {missing_klines}")
            audit_report_success = False
        else:
            log.info("  -> УСПЕХ АУДИТА [KLINES]: Сервер сообщает, что все Klines на месте.")

        if missing_oi:
            log.error(f"  -> ПРОВАЛ АУДИТА [OI]: Сервер сообщает об отсутствующем Open Interest для {len(missing_oi)} монет: {missing_oi}")
            audit_report_success = False
        else:
            log.info("  -> УСПЕХ АУДИТА [OI]: Сервер сообщает, что весь Open Interest на месте.")

        if missing_fr:
            log.error(f"  -> ПРОВАЛ АУДИТА [FR]: Сервер сообщает об отсутствующем Funding Rate для {len(missing_fr)} монет: {missing_fr}")
            audit_report_success = False
        else:
            log.info("  -> УСПЕХ АУДИТА [FR]: Сервер сообщает, что весь Funding Rate на месте.")

        if not audit_report_success:
            log.error("  -> ПРОВАЛ ЭТАПА 1: 'audit_report' содержит ошибки.")
            # Мы можем вернуть False здесь, но надежнее провести и Этап 2.

        # --- ЭТАП 2: Независимая проверка 3 последних свечей ---
        log.info("\n--- Этап 2: Независимая проверка 3-х последних свечей (OI/FR) ---")
        deep_check_success = True
        data_list = data.get("data", [])
        
        printed_sample_candles = False

        if not data_list and audit_report_success:
             log.warning("  -> ВНИМАНИЕ: 'audit_report' чист, но список 'data' пуст. Проверка невозможна.")
             return False

        for coin_data in data_list:
            symbol = coin_data.get('symbol', 'N/A')
            candles = coin_data.get('data', [])

            log.info(f"  -> ИНФО [{symbol}]: Найдено {len(candles)} свечей.")

            if len(candles) < 3:
                log.warning(f"  -> ВНИМАНИЕ [{symbol}]: Найдено менее 3 свечей ({len(candles)} шт). Проверяю те, что есть.")
                if not candles:
                    log.error(f"  -> ПРОВАЛ [{symbol}]: Монета есть в списке, но у нее 0 свечей.")
                    deep_check_success = False
                    continue

            for i, candle in enumerate(candles[-3:]):
                candle_index = len(candles) - 3 + i
                candle_name = f"T-{2-i}" # (T-2, T-1, T-0)

                if 'openInterest' not in candle:
                    log.error(f"  -> ПРОВАЛ НЕЗАВИСИМОЙ ПРОВЕРКИ [{symbol}]: 'openInterest' отсутствует в свече {candle_name} (Индекс: {candle_index})")
                    deep_check_success = False

                if 'fundingRate' not in candle:
                    log.error(f"  -> ПРОВАЛ НЕЗАВИСИМОЙ ПРОВЕРКИ [{symbol}]: 'fundingRate' отсутствует в свече {candle_name} (Индекс: {candle_index})")
                    deep_check_success = False
            
            if not printed_sample_candles and candles:
                log.info(f"--- Пример свечей (для {symbol}) ---")
                last_two_candles = candles[-2:]
                for i, candle in enumerate(last_two_candles):
                    ot = candle.get('openTime')
                    ct = candle.get('closeTime')
                    o = candle.get('openPrice')
                    h = candle.get('highPrice')
                    l = candle.get('lowPrice')
                    c = candle.get('closePrice')
                    oi = candle.get('openInterest', 'N/A') 
                    fr = candle.get('fundingRate', 'N/A')
                    log.info(f"     [T-{len(last_two_candles)-1-i}]: OT: {ot}, CT: {ct}, O: {o}, H: {h}, L: {l}, C: {c}, OI: {oi}, FR: {fr}")
                printed_sample_candles = True
            
        if deep_check_success:
            log.info("  -> УСПЕХ ЭТАПА 2: Глубокая проверка свечей пройдена. Все ключи OI/FR на месте.")
        else:
             log.error("  -> ПРОВАЛ ЭТАПА 2: В последних 3 свечах отсутствуют ключи OI/FR.")

        return audit_report_success and deep_check_success

    except Exception as e:
        log.error(f"  -> КРИТИЧЕСКАЯ ОШИБКА ВАЛИДАЦИИ: {e}", exc_info=True)
        return False


async def run_single_test(session: aiohttp.ClientSession, timeframe: str) -> Tuple[str, bool]:
    """
    Выполняет полный цикл теста для одного *первичного* таймфрейма:
    1. Запускает задачу (POST)
    2. Ждет кэш (GET)
    3. Валидирует данные
    (Код этой функции не изменен)
    """
    # Устанавливаем имя задачи для логгера
    asyncio.current_task().set_name(f"TF-{timeframe}")

    post_url = f"{BASE_URL}/get-market-data"
    cache_url = f"{BASE_URL}/cache/{timeframe}"
    start_time = time.time()

    try:
        # --- Шаг 1: Запуск задачи (POST) ---
        log.info(f"Шаг 1: Отправка POST-запроса на /get-market-data...")
        post_timeout = aiohttp.ClientTimeout(total=30)
        async with session.post(post_url, json={"timeframe": timeframe}, timeout=post_timeout) as response:

            if response.status == 202:
                log.info(f"Шаг 1 УСПЕХ: Задача принята (Статус 202).")
            elif response.status == 409:
                log.warning(f"Шаг 1 ПРЕДУПРЕЖДЕНИЕ: Задача уже выполняется (Статус 409). Продолжаем ожидание...")
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
    get_timeout = aiohttp.ClientTimeout(total=30) # Таймаут для каждого GET-запроса

    while time.time() - wait_start_time < CACHE_WAIT_TIMEOUT:
        try:
            async with session.get(cache_url, timeout=get_timeout) as response:

                if response.status == 200:
                    log.info(f"Шаг 2 УСПЕХ: Кэш получен (Статус 200)!")

                    # --- Шаг 3: Валидация ---
                    try:
                        json_data = await response.json()
                        log.info(f"Шаг 3: Валидация данных (Этап 1 + Этап 2)...")
                        is_valid = validate_data_collection(json_data, timeframe)

                        total_time = time.time() - start_time
                        log.info(f"Тест завершен за {total_time:.2f}с. Результат: {'УСПЕХ' if is_valid else 'ПРОВАЛ'}")
                        return timeframe, is_valid

                    except json.JSONDecodeError:
                        log.error(f"Шаг 3 ПРОВАЛ: Сервер вернул не-JSON ответ: {await response.text()[:150]}...")
                        return timeframe, False
                    except aiohttp.ContentTypeError:
                         log.error(f"Шаг 3 ПРОВАЛ: Сервер вернул не-JSON content type. Ответ: {await response.text()[:150]}...")
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

    # Если вышли из цикла по таймауту
    log.error(f"Шаг 2 ПРОВАЛ: Таймаут! Кэш не появился за {CACHE_WAIT_TIMEOUT} секунд.")
    return timeframe, False


# --- Изменение №1: Новая функция для зависимых тестов (8h) ---
async def run_cache_check_only(session: aiohttp.ClientSession, timeframe: str) -> Tuple[str, bool]:
    """
    Выполняет цикл теста для *зависимого* таймфрейма (например, 8h).
    Эта функция НЕ делает POST-запрос. Она только ждет и валидирует кэш.
    """
    # Устанавливаем имя задачи для логгера
    asyncio.current_task().set_name(f"TF-{timeframe}")

    cache_url = f"{BASE_URL}/cache/{timeframe}"
    start_time = time.time() # Начинаем отсчет с момента начала проверки

    # --- Шаг 1: Пропускаем POST-запрос ---
    log.info(f"Шаг 1: Пропуск POST-запроса (зависимая задача).")

    # --- Шаг 2: Ожидание кэша (GET) ---
    log.info(f"Шаг 2: Ожидание кэша (Таймаут {CACHE_WAIT_TIMEOUT}с)...")
    wait_start_time = time.time()
    get_timeout = aiohttp.ClientTimeout(total=30)

    while time.time() - wait_start_time < CACHE_WAIT_TIMEOUT:
        try:
            async with session.get(cache_url, timeout=get_timeout) as response:

                if response.status == 200:
                    log.info(f"Шаг 2 УСПЕХ: Кэш получен (Статус 200)!")

                    # --- Шаг 3: Валидация ---
                    try:
                        json_data = await response.json()
                        log.info(f"Шаг 3: Валидация данных (Этап 1 + Этап 2)...")
                        is_valid = validate_data_collection(json_data, timeframe)

                        total_time = time.time() - start_time
                        log.info(f"Тест завершен за {total_time:.2f}с. Результат: {'УСПЕХ' if is_valid else 'ПРОВАЛ'}")
                        return timeframe, is_valid

                    except json.JSONDecodeError:
                        log.error(f"Шаг 3 ПРОВАЛ: Сервер вернул не-JSON ответ: {await response.text()[:150]}...")
                        return timeframe, False
                    except aiohttp.ContentTypeError:
                         log.error(f"Шаг 3 ПРОВАЛ: Сервер вернул не-JSON content type. Ответ: {await response.text()[:150]}...")
                         return timeframe, False

                elif response.status == 404:
                    log.info(f"Шаг 2 ОЖИДАНИЕ: Кэш пока пуст (404). Проверка через {CACHE_POLL_INTERVAL}с...")
                else:
                    log.warning(f"Шаг 2 ОШИБКА ОЖИДАНИЯ: Сервер вернул {response.status}. Повтор...")

        except asyncio.TimeoutError:
            log.warning(f"Шаг 2 ОШИБКА ОЖИДАНИЯ: Таймаут GET-запроса. Повтор...")
        except aiohttp.ClientError as e:
            log.warning(f"Шаг 2 ОШИБКА ОЖИДАНИЯ: Ошибка GET-запроса: {e}. Повтор...")

        await asyncio.sleep(CACHE_POLL_INTERVAL)

    # Если вышли из цикла по таймауту
    log.error(f"Шаг 2 ПРОВАЛ: Таймаут! Кэш не появился за {CACHE_WAIT_TIMEOUT} секунд.")
    return timeframe, False
# --- Конец Изменения №1 ---


# --- Изменение №1: Полностью переписанная main_test ---
async def main_test():
    """
    Запускает тесты, управляя зависимостями (8h от 4h).
    """
    log.info(f"--- ЗАПУСК ТЕСТА С УЧЕТОМ ЗАВИСИМОСТЕЙ ---")
    log.info(f"Цель: {BASE_URL}")
    log.info(f"Таймфреймы в конфиге: {TIMEFRAMES_TO_TEST}")
    log.info(f"Общий таймаут на задачу: {CACHE_WAIT_TIMEOUT}с")

    start_time = time.time()
    
    # Карта зависимостей: 'зависимый_тф': 'первичный_тф'
    dependent_map = {'8h': '4h'}
    
    # 1. Разделяем задачи
    # Уникальный набор всех ТФ, которые мы должны проверить
    all_tasks_to_run = set(TIMEFRAMES_TO_TEST)
    
    primary_tasks_tf = []
    dependent_tasks_tf = []
    
    # Автоматически добавляем 8h, если 4h есть в списке (как ты и просил)
    if '4h' in all_tasks_to_run and '8h' not in all_tasks_to_run:
        log.info("Обнаружен '4h'. Автоматически добавляю '8h' в список проверок.")
        all_tasks_to_run.add('8h')

    # Распределяем задачи
    for tf in all_tasks_to_run:
        dependency = dependent_map.get(tf)
        if dependency and dependency in all_tasks_to_run:
            # Это зависимая задача (8h) и ее "родитель" (4h) тоже в списке
            dependent_tasks_tf.append(tf)
        elif dependency and dependency not in all_tasks_to_run:
            # Зависимая задача (8h) есть, а ее "родителя" (4h) нет
            log.warning(f"ПРЕДУПРЕЖДЕНИЕ: {tf} требует {dependency}, но {dependency} нет в списке. {tf} будет пропущен.")
        else:
            # Это первичная задача (1h, 4h, 12h, 1d)
            primary_tasks_tf.append(tf)

    log.info(f"Первичные задачи (параллельно): {primary_tasks_tf}")
    log.info(f"Зависимые задачи (последовательно): {dependent_tasks_tf}")

    session_timeout = aiohttp.ClientTimeout(total=CACHE_WAIT_TIMEOUT + 60)
    results_map = {} # Словарь для хранения итогов: {'4h': True, '1h': False, ...}

    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        
        # 2. Запускаем первичные задачи параллельно
        if primary_tasks_tf:
            log.info("--- ЗАПУСК ПЕРВИЧНЫХ ЗАДАЧ ---")
            async_tasks = [run_single_test(session, tf) for tf in primary_tasks_tf]
            primary_results = await asyncio.gather(*async_tasks)
            results_map.update(dict(primary_results))
        else:
            log.warning("Нет первичных задач для запуска.")

        # 3. Запускаем зависимые задачи последовательно
        if dependent_tasks_tf:
            log.info("--- ЗАПУСК ЗАВИСИМЫХ ЗАДАЧ ---")
            for tf in dependent_tasks_tf:
                dependency = dependent_map[tf] # (e.g., '4h')
                
                # Проверяем, что "родитель" был запущен и прошел успешно
                if results_map.get(dependency) is True:
                    log.info(f"Тест '{dependency}' прошел. Запускаю зависимую проверку для '{tf}'...")
                    # Запускаем проверку *только* кэша
                    tf_result, success = await run_cache_check_only(session, tf)
                    results_map[tf_result] = success
                else:
                    log.error(f"Тест '{dependency}' ПРОВАЛЕН или не был запущен. Зависимая проверка для '{tf}' пропущена.")
                    results_map[tf] = False # Отмечаем зависимый тест как проваленный
        
    total_duration = time.time() - start_time
    log.info(f"--- ТЕСТ ЗАВЕРШЕН (Общее время: {total_duration:.2f}с) ---")

    # 4. Подводим итоги
    failed_tests = []
    success_count = 0
    total_tests_run = len(all_tasks_to_run)

    for timeframe in all_tasks_to_run:
        success = results_map.get(timeframe, False) # Считаем False, если ключа нет
        if success:
            log.info(f"  [+] УСПЕХ: {timeframe}")
            success_count += 1
        else:
            log.error(f"  [-] ПРОВАЛ: {timeframe}")
            failed_tests.append(timeframe)

    if failed_tests:
        log.error(f"\nИтог: ПРОВАЛЕНО {len(failed_tests)} из {total_tests_run} тестов ({', '.join(failed_tests)}).")
        exit(1)
    else:
        log.info(f"\nИтог: УСПЕХ. Все {total_tests_run} тестов пройдены.")
# --- Конец Изменения №1 ---


if __name__ == "__main__":
    try:
        asyncio.run(main_test())
    except KeyboardInterrupt:
        log.warning("\nТест прерван пользователем.")

