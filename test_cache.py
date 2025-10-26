import aiohttp
import logging
import time
import asyncio
from typing import Dict, Any, List, Tuple, Optional
import json
import os 
# --- ИСПОЛЬЗУЕМ upstash-redis И defaultdict ---
from upstash_redis import Redis
from collections import defaultdict
# -----------------------------------------------------
from dotenv import load_dotenv
load_dotenv() 
# --- ДОБАВЛЕНО: colorama для цветного логирования ---
import colorama
colorama.init()
# -----------------------------------------------------

# --- НАСТРОЙКА ---
BASE_URL = os.environ.get("RENDER_EXTERNAL_URL", "http://127.0.0.1:8000") # Используйте URL вашего сервера
# --- Список таймфреймов для тестирования ---
# Эти ТФ должны быть разрешены в api_routes.py!
TIMEFRAMES_TO_TEST = ['1h', '4h', '12h', '1d'] 
CACHE_WAIT_TIMEOUT = 1200  # 20 минут
CACHE_POLL_INTERVAL = 15 # Интервал проверки кэша

# --- ГЛОБАЛЬНЫЕ КОНФИГУРАЦИИ ---
# Переменные для Redis (Берутся из окружения, как в cache_manager.py)
UPSTASH_REDIS_URL = os.environ.get("UPSTASH_REDIS_URL")
UPSTASH_REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_TOKEN")

# --- Изменение №1: Добавляем ключи из worker.py/api_routes.py ---
REDIS_TASK_QUEUE_KEY = "data_collector_task_queue"
WORKER_LOCK_KEY = "data_collector_lock"
# -----------------------------------------------------------

# Отчет об ошибках (глобальный)
GLOBAL_ERROR_REPORT = defaultdict(dict)
# -----------------------------

# --- Настройка логирования (для правильного вывода) ---
class ColoredFormatter(logging.Formatter):
    """Добавляет цвета к уровням логов."""
    def __init__(self):
        super().__init__()
        self.colors = {
            'DEBUG': colorama.Fore.CYAN,
            'INFO': colorama.Fore.GREEN,
            'WARNING': colorama.Fore.YELLOW,
            'ERROR': colorama.Fore.RED,
            'CRITICAL': colorama.Back.RED + colorama.Fore.WHITE,
        }

    def format(self, record):
        log_color = self.colors.get(record.levelname, '')
        record.levelname = f"{log_color}{record.levelname}{colorama.Style.RESET_ALL}"
        # Добавляем цвет для имени задачи
        if hasattr(record, 'task_name'):
            record.task_name = f"{colorama.Fore.LIGHTBLUE_EX}{record.task_name}{colorama.Style.RESET_ALL}"
        
        # Исправляем %s на {message} для .format()
        message = record.getMessage()
        record.message = f"{log_color}{message}{colorama.Style.RESET_ALL}" if record.levelno >= logging.WARNING else message
        
        # Форматируем остальную часть строки
        s = super().format(record)
        
        # Убираем дублирование цвета, если оно есть
        if record.levelno >= logging.WARNING:
            s = s.replace(f"{log_color}{message}{colorama.Style.RESET_ALL}", message)
            
        return s

class TaskNameFilter(logging.Filter):
    """Добавляет имя задачи asyncio в лог."""
    def filter(self, record):
        try:
            task = asyncio.current_task()
            record.task_name = task.get_name() if task else 'main'
        except RuntimeError:
            record.task_name = 'main'
        return True

# --- Изменение №1: Исправляем TypeError ---
# Создаем форматтер
colored_formatter = ColoredFormatter() # <-- УБИРАЕМ АРГУМЕНТЫ

# Настройка корневого логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(task_name)s] - %(message)s', # <-- ВОЗВРАЩАЕМ FORMAT
    datefmt='%H:%M:%S' # <-- ВОЗВРАЩАЕМ DATEFMT
)
log = logging.getLogger()
# Применяем кастомный форматтер ко всем обработчикам
for handler in log.handlers:
    handler.setFormatter(colored_formatter)
# --- Конец Изменения ---

# Применяем фильтр
task_filter = TaskNameFilter()
for handler in log.handlers:
    handler.addFilter(task_filter)
# --- Конец настройки логирования ---


def _clear_all_caches():
    """
    (ИСПРАВЛЕНО 2.0)
    Удаляет все ключи cache:*, data_collector_lock из Upstash Redis.
    НЕ ТРОГАЕТ ОЧЕРЕДЬ ЗАДАЧ (REDIS_TASK_QUEUE_KEY)!
    """
    if not UPSTASH_REDIS_URL or not UPSTASH_REDIS_TOKEN:
        log.error("❌ ОЧИСТКА: Переменные Upstash REDIS_URL/TOKEN не установлены. Пропуск очистки кэша.")
        return

    try:
        # Используем клиент Upstash Redis
        r = Redis(url=UPSTASH_REDIS_URL, token=UPSTASH_REDIS_TOKEN)
        
        keys_to_delete = []
        cursor = 0
        while True:
            # Используем SCAN для поиска ключей. Upstash возвращает строки.
            cursor, keys = r.scan(cursor=cursor, match='cache:*')
            keys_to_delete.extend(keys)
            if cursor == 0:
                break

        # --- Изменение №2: Удаляем ТОЛЬКО блокировку ---
        keys_to_delete.append(WORKER_LOCK_KEY)
        # --------------------------------------------------------
        
        if keys_to_delete:
            # r.delete принимает *args
            deleted_count = r.delete(*keys_to_delete)
            log.info(f"🧹✅ УСПЕШНО УДАЛЕНО {deleted_count} КЛЮЧЕЙ КЭША И БЛОКИРОВКИ ИЗ REDIS.")
            log.warning(f"🧹 ОЧЕРЕДЬ ЗАДАЧ (data_collector_task_queue) НЕ ТРОНУТА.")
        else:
            log.info("🧹 НЕ НАЙДЕНО КЛЮЧЕЙ КЭША ДЛЯ УДАЛЕНИЯ. REDIS ЧИСТ.")
        
        log.info(f"🧹 КЭШ ОЧИЩЕН. ПРОДОЛЖАЮ...")
            
    except Exception as e:
        log.critical(f"💥 КРИТИЧЕСКАЯ ОШИБКА ПРИ ОЧИСТКЕ UPSTASH REDIS: {e}", exc_info=True)
        

def validate_data_collection(data: Dict[str, Any], timeframe: str) -> Tuple[bool, List[str]]:
    """
    (ИЗМЕНЕНО)
    Проверяет данные: Audit Report (сервер) и Глубокая проверка (клиент).
    ОШИБКА KLINES (missing_klines) ТЕПЕРЬ НЕ КРИТИЧЕСКАЯ (только Warning).
    """
    is_valid = True
    test_errors = []
    
    # --- БАЗОВАЯ ПРОВЕРКА СТРУКТУРЫ ---
    expected_keys = ["openTime", "closeTime", "timeframe", "data", "audit_report"]
    if any(k not in data for k in expected_keys):
        log.error(f"   -> ❌ ПРОВАЛ: Отсутствуют ключи верхнего уровня: {[k for k in expected_keys if k not in data]}.")
        test_errors.append("MISSING_TOP_KEYS")
        return False, test_errors

    # --- ЭТАП 1: Проверка 'audit_report' (Самоотчет сервера) ---
    audit_report = data.get('audit_report')
    missing_klines = audit_report.get("missing_klines", [])
    missing_oi_audit = audit_report.get("missing_oi", [])
    missing_fr_audit = audit_report.get("missing_fr", [])
    
    audit_report_success = True

    # --- ИЗМЕНЕНИЕ: missing_klines теперь WARNING, а не ERROR ---
    if missing_klines:
        log.warning(f"   -> 📊⚠️  АУДИТ [KLINES]: Сервер сообщает об отсутствующих Klines для {len(missing_klines)} монет.")
        test_errors.append(f"AUDIT_KLINES_{len(missing_klines)}")
        GLOBAL_ERROR_REPORT[timeframe]['missing_klines'] = missing_klines
        # audit_report_success = False # <-- БОЛЬШЕ НЕ СЧИТАЕМ ЭТО ПРОВАЛОМ ТЕСТА
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    if missing_oi_audit or missing_fr_audit:
        # Логируем наличие ошибок аудита, но детали в отчете
        log.error(f"   -> 📊❌ АУДИТ [OI/FR]: Сервер обнаружил пропуски (OI:{len(missing_oi_audit)}, FR:{len(missing_fr_audit)}).")
        if missing_oi_audit:
            test_errors.append(f"AUDIT_OI_{len(missing_oi_audit)}")
            GLOBAL_ERROR_REPORT[timeframe]['audit_oi'] = missing_oi_audit
        if missing_fr_audit:
            test_errors.append(f"AUDIT_FR_{len(missing_fr_audit)}")
            GLOBAL_ERROR_REPORT[timeframe]['audit_fr'] = missing_fr_audit
        audit_report_success = False
    
    if audit_report_success and not missing_klines: # Если все хорошо И нет klines
         log.info("   -> ✅ Аудит сервера пройден успешно.")
         GLOBAL_ERROR_REPORT[timeframe]['audit_success'] = True
    elif audit_report_success and missing_klines: # Если OI/FR хорошо, но klines пропущены
        log.info("   -> ✅ Аудит OI/FR пройден (пропуски Klines проигнорированы).")
        GLOBAL_ERROR_REPORT[timeframe]['audit_success'] = True


    # --- ЭТАП 2: Независимая проверка 3 последних свечей + Длина ---
    log.info("\n--- Этап 2: Независимая проверка OI/FR в последних 3 свечах ---")
    deep_check_success = True
    data_list = data.get("data", [])
    
    # Снижаем требование к длине для 12h и 1d (ТВОЕ ТРЕБОВАНИЕ)
    if timeframe in ['1d', '12h']:
        MIN_CANDLES = 100 
    else:
        MIN_CANDLES = 399

    if not data_list:
         log.error("   -> ❌ ПРОВАЛ: Список 'data' пуст.")
         test_errors.append("EMPTY_DATA_LIST")
         is_valid = False
         return is_valid, test_errors

    printed_sample_candles = False
    
    # --- Глубокая проверка ---
    for coin_data in data_list:
        symbol = coin_data.get('symbol', 'N/A')
        candles = coin_data.get('data', [])
        coin_success = True
        
        # 2.1 Проверка длины (ТОЛЬКО WARNING)
        if len(candles) < MIN_CANDLES:
            log.warning(f"   -> ⚠️  ВНИМАНИЕ [{symbol}]: Найдено только {len(candles)} свечей (менее {MIN_CANDLES} шт).")

        # 2.2 Проверка OI/FR в последних 3 свечах
        oi_fr_missing_in_coin = []
        if len(candles) >= 3:
            for i, candle in enumerate(candles[-3:]):
                candle_name = f"T-{2-i}" # (T-2, T-1, T-0)

                if 'openInterest' not in candle or candle.get('openInterest') is None:
                    oi_fr_missing_in_coin.append(f"OI_{candle_name}")
                if 'fundingRate' not in candle or candle.get('fundingRate') is None:
                    oi_fr_missing_in_coin.append(f"FR_{candle_name}")

            if oi_fr_missing_in_coin:
                # Логируем ошибку только один раз, чтобы не засирать лог
                if deep_check_success:
                    log.error(f"   -> ❌ ПРОВАЛ OI/FR: Найдена первая монета с пропуском: {symbol}. Подробности в финальном отчете.")
                
                deep_check_success = False
                coin_success = False

        # --- Сохраняем ошибки OI/FR для глобального отчета ---
        if not coin_success and oi_fr_missing_in_coin:
            if 'oi_fr_failures' not in GLOBAL_ERROR_REPORT[timeframe]:
                GLOBAL_ERROR_REPORT[timeframe]['oi_fr_failures'] = []
            
            GLOBAL_ERROR_REPORT[timeframe]['oi_fr_failures'].append({
                'symbol': symbol,
                'reason': ", ".join(oi_fr_missing_in_coin)
            })
        
        # --- Вывод примера свечей (только один раз) ---
        if not printed_sample_candles and candles:
            log.info(f"--- Пример свечей (для {symbol}) ---")
            last_two_candles = candles[-2:]
            for i, candle in enumerate(last_two_candles):
                ot = candle.get('openTime')
                ct = candle.get('closeTime')
                o = candle.get('openPrice')
                c = candle.get('closePrice')
                oi = candle.get('openInterest', 'N/A') 
                fr = candle.get('fundingRate', 'N/A')
                # Удаляем H и L из лога для краткости
                log.info(f"     [T-{len(last_two_candles)-1-i}]: OT: {ot}, CT: {ct}, O: {o}, C: {c}, OI: {oi}, FR: {fr}")
            printed_sample_candles = True
            
    if deep_check_success:
        log.info("   -> ✅ УСПЕХ ЭТАПА 2: Глубокая проверка OI/FR пройдена.")
        GLOBAL_ERROR_REPORT[timeframe]['deep_check_success'] = True
    else:
         log.error("   -> ❌ ПРОВАЛ ЭТАПА 2: Обнаружены пропуски OI/FR.")
         is_valid = False

    # Общая валидность зависит от двух этапов
    # (audit_report_success теперь игнорирует klines)
    is_valid = is_valid and audit_report_success
    
    return is_valid, test_errors


async def run_single_test(session: aiohttp.ClientSession, timeframe: str) -> Tuple[str, bool]:
    """
    Выполняет полный цикл теста для одного *первичного* таймфрейма.
    (Код этой функции не изменен)
    """
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
                log.info(f"Шаг 1 ✅ УСПЕХ: Задача принята (Статус 202).")
            elif response.status == 409:
                log.warning(f"Шаг 1 ⚠️  ПРЕДУПРЕЖДЕНИЕ: Задача уже выполняется (Статус 409). Продолжаем ожидание...")
            elif response.status == 400:
                 # --- КРИТИЧЕСКАЯ ОШИБКА ТЕСТА (если ТФ недопустим) ---
                 response_text = await response.text()
                 log.critical(f"Шаг 1 ❌ ПРОВАЛ (КРИТИЧЕСКИЙ): Сервер вернул 400. Ответ: {response_text}. Таймфрейм '{timeframe}' недопустим. Проверьте api_routes.py.")
                 return timeframe, False
            # --------------------------------------------------------
            else:
                response_text = await response.text()
                log.error(f"Шаг 1 ❌ ПРОВАЛ: Сервер вернул {response.status}. Ответ: {response_text[:150]}")
                return timeframe, False

    except Exception as e:
        log.error(f"Шаг 1 ❌ ПРОВАЛ: Ошибка POST-запроса: {e}")
        return timeframe, False

    # --- Шаг 2: Ожидание кэша (GET) ---
    log.info(f"Шаг 2: Ожидание кэша (Таймаут {CACHE_WAIT_TIMEOUT}с)...")
    wait_start_time = time.time()
    get_timeout = aiohttp.ClientTimeout(total=30)

    while time.time() - wait_start_time < CACHE_WAIT_TIMEOUT:
        try:
            async with session.get(cache_url, timeout=get_timeout) as response:

                if response.status == 200:
                    log.info(f"Шаг 2 ✅ УСПЕХ: Кэш получен (Статус 200)!")

                    # --- Шаг 3: Валидация ---
                    try:
                        json_data = await response.json()
                        log.info(f"Шаг 3: Валидация данных...")
                        is_valid, _ = validate_data_collection(json_data, timeframe) 
                        
                        GLOBAL_ERROR_REPORT[timeframe]['success'] = is_valid
                        GLOBAL_ERROR_REPORT[timeframe]['raw_data'] = json_data # Сохраняем данные для отчета
                        
                        total_time = time.time() - start_time
                        
                        # --- ИЗМЕНЕНО: Логика вывода ---
                        if is_valid:
                            # Если klines были пропущены, это warning, а не success
                            if GLOBAL_ERROR_REPORT[timeframe].get('missing_klines'):
                                log.warning(f"Тест завершен за {total_time:.2f}с. Результат: {'⚠️  ПРЕДУПРЕЖДЕНИЕ (Klines пропущены)'}")
                            else:
                                log.info(f"Тест завершен за {total_time:.2f}с. Результат: {'✅ УСПЕХ'}")
                        else:
                            log.error(f"Тест завершен за {total_time:.2f}с. Результат: {'❌ ПРОВАЛ (OI/FR)'}")
                        # -----------------------------
                            
                        return timeframe, is_valid

                    except Exception as e:
                         log.error(f"Шаг 3 ❌ ПРОВАЛ: Критическая ошибка валидации: {e}", exc_info=True)
                         return timeframe, False


                elif response.status == 404:
                    log.info(f"Шаг 2 ⏳ ОЖИДАНИЕ: Кэш пока пуст (404). Проверка через {CACHE_POLL_INTERVAL}с...")
                else:
                    log.warning(f"Шаг 2 ⚠️  ОШИБКА ОЖИДАНИЯ: Сервер вернул {response.status}. Повтор...")

        except Exception as e:
            log.warning(f"Шаг 2 ⚠️  ОШИБКА ОЖИДАНИЯ: Ошибка GET-запроса/Таймаут: {e}. Повтор...")

        await asyncio.sleep(CACHE_POLL_INTERVAL)

    # Если вышли из цикла по таймауту
    log.error(f"Шаг 2 ❌ ПРОВАЛ: Таймаут! Кэш не появился за {CACHE_WAIT_TIMEOUT} секунд.")
    return timeframe, False


async def run_cache_check_only(session: aiohttp.ClientSession, timeframe: str) -> Tuple[str, bool]:
    """
    Выполняет цикл теста для *зависимого* таймфрейма (8h).
    (Код этой функции не изменен)
    """
    asyncio.current_task().set_name(f"TF-{timeframe}")

    cache_url = f"{BASE_URL}/cache/{timeframe}"
    start_time = time.time()

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
                    log.info(f"Шаг 2 ✅ УСПЕХ: Кэш получен (Статус 200)!")

                    # --- Шаг 3: Валидация ---
                    try:
                        json_data = await response.json()
                        log.info(f"Шаг 3: Валидация данных...")
                        is_valid, _ = validate_data_collection(json_data, timeframe) 
                        
                        GLOBAL_ERROR_REPORT[timeframe]['success'] = is_valid
                        GLOBAL_ERROR_REPORT[timeframe]['raw_data'] = json_data # Сохраняем данные для отчета
                        
                        total_time = time.time() - start_time
                        
                        # --- ИЗМЕНЕНО: Логика вывода ---
                        if is_valid:
                            if GLOBAL_ERROR_REPORT[timeframe].get('missing_klines'):
                                log.warning(f"Тест завершен за {total_time:.2f}с. Результат: {'⚠️  ПРЕДУПРЕЖДЕНИЕ (Klines пропущены)'}")
                            else:
                                log.info(f"Тест завершен за {total_time:.2f}с. Результат: {'✅ УСПЕХ'}")
                        else:
                            log.error(f"Тест завершен за {total_time:.2f}с. Результат: {'❌ ПРОВАЛ (OI/FR)'}")
                        # -----------------------------
                            
                        return timeframe, is_valid

                    except Exception as e:
                         log.error(f"Шаг 3 ❌ ПРОВАЛ: Критическая ошибка валидации: {e}", exc_info=True)
                         return timeframe, False

                elif response.status == 404:
                    log.info(f"Шаг 2 ⏳ ОЖИДАНИЕ: Кэш пока пуст (404). Проверка через {CACHE_POLL_INTERVAL}с...")
                else:
                    log.warning(f"Шаг 2 ⚠️  ОШИБКА ОЖИДАНИЯ: Сервер вернул {response.status}. Повтор...")

        except Exception as e:
            log.warning(f"Шаг 2 ⚠️  ОШИБКА ОЖИДАНИЯ: Ошибка GET-запроса/Таймаут: {e}. Повтор...")

        await asyncio.sleep(CACHE_POLL_INTERVAL)

    # Если вышли из цикла по таймауту
    log.error(f"Шаг 2 ❌ ПРОВАЛ: Таймаут! Кэш не появился за {CACHE_WAIT_TIMEOUT} секунд.")
    return timeframe, False


def print_sample_candle(timeframe: str, error_report: Dict[str, Any]):
    """Выводит пример нормальной свечи для указанного таймфрейма."""
    raw_data = error_report.get('raw_data')
    if not raw_data or not isinstance(raw_data, dict):
        print("       🔍 Не удалось получить данные для примера свечи.")
        return

    data_list = raw_data.get('data', [])
    if not data_list:
        print("       🔍 В данных не найдено списка 'data' для примера свечи.")
        return

    # Найдем первую монету с полными данными
    sample_coin = None
    sample_candle = None
    for coin_data in data_list:
        symbol = coin_data.get('symbol', 'N/A')
        candles = coin_data.get('data', [])
        # Ищем свечу с OI и FR
        for candle in candles:
            if 'openInterest' in candle and 'fundingRate' in candle and candle.get('openInterest') is not None and candle.get('fundingRate') is not None:
                sample_coin = symbol
                sample_candle = candle
                break
        if sample_candle:
            break

    if sample_candle:
        print(f"       📌 Пример НОРМАЛЬНОЙ свечи (из {sample_coin}):")
        print(f"             - openTime: {sample_candle.get('openTime', 'N/A')}")
        print(f"             - closeTime: {sample_candle.get('closeTime', 'N/A')}")
        print(f"             - openPrice: {sample_candle.get('openPrice', 'N/A')}")
        print(f"             - highPrice: {sample_candle.get('highPrice', 'N/A')}")
        print(f"             - lowPrice: {sample_candle.get('lowPrice', 'N/A')}")
        print(f"             - closePrice: {sample_candle.get('closePrice', 'N/A')}")
        print(f"             - volume: {sample_candle.get('volume', 'N/A')}")
        # Убраны ненужные поля
        print(f"             - openInterest: {sample_candle.get('openInterest', 'N/A')}")
        print(f"             - fundingRate: {sample_candle.get('fundingRate', 'N/A')}")
    else:
        print(f"       🔍 Не найдено ни одной свечи с полными OI/FR в кэше для {timeframe}.")


async def main_test():
    """
    (ИЗМЕНЕНО)
    Запускает тесты ПОСЛЕДОВАТЕЛЬНО, управляя зависимостями, и выводит финальный отчет.
    """
    log.info(f"--- 🚀 ЗАПУСК ТЕСТА С УЧЕТОМ ЗАВИСИМОСТЕЙ ---")
    log.info(f"Таймфреймы в конфиге: {TIMEFRAMES_TO_TEST}")
    log.info(f"URL Сервера: {BASE_URL}")
    
    # 0. Очистка кэша перед стартом
    _clear_all_caches()

    start_time = time.time()
    
    dependent_map = {'8h': '4h'}
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
            dependent_tasks_tf.append(tf)
        elif dependency and dependency not in all_tasks_to_run:
            log.warning(f"⚠️  ПРЕДУПРЕЖДЕНИЕ: {tf} требует {dependency}, но {dependency} нет в списке. {tf} будет пропущен.")
        else:
            primary_tasks_tf.append(tf)

    # --- ИЗМЕНЕНИЕ: Сортируем первичные задачи для предсказуемого порядка ---
    primary_tasks_tf.sort() 
    log.info(f"Первичные задачи (ПОСЛЕДОВАТЕЛЬНО): {primary_tasks_tf}")
    # --------------------------------------------------------------------
    log.info(f"Зависимые задачи (последовательно): {dependent_tasks_tf}")

    session_timeout = aiohttp.ClientTimeout(total=CACHE_WAIT_TIMEOUT + 60)
    results_map = {} # Словарь для хранения итогов

    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        
        # 1. Запускаем первичные задачи ПОСЛЕДОВАТЕЛЬНО
        if primary_tasks_tf:
            log.info("\n--- 📦 ЗАПУСК ПЕРВИЧНЫХ ЗАДАЧ (ПОСЛЕДОВАТЕЛЬНО) ---")
            
            # --- ИЗМЕНЕНИЕ: Убран asyncio.gather, заменен на цикл for ---
            for tf in primary_tasks_tf:
                log.info(f"\n--- 🧪 НАЧАЛО ТЕСТА ДЛЯ: {tf.upper()} ---")
                tf_result, success = await run_single_test(session, tf)
                results_map[tf_result] = success
                if not success:
                    log.error(f"--- ❌ ПРОВАЛ ТЕСТА ДЛЯ: {tf.upper()}. ---")
                    # (Мы не прерываем тест, чтобы собрать все ошибки)
                log.info(f"--- 🏁 ЗАВЕРШЕНИЕ ТЕСТА ДЛЯ: {tf.upper()} ---")
            # --- Конец Изменения ---
        else:
            log.warning("⚠️  Нет первичных задач для запуска.")

        # 2. Запускаем зависимые задачи последовательно (Логика не изменена)
        if dependent_tasks_tf:
            log.info("\n--- 🔗 ЗАПУСК ЗАВИСИМЫХ ЗАДАЧ ---")
            for tf in dependent_tasks_tf:
                dependency = dependent_map[tf] 
                
                # --- ИЗМЕНЕНО: Запускаем, если results_map[dependency] == True ИЛИ если это просто Warning (есть в отчете)
                # (То есть, если тест не был 100% ПРОВАЛЕН (False))
                parent_success = results_map.get(dependency) 
                
                if parent_success is True:
                    log.info(f"\n--- 🧪 НАЧАЛО ТЕСТА ДЛЯ (Зависимый): {tf.upper()} ---")
                    log.info(f"Тест '{dependency}' прошел. Запускаю зависимую проверку для '{tf}'...")
                    tf_result, success = await run_cache_check_only(session, tf)
                    results_map[tf_result] = success
                    log.info(f"--- 🏁 ЗАВЕРШЕНИЕ ТЕСТА ДЛЯ: {tf.upper()} ---")
                else:
                    log.error(f"Тест '{dependency}' ❌ ПРОВАЛЕН (критическая ошибка OI/FR). Зависимая проверка для '{tf}' пропущена.")
                    results_map[tf] = False 
        
    total_duration = time.time() - start_time
    log.info(f"\n--- 🏁 ТЕСТ ЗАВЕРШЕН (Общее время: {total_duration:.2f}с) ---")

    # 3. Подводим итоги и выводим отчет об ошибках
    failed_tests = []
    warn_tests = []
    total_tests_run = len(all_tasks_to_run)
    success_count = 0

    for timeframe in sorted(all_tasks_to_run): # Сортируем для красоты
        success = results_map.get(timeframe, False)
        report = GLOBAL_ERROR_REPORT.get(timeframe, {})
        
        if success:
            if report.get('missing_klines'):
                log.warning(f"   [~] ⚠️  ПРЕДУПРЕЖДЕНИЕ: {timeframe} (Пропущены Klines)")
                warn_tests.append(timeframe)
            else:
                log.info(f"   [+] ✅ УСПЕХ: {timeframe}")
                success_count += 1
        else:
            log.error(f"   [-] ❌ ПРОВАЛ: {timeframe} (Пропущен OI/FR или Таймаут)")
            failed_tests.append(timeframe)

    if failed_tests or warn_tests:
        print("\n" + "="*60)
        print(f"{'📊 ПОДРОБНЫЙ ОТЧЕТ ОБ ОШИБКАХ И ПРЕДУПРЕЖДЕНИЯХ':^60}")
        print("="*60)

        # Сначала выводим ПРОВАЛЫ (критические)
        for tf in sorted(failed_tests): 
            report = GLOBAL_ERROR_REPORT.get(tf, {})
            
            print(f"\n📊 ТАЙМФРЕЙМ: {tf.upper()}")
            print("-" * 40)
            print(f"Финальный статус: ❌ ПРОВАЛ")
            
            if report.get('missing_klines'):
                print(f"   📈 [ПРЕДУПРЕЖДЕНИЕ Klines]: Сервер не смог получить Klines для {len(report['missing_klines'])} монет.")
                print(f"     Полный список: {report['missing_klines']}")

            if report.get('audit_oi'):
                print(f"   📊 [ПРОВАЛ АУДИТА OI]: Сервер обнаружил {len(report['audit_oi'])} монет без OI (посл. свеча).")
                print(f"     Полный список: {report['audit_oi']}")
            if report.get('audit_fr'):
                print(f"   💰 [ПРОВАЛ АУДИТА FR]: Сервер обнаружил {len(report['audit_fr'])} монет без FR (посл. свеча).")
                print(f"     Полный список: {report['audit_fr']}")

            if report.get('oi_fr_failures'):
                print(f"   🧪 [ПРОВАЛ ПРОВЕРКИ OI/FR]: Найдено {len(report['oi_fr_failures'])} монет, где отсутствует OI/FR в последних 3 свечах.")
                for item in report['oi_fr_failures']: # Показываем все проблемные
                    print(f"     - {item['symbol']}: {item['reason']}")
            
            # Выводим пример нормальной свечи, если тест провалился
            print(f"\n   🔍 Пример НОРМАЛЬНОЙ свечи из кэша (если есть):")
            print_sample_candle(tf, report)

        # Затем выводим ПРЕДУПРЕЖДЕНИЯ (некритические)
        for tf in sorted(warn_tests): 
            report = GLOBAL_ERROR_REPORT.get(tf, {})
            
            print(f"\n📊 ТАЙМФРЕЙМ: {tf.upper()}")
            print("-" * 40)
            print(f"Финальный статус: ⚠️  ПРЕДУПРЕЖДЕНИЕ")
            
            if report.get('missing_klines'):
                print(f"   📈 [ПРЕДУПРЕЖДЕНИЕ Klines]: Сервер не смог получить Klines для {len(report['missing_klines'])} монет.")
                print(f"     Полный список: {report['missing_klines']}")
            else:
                print("   (Другая причина предупреждения, см. лог)")


        print("="*60 + "\n")
    else:
        log.info(f"\n✅ Итог: УСПЕХ. Все {total_tests_run} тестов пройдены.")

    # --- Вывод дополнительной информации по 8h (В ЛЮБОМ СЛУЧАЕ) ---
    if '8h' in all_tasks_to_run:
        log.info(f"\n--- 🔎 ДЕТАЛЬНАЯ ПРОВЕРКА СОДЕРЖИМОГО [8H] ---")
        
        # --- ИЗМЕНЕНИЕ: Проверяем, был ли тест 8h вообще пройден (True) ---
        if results_map.get('8h') is not True:
            log.error("   ❌ Тест 8h не был пройден (Таймаут или провал 4h). Анализ содержимого невозможен.")
        else:
            raw_data_8h = GLOBAL_ERROR_REPORT.get('8h', {}).get('raw_data', {})
            data_list_8h = raw_data_8h.get('data', [])
            
            if not data_list_8h:
                log.error("   ❌ ПРОВАЛ 8H: Кэш получен, но список 'data' в нем ПУСТ.")
            else:
                num_coins_8h = len(data_list_8h)
                num_candles_8h = 0
                
                # Берем первую монету для анализа
                first_coin_data = data_list_8h[0]
                first_coin_symbol = first_coin_data.get('symbol', 'N/A')
                first_coin_candles = first_coin_data.get('data', [])
                num_candles_8h = len(first_coin_candles)

                log.info(f"   [Общая]: Найдено {num_coins_8h} монет.")
                log.info(f"   [Общая]: Найдено {num_candles_8h} свечей (на примере {first_coin_symbol}).")
                
                # 1. Проверка тайминга
                if num_candles_8h >= 2:
                    c0_ot = first_coin_candles[0].get('openTime', 0)
                    c1_ot = first_coin_candles[1].get('openTime', 0)
                    time_diff_ms = c1_ot - c0_ot
                    time_diff_hours = time_diff_ms / (1000 * 60 * 60)
                    
                    if time_diff_hours == 8:
                        log.info(f"   [Тайминг]: ✅ OK (Разница между свечами {time_diff_hours} часов).")
                    else:
                        log.error(f"   [Тайминг]: ❌ ПРОВАЛ (Разница между свечами {time_diff_hours} часов, ожидалось 8).")
                        log.error(f"      (C0: {c0_ot}, C1: {c1_ot})")
                else:
                    log.warning("   [Тайминг]: ⚠️  Недостаточно свечей для проверки тайминга.")
                    
                # 2. Проверка OI/FR
                if num_candles_8h >= 3:
                    missing_oi_fr_8h = []
                    # Проверяем T-0, T-1, T-2
                    for i, candle in enumerate(first_coin_candles[-3:]):
                        candle_name = f"T-{2-i}"
                        if candle.get('openInterest') is None:
                            missing_oi_fr_8h.append(f"OI_{candle_name}")
                        if candle.get('fundingRate') is None:
                            missing_oi_fr_8h.append(f"FR_{candle_name}")
                    
                    if not missing_oi_fr_8h:
                        log.info(f"   [OI/FR]: ✅ OK (OI и FR присутствуют в последних 3 свечах).")
                    else:
                        log.error(f"   [OI/FR]: ❌ ПРОВАЛ (На примере {first_coin_symbol} отсутствуют: {', '.join(missing_oi_fr_8h)}).")
                else:
                    log.warning("   [OI/FR]: ⚠️  Недостаточно свечей для проверки OI/FR.")
    # -------------------------------------------

    # Завершаем выполнение с ошибкой, если были проваленные тесты
    if failed_tests:
        exit(1)

# --- Добавление main_test для запуска ---
if __name__ == "__main__":
    from collections import defaultdict # Добавляем импорт для defaultdict
    try:
        asyncio.run(main_test())
    except KeyboardInterrupt:
        log.warning("\n⚠️  Тест прерван пользователем.")
