import httpx
import asyncio
import sys
import logging
import time
import os 
import json 
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from cache_manager import get_redis_connection
from redis.asyncio import Redis as AsyncRedis 
from config import REDIS_TASK_QUEUE_KEY, WORKER_LOCK_KEY, WORKER_LOCK_VALUE


# --- 1. Загрузка конфигурации из .env ---
load_dotenv()  
BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000") 
SECRET_TOKEN = os.environ.get("SECRET_TOKEN") 

# --- Настройки ---
POLL_INTERVAL_SEC = 10
MAX_WAIT_MINUTES_PER_TASK = 15
# -----------------

# --- Задачи для "прогрева" ---
TASKS_TO_RUN = ["global_fr", "1h", "4h", "12h", "1d"] 
CACHE_KEYS_TO_VALIDATE = ["global_fr", "1h", "4h", "8h", "12h", "1d"]
# ---------------------------

# (Настройка логгера)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("CACHE_WARMUP")

# (Константы проверки данных)
EXPECTED_CANDLE_KEYS = [
    "openTime", "openPrice", "highPrice", "lowPrice", "closePrice", "volume",
    "volumeDelta"
]
EXPECTED_TOP_LEVEL_KEYS = [
    "openTime", "closeTime", "timeframe", "audit_report", "data"
]
EXPECTED_COIN_DATA_KEYS = [
    "symbol", "exchanges", "data"
]


# --- НОВАЯ ФУНКЦИЯ: Принудительная очистка кэша ---
async def _cleanup_all_cache_keys(redis_conn: AsyncRedis, log_prefix: str):
    """Принудительно удаляет все ключи кэша (cache:*) перед тестом."""
    log.info(f"{log_prefix} --- ПРИНУДИТЕЛЬНАЯ ОЧИСТКА КЭША (cache:*) ---")
    
    try:
        # 1. Получаем все ключи, начинающиеся с 'cache:' (включая некорректные ключи, которые могли быть созданы)
        keys_to_delete_cache = await redis_conn.keys("cache:*")
        # 2. Добавляем ключи очереди и блокировки
        keys_to_delete_all = keys_to_delete_cache + [WORKER_LOCK_KEY, REDIS_TASK_QUEUE_KEY]
        
        deleted_count = 0
        if keys_to_delete_all:
            # Используем * для распаковки списка ключей
            deleted_count = await redis_conn.delete(*keys_to_delete_all)
            
        log.info(f"{log_prefix} Удалено {deleted_count} старых ключей кэша/блокировок/очереди.")
        
    except Exception as e:
        log.error(f"{log_prefix} Критическая ошибка при очистке Redis: {e}", exc_info=True)
    
    # Дополнительная гарантия очистки (если KEYS не сработал или список был пуст)
    try:
        await redis_conn.delete(WORKER_LOCK_KEY, REDIS_TASK_QUEUE_KEY)
        log.info(f"{log_prefix} Очищены блокировка воркера и очередь задач.")
    except Exception as e:
        log.error(f"{log_prefix} Ошибка при удалении блокировок/очереди: {e}", exc_info=True)

    log.info("--- (Redis очищен. Начинаем) ---")


# --- Асинхронный хелпер для очистки очереди ---
async def _clean_test_task_from_queue(redis_conn: AsyncRedis, log_prefix: str):
    """Очищает тестовую задачу (1h) из очереди после ее завершения."""
    count = 0
    # Цикл продолжается, пока очередь не опустеет или не будет найдена реальная задача.
    while True:
        try:
            # ИЗМЕНЕНИЕ: Используем rpop, чтобы избежать конфликтов с lpop воркера.
            task_json = await redis_conn.rpop(REDIS_TASK_QUEUE_KEY)
            
            if task_json is None:
                break
                
            task_data = json.loads(task_json)
            
            if task_data.get('timeframe') == '1h':
                # Это тестовая задача, удаляем ее (rpop уже удалил).
                log.info(f"{log_prefix} ... Очищена тестовая задача '1h' из очереди.")
                count += 1
            else:
                 # Если это НЕ '1h' (реальная задача, которую мы не трогаем), 
                 # мы возвращаем ее в начало очереди (lpush) и прекращаем очистку.
                 await redis_conn.lpush(REDIS_TASK_QUEUE_KEY, task_json) 
                 break
                 
        except Exception as e:
            log.warning(f"{log_prefix} Не удалось очистить тестовую '1h' задачу: {e}")
            break
            
    if count > 0:
        log.info(f"{log_prefix} Всего очищено {count} тестовых задач.")


async def wait_for_worker_to_be_free(client: httpx.AsyncClient, task_name: str):
    """
    Опрашивает сервер, пока воркер не освободится.
    """
    log.info(f"--- Ожидаю завершения задачи '{task_name}' (опрос каждые {POLL_INTERVAL_SEC} сек)...")
    start_time = time.time()
    max_wait_time_sec = MAX_WAIT_MINUTES_PER_TASK * 60
    
    redis_conn = await get_redis_connection()
    if not redis_conn:
        log.error("💥 [FAIL] Не удалось подключиться к Redis. Проверка очереди невозможна.")
        
    
    # 2. Ждем, пока воркер освободит блокировку
    while True:
        if time.time() - start_time > max_wait_time_sec:
            raise TimeoutError(f"Таймаут! Задача '{task_name}' не завершилась за {MAX_WAIT_MINUTES_PER_TASK} мин.")

        try:
            # --- ПУТЬ БЕЗ ПРЕФИКСА ---
            response = await client.post("/get-market-data", json={"timeframes": ["1h"]}) 

            if response.status_code == 202:
                log.info(f"✅ Воркер освободился (получен 202). Задача 'S{task_name}' выполнена.")
                
                if redis_conn:
                    await _clean_test_task_from_queue(redis_conn, "[WAIT_FOR_FREE]")
                
                return
            
            elif response.status_code == 409:
                log.info(f"... Воркер занят (409). Жду {POLL_INTERVAL_SEC} сек...")
                await asyncio.sleep(POLL_INTERVAL_SEC)
            
            elif response.status_code == 503:
                 log.warning(f"... Redis/Сервис недоступен (503). Жду {POLL_INTERVAL_SEC} сек...")
                 await asyncio.sleep(POLL_INTERVAL_SEC)
            
            else:
                log.error(f"Неожиданный статус при опросе воркера: {response.status_code} {response.text}")
                await asyncio.sleep(POLL_INTERVAL_SEC)
                
        except Exception as e:
            log.error(f"Ошибка при опросе воркера: {e}", exc_info=False)
            await asyncio.sleep(POLL_INTERVAL_SEC)


async def post_task(client: httpx.AsyncClient, task_name: str):
    """
    Отправляет 1 задачу (Klines или FR) на сервер.
    """
    if task_name == "global_fr":
        # Это задача FR
        if not SECRET_TOKEN: 
            log.error("💥 [FAIL] SECRET_TOKEN не найден в .env. Не могу запустить задачу 'global_fr'.")
            raise ValueError("SECRET_TOKEN not set")
            
        # --- ПУТЬ БЕЗ ПРЕФИКСА ---
        log.info("Запускаю задачу 'global_fr' (POST /internal/update-fr)...")
        headers = {"Authorization": f"Bearer {SECRET_TOKEN}"}
        response = await client.post("/internal/update-fr", headers=headers)
    
    else:
        # Это задача Klines (1h, 4h, 12h, 1d)
        # --- ПУТЬ БЕЗ ПРЕФИКСА ---
        log.info(f"Запускаю задачу '{task_name}' (POST /get-market-data)...")
        response = await client.post("/get-market-data", json={"timeframes": [task_name]})

    # (Обработка ответа)
    if response.status_code == 202:
        log.info(f"✅ [OK] Задача '{task_name}' принята в очередь.")
    elif response.status_code == 409:
        log.warning(f"Воркер уже был занят (409). Ожидаю его завершения...")
    else:
        response.raise_for_status() 


def validate_cache_data(data: dict, key: str):
    """
    Валидация данных из кеша без вывода больших объемов данных в логи
    """
    log.info(f"--- 🔬 Валидация данных для 'cache:{key}' ---")
    
    # 1. Проверка 'global_fr'
    if key == 'global_fr':
        data = data.get('data', {})
        if not isinstance(data, dict):
            raise ValueError(f"'global_fr' должен быть словарем (dict) после распаковки")
        
        if not data:
            log.warning(f"Validation WARNING: 'cache:{key}' пуст (нет данных).")
            return
            
        first_key = list(data.keys())[0]
        first_value = data[first_key]
        
        if not isinstance(first_key, str):
            raise ValueError("Ключ в 'global_fr' должен быть строкой (символом)")
        
        if not isinstance(first_value, list):
            raise ValueError("Значение в 'global_fr' должно быть списком (list)")
        
        if "openTime" not in first_value[0]:
            raise ValueError("Отсутствует 'openTime' в данных global_fr")
        
        if "fundingRate" not in first_value[0]:
            raise ValueError("Отсутствует 'fundingRate' в данных global_fr")
        
        log.info(f"✅ [OK] Валидация для 'cache:{key}' прошла успешно.")
        return

    # 2. Проверка Klines (1h, 4h, 8h, 12h, 1d)
    for top_key in EXPECTED_TOP_LEVEL_KEYS:
        if top_key not in data:
            raise ValueError(f"Отсутствует ключ верхнего уровня '{top_key}' в ответе {key}")
    
    if data["timeframe"] != key:
        raise ValueError(f"Timeframe не совпадает: ожидался {key}, получен {data['timeframe']}")
    
    if not isinstance(data["data"], list):
        raise ValueError(f"'data' должен быть списком, получен {type(data['data'])}")
    
    if not data["data"]:
        log.warning(f"Validation WARNING: 'cache:{key}' содержит пустой список 'data'. Аудит: {data['audit_report']}")
        return

    log.info(f"Найдено {len(data['data'])} монет в 'data' (проверяем первую).")

    coin_data = data["data"][0]
    for coin_key in EXPECTED_COIN_DATA_KEYS:
        if coin_key not in coin_data:
            raise ValueError(f"Отсутствует ключ '{coin_key}' в coin_data (data[0])")
    
    if not isinstance(coin_data["data"], list):
        raise ValueError(f"coin_data['data'] должен быть списком")
    
    if len(coin_data["data"]) == 0:
        raise ValueError(f"Список 'data' внутри монеты {coin_data['symbol']} пуст")

    # 3. Проверка ПОСЛЕДНЕЙ свечи (без вывода всего объекта)
    candle = coin_data["data"][-1]
    open_time = candle.get('openTime', 'UNKNOWN')
    log.info(f"Проверяем ключи ПОСЛЕДНЕЙ свечи (OpenTime: {open_time})...")
    
    for candle_key in EXPECTED_CANDLE_KEYS:
        if candle_key not in candle:
            raise ValueError(f"Отсутствует обязательный ключ Klines '{candle_key}' в свече")
    
    if "openInterest" not in candle:
        raise ValueError("Отсутствует ключ 'openInterest' (может быть None)")
    
    if "fundingRate" not in candle:
        raise ValueError("Отсутствует ключ 'fundingRate' (может быть None)")

    log.info(f"✅ [OK] Валидация для 'cache:{key}' прошла успешно.")


async def run_cache_warmup():
    """
    Главный скрипт "прогрева" кэша.
    """
    total_start_time = time.time()
    log.info("--- 🚀 НАЧИНАЮ ПРОГРЕВ КЭША ---")
    log.info(f"Цель: {BASE_URL}")
    log.info(f"Задачи для выполнения: {TASKS_TO_RUN}")

    # --- НОВАЯ ЛОГИКА ОЧИСТКИ ---
    redis_conn = await get_redis_connection()
    if not redis_conn:
        log.error("💥 [FAIL] Критическая ошибка: Redis недоступен. Прогрев отменен.")
        sys.exit(1)
    
    # ПРИНУДИТЕЛЬНАЯ ОЧИСТКА ВСЕХ КЛЮЧЕЙ (включая lock, queue и cache:*)
    await _cleanup_all_cache_keys(redis_conn, "[PRE_TEST]")
    log.info("--- (Redis очищен. Начинаем) ---")
    # ---------------------------

    # Клиент создается с чистым BASE_URL
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=120.0) as client:
        
        # --- Шаг 1: Проверка сервера ---
        try:
            # ПУТЬ БЕЗ ПРЕФИКСА
            response = await client.get("/health") 
            response.raise_for_status()
            log.info("✅ [OK] Сервер доступен.")
        except (httpx.ConnectError, httpx.HTTPStatusError) as e:
            log.error(f"💥 [FAIL] Не удалось подключиться к серверу: {e}")
            return

        # --- Шаг 2: Убедимся, что воркер свободен перед началом ---
        try:
            await wait_for_worker_to_be_free(client, "init_check") 
            log.info("--- (Воркер свободен. Начинаем) ---")
        except TimeoutError as e:
            log.error(f"💥 [FAIL] {e}")
            log.error("Воркер был занят еще до начала теста. Прерываю.")
            return

        # --- Шаг 3: Выполнение всех задач (ENRICH REDIS) ---
        for task in TASKS_TO_RUN:
            task_start_time = time.time()
            log.info(f"--- 🔥 Запускаю задачу: {task} ---")
            
            try:
                # 3a. Отправляем задачу
                await post_task(client, task)
                
                # 3b. Ждем ее завершения
                await wait_for_worker_to_be_free(client, task)
                
                task_end_time = time.time()
                log.info(f"--- ✅ Задача '{task}' УСПЕШНО ЗАВЕРШЕНА за {(task_end_time - task_start_time):.2f} сек. ---")

            except Exception as e:
                log.error(f"💥 [FAIL] КРИТИЧЕСКАЯ ОШИБКА во время выполнения задачи '{task}': {e}")
                log.error("Прогрев кэша прерван.")
                return

        log.info("--- 🏆 ПРОГРЕВ КЭША (ЗАДАЧИ) ЗАВЕРШЕН ---")
        
        # --- Шаг 4: Валидация (CHECK ALL ENDPOINTS) ---
        log.info("--- 🔬 Начинаю валидацию всех кэшей (/get-cache/...) ---")
        all_valid = True
        
        for key in CACHE_KEYS_TO_VALIDATE:
            try:
                log.info(f"Загружаю 'cache:{key}'...")
                # ПУТЬ БЕЗ ПРЕФИКСА
                response = await client.get(f"/get-cache/{key}") 
                response.raise_for_status() 
                
                validate_cache_data(response.json(), key)
                
            except Exception as e:
                log.error(f"💥 [FAIL] ОШИБКА ВАЛИДАЦИИ для 'cache:{key}': {e}", exc_info=True)
                all_valid = False

        # --- Финальный вердикт ---
        if all_valid:
            log.info("--- 🏆🏆🏆 E2E ТЕСТ И ПРОГРЕВ КЭША УСПЕШНО ЗАВЕРШЕНЫ! ---")
        else:
            log.error("--- 💥 E2E ТЕСТ ПРОВАЛЕН. Смотри ошибки валидации выше. ---")
            
    total_end_time = time.time()
    log.info(f"--- Общее время выполнения: {(total_end_time - total_start_time):.2f} сек. ---")


if __name__ == "__main__":
    try:
        asyncio.run(run_cache_warmup())
    except KeyboardInterrupt:
        log.warning("Прогрев кэша прерван вручную.")