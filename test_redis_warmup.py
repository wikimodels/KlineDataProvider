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
POLL_INTERVAL_SEC = 5  # Проверка каждые 5 сек
MAX_WAIT_MINUTES_PER_TASK = 15
CACHE_POLL_INTERVAL_SEC = 3
MAX_CACHE_WAIT_SEC = 60
# -----------------

# --- Задачи для "прогрева" ---
TASKS_TO_RUN = ["global_fr", "1h", "4h", "8h", "12h", "1d"] 
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


async def _cleanup_all_cache_keys(redis_conn: AsyncRedis, log_prefix: str):
    """Принудительно удаляет все ключи кэша (cache:*) перед тестом."""
    log.info(f"{log_prefix} --- ПРИНУДИТЕЛЬНАЯ ОЧИСТКА КЭША (cache:*) ---")
    
    try:
        keys_to_delete_cache = await redis_conn.keys("cache:*")
        
        keys_to_delete_all = keys_to_delete_cache + [WORKER_LOCK_KEY, REDIS_TASK_QUEUE_KEY]
        keys_to_delete_all = [key.decode('utf-8') if isinstance(key, bytes) else key for key in keys_to_delete_all]
        keys_to_delete_all = list(set(keys_to_delete_all)) # Уникальные
        
        deleted_count = 0
        if keys_to_delete_all:
            deleted_count = await redis_conn.delete(*keys_to_delete_all)
            
        log.info(f"{log_prefix} Удалено {deleted_count} старых ключей кэша/блокировок/очереди.")
        
    except Exception as e:
        log.error(f"{log_prefix} Критическая ошибка при очистке Redis: {e}", exc_info=True)
    
    try:
        await redis_conn.delete(WORKER_LOCK_KEY, REDIS_TASK_QUEUE_KEY)
        log.info(f"{log_prefix} Очищены блокировка воркера и очередь задач.")
    except Exception as e:
        log.error(f"{log_prefix} Ошибка при удалении блокировок/очереди: {e}", exc_info=True)

    log.info("--- (Redis очищен. Начинаем) ---")


async def wait_for_cache_to_appear(client: httpx.AsyncClient, cache_key: str, redis_conn: AsyncRedis) -> bool:
    """
    Опрашивает Redis напрямую, пока не появится кэш.
    Возвращает True если появился, False если таймаут.
    """
    log.info(f"[CACHE_WAIT] 🔍 Ожидаю появления 'cache:{cache_key}' в Redis (макс {MAX_CACHE_WAIT_SEC} сек)...")
    start_time = time.time()
    
    full_key = f"cache:{cache_key}"
    attempt = 0
    
    while True:
        attempt += 1
        elapsed = time.time() - start_time
        
        if elapsed > MAX_CACHE_WAIT_SEC:
            log.error(f"[CACHE_WAIT] ❌ Таймаут! Кэш '{full_key}' не появился за {MAX_CACHE_WAIT_SEC} сек.")
            return False
        
        try:
            exists = await redis_conn.exists(full_key)
            
            if exists:
                log.info(f"[CACHE_WAIT] ✅ Кэш '{full_key}' обнаружен в Redis! (попытка #{attempt}, {elapsed:.1f} сек)")
                return True
            else:
                log.info(f"[CACHE_WAIT] ... Попытка #{attempt}: '{full_key}' ещё нет. Жду {CACHE_POLL_INTERVAL_SEC} сек...")
                await asyncio.sleep(CACHE_POLL_INTERVAL_SEC)
                
        except Exception as e:
            log.error(f"[CACHE_WAIT] Ошибка при проверке Redis: {e}")
            await asyncio.sleep(CACHE_POLL_INTERVAL_SEC)


# --- ИЗМЕНЕНИЕ №1 и №2: Полная переработка логики ожидания ---
async def wait_for_worker_to_be_free(redis_conn: AsyncRedis, task_name: str):
    """
    Опрашивает Redis, используя двухфазную проверку,
    чтобы убедиться, что НАШ воркер (lock='processing') взял и завершил задачу.
    """
    log.info(f"--- Ожидаю завершения задачи '{task_name}' (опрос Redis {WORKER_LOCK_KEY} каждые {POLL_INTERVAL_SEC} сек)...")
    max_wait_time_sec = MAX_WAIT_MINUTES_PER_TASK * 60
    
    if not redis_conn:
        log.error("💥 [FAIL] Не удалось подключиться к Redis. Проверка блокировки невозможна.")
        raise ConnectionError("Redis недоступен в wait_for_worker_to_be_free")
        
    # --- ИЗМЕНЕНИЕ №2: Специальная логика для 'init_check' ---
    # (Мы не можем ждать 'processing', т.к. задача еще не отправлена.
    # Мы просто ждем, пока все "зомби" (если они есть) не уйдут)
    if task_name == "init_check":
        log.info("... [init_check] Ожидаю, пока ЛЮБАЯ блокировка не будет снята (Фаза 2)...")
        phase1_start_time = time.time()
        while time.time() - phase1_start_time < max_wait_time_sec:
            try:
                lock_status_bytes = await redis_conn.get(WORKER_LOCK_KEY)
                lock_status = lock_status_bytes.decode('utf-8') if lock_status_bytes else None
                
                if lock_status is None:
                    log.info(f"✅ [init_check] Воркер свободен (Lock=None).")
                    return # УСПЕХ
                else:
                    log.warning(f"... [init_check] Воркер (возможно, 'зомби') занят (Lock='{lock_status}'). Жду {POLL_INTERVAL_SEC} сек...")
                    await asyncio.sleep(POLL_INTERVAL_SEC)
                    
            except Exception as e:
                log.error(f"[init_check] Ошибка при опросе Redis (lock): {e}", exc_info=False)
                await asyncio.sleep(POLL_INTERVAL_SEC)
        
        raise TimeoutError(f"Таймаут [init_check]! Блокировка не была снята за {max_wait_time_sec} сек.")
    # --- КОНЕЦ ИЗМЕНЕНИЯ №2 ---

    # --- Фаза 1: Ждем, пока НАШ воркер (processing) ЗАХВАТИТ задачу ---
    log.info(f"... Фаза 1: Ожидаю, пока '{WORKER_LOCK_VALUE}' не появится в {WORKER_LOCK_KEY} (Макс {max_wait_time_sec} сек)...")
    phase1_start_time = time.time()
    task_taken = False
    
    while time.time() - phase1_start_time < max_wait_time_sec:
        try:
            lock_status_bytes = await redis_conn.get(WORKER_LOCK_KEY)
            lock_status = lock_status_bytes.decode('utf-8') if lock_status_bytes else None
            
            if lock_status == WORKER_LOCK_VALUE:
                log.info(f"✅ [Фаза 1] НАШ воркер захватил задачу (Lock='{lock_status}'). Перехожу к Фазе 2.")
                task_taken = True
                break
            elif lock_status is not None:
                # --- Это "фантомный" лок (например, busy_by_...) ---
                log.warning(f"... [Фаза 1] 'Фантомный' воркер занят (Lock='{lock_status}'). Жду, пока он освободит...")
                await asyncio.sleep(POLL_INTERVAL_SEC)
            else:
                # Lock is None,
                log.info(f"... [Фаза 1] Воркер свободен (Lock=None). Ожидаю захвата задачи '{task_name}'... Жду {POLL_INTERVAL_SEC} сек...")
                await asyncio.sleep(POLL_INTERVAL_SEC)

        except Exception as e:
            log.error(f"[Фаза 1] Ошибка при опросе Redis (lock): {e}", exc_info=False)
            await asyncio.sleep(POLL_INTERVAL_SEC)
            
    if not task_taken:
         raise TimeoutError(f"Таймаут Фазы 1! НАШ воркер (lock='{WORKER_LOCK_VALUE}') не захватил задачу '{task_name}' за {max_wait_time_sec} сек.")

    # --- Фаза 2: Ждем, пока НАШ воркер (processing) ОСВОБОДИТ задачу ---
    log.info(f"... Фаза 2: Ожидаю, пока '{WORKER_LOCK_VALUE}' не исчезнет (воркер завершит работу)...")
    phase2_start_time = time.time()
    
    while time.time() - phase2_start_time < max_wait_time_sec:
        try:
            lock_status_bytes = await redis_conn.get(WORKER_LOCK_KEY)
            lock_status = lock_status_bytes.decode('utf-8') if lock_status_bytes else None
            
            if lock_status == WORKER_LOCK_VALUE:
                log.info(f"... [Фаза 2] НАШ воркер все еще занят (Lock='{lock_status}'). Жду {POLL_INTERVAL_SEC} сек...")
                await asyncio.sleep(POLL_INTERVAL_SEC)
            else:
                log.info(f"✅ [Фаза 2] НАШ воркер освободился (Lock='{lock_status}'). Задача '{task_name}' выполнена.")
                return # УСПЕХ

        except Exception as e:
            log.error(f"[Фаза 2] Ошибка при опросе Redis (lock): {e}", exc_info=False)
            await asyncio.sleep(POLL_INTERVAL_SEC)

    raise TimeoutError(f"Таймаут Фазы 2! НАШ воркер (lock='{WORKER_LOCK_VALUE}') не освободил задачу '{task_name}' за {max_wait_time_sec} сек.")
# --- КОНЕЦ ИЗМЕНЕНИЯ №1 ---


async def post_task(client: httpx.AsyncClient, task_name: str, redis_conn: AsyncRedis):
    """
    Отправляет 1 задачу (Klines или FR) на сервер.
    """
    if task_name == "global_fr":
        if not SECRET_TOKEN: 
            log.error("💥 [FAIL] SECRET_TOKEN не найден в .env. Не могу запустить задачу 'global_fr'.")
            raise ValueError("SECRET_TOKEN not set")
            
        log.info("Запускаю задачу 'global_fr' (POST /internal/update-fr)...")
        headers = {"Authorization": f"Bearer {SECRET_TOKEN}"}
        response = await client.post("/internal/update-fr", headers=headers)
    
    else:
        cache_key_to_clear = f"cache:{task_name}"
        log.info(f"Очищаю '{cache_key_to_clear}', чтобы API инициировал обновление...")
        await redis_conn.delete(cache_key_to_clear)
        
        log.info(f"Запускаю задачу '{task_name}' (POST /get-market-data)...")
        response = await client.post("/get-market-data", json={"timeframes": [task_name]})

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
        
        if not first_value:
             log.warning(f"Validation WARNING: 'cache:{key}' содержит пустой список для {first_key}.")
             return
        
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

    # 3. Проверка ПОСЛЕДНЕЙ свечи
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

    redis_conn = await get_redis_connection()
    if not redis_conn:
        log.error("💥 [FAIL] Критическая ошибка: Redis недоступен. Прогрев отменен.")
        sys.exit(1)
    
    await _cleanup_all_cache_keys(redis_conn, "[PRE_TEST]")

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=120.0) as client:
        
        try:
            response = await client.get("/health") 
            response.raise_for_status()
            log.info("✅ [OK] Сервер доступен.")
        except (httpx.ConnectError, httpx.HTTPStatusError) as e:
            log.error(f"💥 [FAIL] Не удалось подключиться к серверу: {e}")
            return

        try:
            # --- ИЗМЕНЕНИЕ №2: Этот вызов теперь ждет, пока ВСЕ (включая зомби) не закончат ---
            await wait_for_worker_to_be_free(redis_conn, "init_check") 
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
                await post_task(client, task, redis_conn)
                
                # --- ИЗМЕНЕНИЕ №1: Этот вызов теперь ЖДЕТ, пока НАШ воркер не ЗАВЕРШИТ ---
                await wait_for_worker_to_be_free(redis_conn, task)
                
                log.info(f"[POST_TASK] ⏳ Задача '{task}' обработана воркером. Ожидаю сохранения в Redis...")
                cache_appeared = await wait_for_cache_to_appear(client, task, redis_conn)
                
                if not cache_appeared:
                    log.error(f"[POST_TASK] ❌ Кэш '{task}' НЕ появился в Redis за {MAX_CACHE_WAIT_SEC} сек!")
                    log.error("Прогрев кэша прерван.")
                    return
                
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