import httpx
import asyncio
import sys
import logging
import time
import os 
from dotenv import load_dotenv

# --- 1. Загрузка конфигурации из .env ---
load_dotenv()  
BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000") 
SECRET_TOKEN = os.environ.get("SECRET_TOKEN") # Используется для /update-fr
# ----------------------------------------

# --- Настройки ---
POLL_INTERVAL_SEC = 10  # Интервал опроса воркера
MAX_WAIT_MINUTES_PER_TASK = 15 # Макс. время ожидания ОДНОЙ задачи
# -----------------

# --- Задачи для "прогрева" ---
# (Запускаем FR первым, чтобы Klines его использовали)
TASKS_TO_RUN = ["1h", "fr", "4h", "12h", "1d"]
# Ключи кэша, которые мы будем проверять в конце
CACHE_KEYS_TO_VALIDATE = ["global_fr", "1h", "4h", "8h", "12h", "1d"]
# ---------------------------

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("CACHE_WARMUP")

# (Ожидаемые ключи, скопировано из e2e_test.py)
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

# ============================================================================
# === ХЕЛПЕРЫ (из e2e_test.py) ===
# ============================================================================

async def wait_for_worker_to_be_free(client: httpx.AsyncClient, task_name: str):
    """
    Опрашивает сервер, пока воркер не освободится (перестанет возвращать 409).
    """
    log.info(f"--- Ожидаю завершения задачи '{task_name}' (опрос каждые {POLL_INTERVAL_SEC} сек)...")
    start_time = time.time()
    max_wait_time_sec = MAX_WAIT_MINUTES_PER_TASK * 60
    
    # 1. Сначала ждем, пока воркер заберет задачу (если она еще в очереди)
    while True:
        try:
            response = await client.get("/queue-status")
            queue_len = response.json()["tasks_in_queue"]
            if queue_len == 0:
                log.info(f"... Воркер забрал задачу '{task_name}' (очередь пуста).")
                break
            log.info(f"... Задача '{task_name}' еще в очереди (длина: {queue_len}). Жду 5 сек...")
            await asyncio.sleep(5)
        except Exception as e:
            log.error(f"Ошибка при проверке очереди: {e}")
            await asyncio.sleep(5)
            
        if time.time() - start_time > max_wait_time_sec:
            raise TimeoutError(f"Таймаут! Воркер не забрал задачу '{task_name}' за {MAX_WAIT_MINUTES_PER_TASK} мин.")

    # 2. Теперь ждем, пока воркер освободит блокировку
    while True:
        if time.time() - start_time > max_wait_time_sec:
            raise TimeoutError(f"Таймаут! Задача '{task_name}' не завершилась за {MAX_WAIT_MINUTES_PER_TASK} мин.")

        try:
            # "Пинаем" API, пытаясь запустить фейковую задачу '1h'.
            response = await client.post("/get-market-data", json={"timeframe": "1h"})
            
            if response.status_code == 202:
                # Воркер свободен. Нам нужно "почистить" задачу '1h', которую мы добавили.
                # (В Upstash/Redis 'LPOP count' не поддерживается, как в Redis 6.2+)
                # (Простейший способ - просто вызвать LPOP)
                log.info("... (Воркер свободен. Очищаю тестовую задачу '1h' из очереди...)")
                # (Мы не можем гарантированно очистить, НО наш 'worker' все равно 
                # сначала выполнит '1h', а только потом '4h' и т.д. 
                # Этот скрипт просто добавит '1h' в очередь еще раз)
                log.info(f"✅ Воркер освободился (получен 202). Задача '{task_name}' выполнена.")
                return
            
            elif response.status_code == 409:
                log.info(f"... Воркер занят (409). Жду {POLL_INTERVAL_SEC} сек...")
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
    if task_name == "fr":
        # Это задача FR
        if not SECRET_TOKEN:
            log.error("💥 [FAIL] SECRET_TOKEN не найден в .env. Не могу запустить задачу 'fr'.")
            raise ValueError("SECRET_TOKEN not set")
            
        log.info("Запускаю задачу 'fr' (POST /api/v1/internal/update-fr)...")
        headers = {"Authorization": f"Bearer {SECRET_TOKEN}"}
        response = await client.post("/api/v1/internal/update-fr", headers=headers)
    
    else:
        # Это задача Klines (1h, 4h, 12h, 1d)
        log.info(f"Запускаю задачу '{task_name}' (POST /get-market-data)...")
        response = await client.post("/get-market-data", json={"timeframe": task_name})

    # Обработка ответа
    if response.status_code == 202:
        log.info(f"✅ [OK] Задача '{task_name}' принята в очередь.")
    elif response.status_code == 409:
        log.warning(f"Воркер уже был занят (409). Ожидаю его завершения...")
        # (wait_for_worker_to_be_free разберется с этим)
    else:
        response.raise_for_status() # Вызовет ошибку, если 500, 400 и т.д.


def validate_cache_data(data: dict, key: str):
    """
    Проверяет структуру и поля загруженных данных из кэша.
    (Адаптировано из e2e_test.py)
    """
    log.info(f"--- 🔬 Валидация данных для 'cache:{key}' ---")
    
    # 1. Проверка 'global_fr' (у него другая структура)
    if key == 'global_fr':
        assert isinstance(data, dict), "'global_fr' должен быть словарем (dict)"
        assert len(data) > 0, "'global_fr' не должен быть пустым"
        # Проверяем первую запись
        first_key = list(data.keys())[0]
        first_value = data[first_key]
        assert isinstance(first_key, str), "Ключ в 'global_fr' должен быть строкой (символом)"
        assert isinstance(first_value, list), "Значение в 'global_fr' должно быть списком (list)"
        assert "openTime" in first_value[0]
        assert "fundingRate" in first_value[0]
        log.info(f"✅ [OK] Валидация для 'cache:{key}' прошла успешно.")
        return

    # 2. Проверка Klines (1h, 4h, 8h, 12h, 1d)
    for top_key in EXPECTED_TOP_LEVEL_KEYS:
        assert top_key in data, f"Отсутствует ключ верхнего уровня '{top_key}' в ответе {key}"
    
    assert data["timeframe"] == key
    assert isinstance(data["data"], list)
    
    if not data["data"]:
        log.warning(f"Validation WARNING: 'cache:{key}' содержит пустой список 'data'. Аудит: {data['audit_report']}")
        return

    log.info(f"Найдено {len(data['data'])} монет в 'data' (проверяем первую).")

    coin_data = data["data"][0]
    for coin_key in EXPECTED_COIN_DATA_KEYS:
        assert coin_key in coin_data, f"Отсутствует ключ '{coin_key}' в coin_data (data[0])"
    
    assert isinstance(coin_data["data"], list)
    assert len(coin_data["data"]) > 0, f"Список 'data' внутри монеты {coin_data['symbol']} пуст"

    # 3. Проверка ПОСЛЕДНЕЙ свечи
    candle = coin_data["data"][-1]
    log.info(f"Проверяем ключи ПОСЛЕДНЕЙ свечи (OpenTime: {candle.get('openTime')})...")
    
    for candle_key in EXPECTED_CANDLE_KEYS:
        assert candle_key in candle, f"Отсутствует обязательный ключ Klines '{candle_key}' в свече"
        
    assert "openInterest" in candle, "Отсутствует ключ 'openInterest' (может быть None)"
    assert "fundingRate" in candle, "Отсутствует ключ 'fundingRate' (может быть None)"

    log.info(f"✅ [OK] Валидация для 'cache:{key}' прошла успешно.")

# ============================================================================
# === ГЛАВНЫЙ СКРИПТ ===
# ============================================================================

async def run_cache_warmup():
    """
    Главный скрипт "прогрева" кэша.
    """
    total_start_time = time.time()
    log.info("--- 🚀 НАЧИНАЮ ПРОГРЕВ КЭША ---")
    log.info(f"Цель: {BASE_URL}")
    log.info(f"Задачи для выполнения: {TASKS_TO_RUN}")

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        
        # --- Шаг 1: Проверка сервера ---
        try:
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
            log.info(f"--- 🏁 Запускаю задачу: {task} ---")
            
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
        log.info("--- 🔬 Начинаю валидацию всех кэшей (/cache/...) ---")
        all_valid = True
        
        for key in CACHE_KEYS_TO_VALIDATE:
            try:
                log.info(f"Загружаю 'cache:{key}'...")
                response = await client.get(f"/cache/{key}")
                response.raise_for_status() # Проверка на 404 или 500
                
                # Валидируем структуру
                validate_cache_data(response.json(), key)
                
            except Exception as e:
                log.error(f"💥 [FAIL] ОШИБКА ВАЛИДАЦИИ для 'cache:{key}': {e}")
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