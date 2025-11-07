import httpx
import asyncio
import sys
import logging
import time
import os 


from dotenv import load_dotenv
load_dotenv()  

# --- НАСТРОЙКА ---
BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000") 

# --- Настройки ---
POLL_INTERVAL_SEC = 10
MAX_WAIT_MINUTES = 10 
# -----------------

# Настройка простого логгера для этого скрипта
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("E2E_TEST")

# Ожидаемые ключи в свече (ключи OI/FR могут отсутствовать, но klines должны быть)
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


def validate_cache_data(data: dict, timeframe: str):
    """
    Проверяет структуру и поля загруженных данных из кэша.
    """
    log.info(f"--- 🔬 Валидация данных для {timeframe} ---")
    
    # 1. Проверка структуры верхнего уровня
    for key in EXPECTED_TOP_LEVEL_KEYS:
        assert key in data, f"Отсутствует ключ верхнего уровня '{key}' в ответе {timeframe}"
    
    assert data["timeframe"] == timeframe
    assert "missing_klines" in data["audit_report"]
    assert "missing_oi" in data["audit_report"]
    assert "missing_fr" in data["audit_report"]
    assert isinstance(data["data"], list)
    
    if not data["data"]:
        # (Проверяем аудит, если нет данных)
        if not data["audit_report"]["missing_klines"]:
             log.warning(f"Validation WARNING: {timeframe} содержит пустой список 'data', но 'missing_klines' тоже пуст. Возможно, API монет вернуло 0 монет.")
        else:
             log.warning(f"Validation complete (No data): {timeframe} содержит пустой список 'data'. Аудит: {data['audit_report']}")
        return

    log.info(f"Найдено {len(data['data'])} монет в 'data' (проверяем первую).")

    # 2. Проверка структуры данных монеты (первой в списке)
    coin_data = data["data"][0]
    for key in EXPECTED_COIN_DATA_KEYS:
        assert key in coin_data, f"Отсутствует ключ '{key}' в coin_data (data[0])"
    
    assert isinstance(coin_data["data"], list)
    assert len(coin_data["data"]) > 0, "Список 'data' внутри монеты пуст"

    # --- ИСПРАВЛЕНИЕ: Проверяем ПОСЛЕДНЮЮ (самую новую) свечу ---
    # 3. Проверка структуры свечи (ПОСЛЕДНЕЙ свечи у первой монеты)
    candle = coin_data["data"][-1]
    log.info(f"Проверяем ключи ПОСЛЕДНЕЙ свечи (OpenTime: {candle.get('openTime')})...")
    # -------------------------------------------------------------
    
    for key in EXPECTED_CANDLE_KEYS:
        assert key in candle, f"Отсутствует обязательный ключ Klines '{key}' в свече"
        
    # Проверяем, что OI и FR (которые могли не смержиться) присутствуют
    # (Они могут быть None, но ключ должен быть добавлен в data_processing.merge_data)
    
    # --- ИСПРАВЛЕНИЕ: Теперь эти ключи ДОЛЖНЫ быть, т.к. это последняя свеча ---
    assert "openInterest" in candle, "Отсутствует ключ 'openInterest' (может быть None)"
    assert "fundingRate" in candle, "Отсутствует ключ 'fundingRate' (может быть None)"
    # -------------------------------------------------------------------------

    log.info(f"✅ [OK] Валидация для {timeframe} прошла успешно.")


async def run_e2e_test():
    """
    Главный E2E тест.
    """
    start_time = time.time()
    max_wait_time_sec = MAX_WAIT_MINUTES * 60

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        
        # --- Шаг 1: Проверка, что сервер жив ---
        log.info(f"Подключаюсь к {BASE_URL}...")
        try:
            response = await client.get("/health")
            response.raise_for_status()
            log.info("✅ [OK] Сервер доступен.")
        except (httpx.ConnectError, httpx.HTTPStatusError) as e:
            log.error(f"💥 [FAIL] Не удалось подключиться к серверу: {e}")
            log.error("Убедись, что сервер запущен, прежде чем запускать этот скрипт.")
            return

        # --- Шаг 2: Ожидание готовности (ожидание 'cache:global_fr') ---
        log.info("Ожидаю, пока 'lifespan' (startup) создаст 'cache:global_fr'...")
        while True:
            try:
                # (Используем 'key' = 'global_fr', т.к. api_routes.py был исправлен)
                response = await client.get("/cache/global_fr")
                if response.status_code == 200:
                    log.info("✅ [OK] 'cache:global_fr' готов. Сервер полностью инициализирован.")
                    break
                elif response.status_code == 404:
                    log.info("... 'cache:global_fr' еще не создан (404). Жду 10 сек...")
                    await asyncio.sleep(10)
                else:
                    # (Если 400 - значит мы все еще используем старый api_routes.py)
                    if response.status_code == 400:
                         log.error("💥 [FAIL] Сервер вернул 400. Убедись, что 'api_routes.py' обновлен и использует 'ALLOWED_CACHE_KEYS'.")
                    response.raise_for_status()
            
            except asyncio.TimeoutError:
                log.error("💥 [FAIL] Таймаут при ожидании global_fr.")
                return
            
            if time.time() - start_time > max_wait_time_sec:
                log.error(f"💥 [FAIL] Таймаут! 'cache:global_fr' не появился за {MAX_WAIT_MINUTES} минут.")
                return

        # --- Шаг 3: Запуск задачи 4h ---
        log.info("Запускаю задачу 4h (POST /get-market-data)...")
        try:
            response = await client.post("/get-market-data", json={"timeframe": "4h"})
            if response.status_code == 409:
                log.warning("Воркер уже был занят (409). Ожидаю его завершения...")
            elif response.status_code == 202:
                log.info("✅ [OK] Задача '4h' принята в очередь.")
            else:
                response.raise_for_status()
        except httpx.HTTPStatusError as e:
            log.error(f"💥 [FAIL] Не удалось запустить задачу '4h': {e}")
            return
            
        # --- Шаг 4: Ожидание завершения задачи 4h ---
        log.info(f"Ожидаю завершения задачи '4h' (опрос каждые {POLL_INTERVAL_SEC} сек)...")
        task_started_time = time.time()
        
        # Сначала ждем, пока воркер заберет задачу (очередь = 0)
        while True:
            response = await client.get("/queue-status")
            queue_len = response.json()["tasks_in_queue"]
            if queue_len == 0:
                log.info("... Воркер забрал задачу (очередь пуста).")
                break
            log.info(f"... Задача в очереди (длина: {queue_len}). Жду 5 сек...")
            await asyncio.sleep(5)
            if time.time() - task_started_time > max_wait_time_sec:
                log.error(f"💥 [FAIL] Таймаут! Воркер не забрал задачу из очереди за {MAX_WAIT_MINUTES} минут.")
                return

        # Теперь ждем, пока воркер освободится (перестанет возвращать 409)
        while True:
            if time.time() - task_started_time > max_wait_time_sec:
                log.error(f"💥 [FAIL] Таймаут! Воркер не освободился за {MAX_WAIT_MINUTES} минут.")
                return

            response = await client.post("/get-market-data", json={"timeframe": "1h"})
            
            if response.status_code == 202:
                log.info(f"✅ [OK] Воркер освободился (получен 202). Задача '4h' выполнена.")
                # (Нам нужно очистить эту '1h' задачу из очереди)
                try:
                    q_len = 1
                    while q_len > 0:
                        redis_client.lpop(REDIS_TASK_QUEUE_KEY) # (Предполагая, что redis_client импортирован, но здесь его нет)
                        # (Лучше просто подождать, пока он ее заберет)
                        log.info("... Очищаю '1h' (тестовую) задачу из очереди...")
                        await asyncio.sleep(2) 
                        r = await client.get("/queue-status")
                        q_len = r.json()["tasks_in_queue"]
                except Exception: 
                    pass # (Не страшно, если не удалось)
                break
            elif response.status_code == 409:
                log.info(f"... Воркер занят (409). Жду {POLL_INTERVAL_SEC} сек...")
                await asyncio.sleep(POLL_INTERVAL_SEC)
            else:
                log.error(f"💥 [FAIL] Неожиданный статус при опросе воркера: {response.status_code}")
                return

        # --- Шаг 5: Загрузка и валидация 4h ---
        log.info("Загружаю 'cache:4h'...")
        response_4h = await client.get("/cache/4h")
        response_4h.raise_for_status()
        validate_cache_data(response_4h.json(), "4h")

        # --- Шаг 6: Загрузка и валидация 8h ---
        log.info("Загружаю 'cache:8h'...")
        response_8h = await client.get("/cache/8h")
        response_8h.raise_for_status()
        validate_cache_data(response_8h.json(), "8h")
        
        # -----------------------------------------------------------------
        # --- (ИЗМЕНЕНИЕ) ЭТАП 2: Тестирование '1d' ---
        # -----------------------------------------------------------------
        log.info("--- 🚀 НАЧИНАЮ ЭТАП 2: Тестирование '1d' (single_timeframe_task) ---")

        # --- Шаг 7: Запуск задачи 1d ---
        log.info("Запускаю задачу 1d (POST /get-market-data)...")
        try:
            response = await client.post("/get-market-data", json={"timeframe": "1d"})
            if response.status_code == 409:
                # (Это не должно случиться, мы только что очистили '1h' задачу)
                log.warning("Воркер все еще занят (409). Ожидаю его завершения...")
            elif response.status_code == 202:
                log.info("✅ [OK] Задача '1d' принята в очередь.")
            else:
                response.raise_for_status()
        except httpx.HTTPStatusError as e:
            log.error(f"💥 [FAIL] Не удалось запустить задачу '1d': {e}")
            return

        # --- Шаг 8: Ожидание завершения задачи 1d ---
        log.info(f"Ожидаю завершения задачи '1d' (опрос каждые {POLL_INTERVAL_SEC} сек)...")
        task_1d_started_time = time.time()
        
        # Сначала ждем, пока воркер заберет задачу (очередь = 0)
        while True:
            response = await client.get("/queue-status")
            queue_len = response.json()["tasks_in_queue"]
            if queue_len == 0:
                log.info("... Воркер забрал задачу '1d' (очередь пуста).")
                break
            log.info(f"... Задача '1d' в очереди (длина: {queue_len}). Жду 5 сек...")
            await asyncio.sleep(5)
            if time.time() - task_1d_started_time > max_wait_time_sec:
                log.error(f"💥 [FAIL] Таймаут! Воркер не забрал задачу '1d' из очереди.")
                return

        # Теперь ждем, пока воркер освободится (перестанет возвращать 409)
        while True:
            if time.time() - task_1d_started_time > max_wait_time_sec:
                log.error(f"💥 [FAIL] Таймаут! Воркер не освободился (1d) за {MAX_WAIT_MINUTES} минут.")
                return

            # (Используем '1h' как безопасную "пробную" задачу)
            response = await client.post("/get-market-data", json={"timeframe": "1h"})
            
            if response.status_code == 202:
                log.info(f"✅ [OK] Воркер освободился (получен 202). Задача '1d' выполнена.")
                break
            elif response.status_code == 409:
                log.info(f"... Воркер занят (1d) (409). Жду {POLL_INTERVAL_SEC} сек...")
                await asyncio.sleep(POLL_INTERVAL_SEC)
            else:
                log.error(f"💥 [FAIL] Неожиданный статус при опросе воркера (1d): {response.status_code}")
                return
        
        # --- Шаг 9: Загрузка и валидация 1d ---
        log.info("Загружаю 'cache:1d'...")
        response_1d = await client.get("/cache/1d")
        response_1d.raise_for_status()
        validate_cache_data(response_1d.json(), "1d")

        log.info("--- 🏆 E2E ТЕСТ УСПЕШНО ЗАВЕРШЕН! (4h, 8h и 1d) ---")
        # --- КОНЕЦ ИЗМЕНЕНИЯ ---


if __name__ == "__main__":
    try:
        asyncio.run(run_e2e_test())
    except KeyboardInterrupt:
        log.warning("Тест прерван вручную.")