import asyncio
import httpx
import logging
import time

# --- НАСТРОЙКИ ТЕСТА ---

# URL вашего сервера на Render
SERVER_URL = "https://server.onrender.com" 

# Таймфреймы, которые мы запускаем (согласно вашему запросу)
TIME_FRAMES_TO_TEST = ['12h']

# Эндпоинты
POST_URL = f"{SERVER_URL}/get-market-data"
CACHE_URL_TEMPLATE = f"{SERVER_URL}/cache/{{timeframe}}"
HEALTH_URL = f"{SERVER_URL}/health"

# Таймауты
POLL_INTERVAL = 20      # Опрашивать кэш каждые 20 секунд
GLOBAL_TIMEOUT = 15 * 60  # Общий таймаут: 15 минут
WARM_UP_TIMEOUT = 120   # Таймаут на "прогрев": 2 минуты
JOB_TRIGGER_TIMEOUT = 90 # Таймаут на запуск задачи (увеличено)

# Настройка логирования
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%H:%M:%S')

# --- ЛОГИКА ТЕСТА ---

async def warm_up_server(client: httpx.AsyncClient) -> bool:
    """
    "Прогревает" сервер на бесплатном тарифе Render,
    пингуя /health, пока он не "проснется".
    """
    logging.info("--- Шаг 0: Прогрев сервера (до 2 минут) ---")
    start_time = time.time()
    
    while time.time() - start_time < WARM_UP_TIMEOUT:
        try:
            response = await client.get(HEALTH_URL, timeout=30)
            if response.status_code == 200:
                logging.info("✅ СЕРВЕР ПРОСНУЛСЯ. Начинаем тест.")
                return True
        except httpx.RequestError:
            logging.info("...сервер еще спит, ждем 10 сек...")
            await asyncio.sleep(10)
    
    logging.error("❌ ПРОВАЛ: Сервер не проснулся за 2 минуты.")
    return False

async def trigger_job(client: httpx.AsyncClient, timeframe: str) -> bool:
    """Отправляет POST-запрос для запуска задачи на сервере."""
    try:
        # Используем увеличенный таймаут
        response = await client.post(POST_URL, json={"timeframe": timeframe}, timeout=JOB_TRIGGER_TIMEOUT)
        
        if response.status_code == 202:
            logging.info(f"✅ ЗАПУСК: Задача для '{timeframe}' успешно отправлена.")
            return True
        else:
            logging.error(f"❌ ЗАПУСК ПРОВАЛЕН: Сервер ответил {response.status_code} для '{timeframe}'.")
            return False
            
    except httpx.RequestError as e:
        logging.error(f"❌ ОШИБКА ЗАПУСКА: Не удалось подключиться к серверу для '{timeframe}'. {e}")
        return False

async def check_cache(client: httpx.AsyncClient, timeframe: str) -> bool:
    """Опрашивает кэш, пока не получит 200 OK."""
    url = CACHE_URL_TEMPLATE.format(timeframe=timeframe)
    start_time = time.time()
    
    # Считаем 404 подряд, чтобы не спамить лог
    consecutive_404 = 0
    
    while time.time() - start_time < GLOBAL_TIMEOUT:
        try:
            response = await client.get(url, timeout=30)
            
            if response.status_code == 200:
                consecutive_404 = 0
                data = response.json()
                if data.get('data'):
                    logging.info(f"✅ ГОТОВО: Кэш для '{timeframe}' готов и содержит {len(data['data'])} монет.")
                    return True
                else:
                    logging.warning(f"⚠️  Кэш для '{timeframe}' готов, но пуст (0 монет). Проверяем снова...")
            
            elif response.status_code == 404:
                if consecutive_404 % 5 == 0: # Логируем только каждый 5-й 404
                    logging.info(f"⏳ ОЖИДАНИЕ: Кэш для '{timeframe}' пока пуст (404)...")
                consecutive_404 += 1
            else:
                consecutive_404 = 0
                logging.warning(f"⚠️  ОШИБКА КЭША: Сервер ответил {response.status_code} при проверке '{timeframe}'.")
                
        except httpx.RequestError as e:
            consecutive_404 = 0
            logging.error(f"❌ ОШИБКА ОПРОСА: Не удалось подключиться для проверки '{timeframe}'. {e}")
        
        await asyncio.sleep(POLL_INTERVAL)
    
    logging.error(f"❌ ТАЙМАУТ: Кэш для '{timeframe}' не появился за {GLOBAL_TIMEOUT / 60:.0f} минут.")
    return False

async def main():
    """Основная функция для запуска и мониторинга всех задач."""
    logging.info(f"--- НАЧАЛО СТРЕСС-ТЕСТА ---")
    logging.info(f"Цель: {SERVER_URL}")
    logging.info(f"Задачи: {', '.join(TIME_FRAMES_TO_TEST)}")
    
    async with httpx.AsyncClient() as client:
        
        # --- Шаг 0: Прогрев ---
        if not await warm_up_server(client):
            logging.critical("--- ТЕСТ ПРОВАЛЕН: Сервер не ответил на прогрев. ---")
            return
        
        # --- Шаг 1: Запускаем все задачи ---
        trigger_tasks = [trigger_job(client, tf) for tf in TIME_FRAMES_TO_TEST]
        trigger_results = await asyncio.gather(*trigger_tasks)
        
        if not all(trigger_results):
            logging.critical("--- ТЕСТ ПРОВАЛЕН: Не удалось запустить одну или несколько задач. ---")
            return

        logging.info("--- Все задачи успешно запущены. Начинаем опрос кэша. ---")
        
        # --- Шаг 2: Ожидаем готовности всех кэшей ---
        check_tasks = [check_cache(client, tf) for tf in TIME_FRAMES_TO_TEST]
        check_results = await asyncio.gather(*check_tasks)

        if all(check_results):
            logging.info(f"--- ТЕСТ УСПЕШЕН: Все {len(TIME_FRAMES_TO_TEST)} таймфреймов успешно обработаны. ---")
        else:
            logging.critical("--- ТЕСТ ПРОВАЛЕН: Один или несколько таймфреймов не были готовы до таймаута. ---")

if __name__ == "__main__":
    asyncio.run(main())

