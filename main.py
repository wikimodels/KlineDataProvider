import uvicorn
from fastapi import FastAPI
import logging
import asyncio
import os # Для CRON_SECRET

# --- 1. Настройка логгирования ---
try:
    from data_collector.logging_setup import logger
except ImportError:
    # Фоллбэк
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.warning("Не удалось импортировать logging_setup. Используется базовый конфиг.")

# --- 2. Импорт Воркера и Роутера ---
from worker import background_worker
from api_routes import router as api_router
# --- Изменение №1: Импортируем fr_fetcher для startup ---
try:
    from data_collector.fr_fetcher import run_fr_update_process
except ImportError:
    logger.critical("Не удалось импортировать run_fr_update_process. Первичная загрузка FR невозможна.")
    async def run_fr_update_process(): # Пустышка
        logger.error("Заглушка run_fr_update_process вызвана.")
# -----------------------------------------------------

app = FastAPI()

# --- 3. Событие Startup (ИЗМЕНЕНО) ---
@app.on_event("startup")
async def startup_event():
    """
    (ИЗМЕНЕНО)
    1. СНАЧАЛА принудительно загружает cache:global_fr.
    2. ПОТОМ запускает фоновый воркер (Klines/OI).
    """
    logger.info("FastAPI запущен. --- НАЧИНАЮ ПЕРВИЧНУЮ ЗАГРУЗКУ FR ---")
    try:
        # --- Изменение №1: Сначала ждем загрузки FR ---
        await run_fr_update_process()
        logger.info("--- ПЕРВИЧНАЯ ЗАГРУЗКА FR ЗАВЕРШЕНА ---")
        # ----------------------------------------
    except Exception as e:
        logger.critical(f"--- КРИТИЧЕСКАЯ ОШИБКА ПРИ ПЕРВИЧНОЙ ЗАГРУЗКЕ FR: {e} ---", exc_info=True)
        # (Продолжаем, но Klines/OI, вероятно, будут без FR, если кэша не было)
    
    logger.info("Запускаю фоновый воркер (data_collector) для Klines/OI...")
    asyncio.create_task(background_worker())
    logger.info("Фоновый воркер (data_collector) успешно запущен.")
    
    # Проверка CRON_SECRET (из твоего api_routes.py)
    if not os.environ.get("CRON_SECRET"):
         logger.warning("!!! CRON_SECRET не установлен. Эндпоинт /api/v1/internal/update-fr НЕ БУДЕТ РАБОТАТЬ. !!!")
    else:
         logger.info("CRON_SECRET загружен. Эндпоинт /api/v1/internal/update-fr активен.")

# --- 4. Подключение эндпоинтов ---
# (Код этой секции не изменен)
app.include_router(api_router)
# -------------------------------------------------------

# --- 5. Запуск Uvicorn ---
# (Код этой секции не изменен)
if __name__ == "__main__":
    # log_config=None предотвращает uvicorn от перезаписи наших логов
    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=None)

