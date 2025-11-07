import uvicorn
from fastapi import FastAPI
# --- ИЗМЕНЕНИЕ: Импортируем asynccontextmanager ---
from contextlib import asynccontextmanager
import logging
import asyncio
import os 
# ----------------------------------------------------

# --- 1. Настройка логгирования ---
try:
    from data_collector.logging_setup import logger
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.warning("Не удалось импортировать logging_setup. Используется базовый конфиг.")

# --- 2. Импорт Воркера и Роутера ---
from worker import background_worker
from api_routes import router as api_router
try:
    from data_collector.fr_fetcher import run_fr_update_process
except ImportError:
    logger.critical("Не удалось импортировать run_fr_update_process. Первичная загрузка FR невозможна.")
    async def run_fr_update_process(): 
        logger.error("Заглушка run_fr_update_process вызвана.")
# -----------------------------------------------------

# --- 3. Обработчик Lifespan (ЗАМЕНЯЕТ @app.on_event) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan обработчик для событий startup и shutdown.
    """
    logger.info("FastAPI запущен. --- НАЧИНАЮ ПЕРВИЧНУЮ ЗАГРУЗКУ FR ---")
    
    # --- Startup Logic ---
    try:
        # 1. Сначала ждем загрузки FR
        await run_fr_update_process()
        logger.info("--- ПЕРВИЧНАЯ ЗАГРУЗКА FR ЗАВЕРШЕНА ---")
        
        # 2. Запускаем фоновый воркер (Klines/OI)
        logger.info("Запускаю фоновый воркер (data_collector) для Klines/OI...")
        asyncio.create_task(background_worker())
        logger.info("Фоновый воркер (data_collector) успешно запущен.")

        # 3. Проверка SECRET_TOKEN
        if not os.environ.get("SECRET_TOKEN"):
             logger.warning("!!! SECRET_TOKEN не установлен. Эндпоинт /api/v1/internal/update-fr НЕ БУДЕТ РАБОТАТЬ. !!!")
        else:
             logger.info("SECRET_TOKEN загружен. Эндпоинт /api/v1/internal/update-fr активен.")

    except Exception as e:
        logger.critical(f"--- КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАПУСКЕ: {e} ---", exc_info=True)
    
    # --- Yield (Сервер готов принимать запросы) ---
    yield # Это точка, где приложение начинает работать
    
    # --- Shutdown Logic (Можно добавить очистку, если нужно) ---
    logger.info("FastAPI завершает работу. Выполняю очистку...")


app = FastAPI(lifespan=lifespan) # --- ИЗМЕНЕНИЕ: Передаем lifespan в FastAPI ---
# -----------------------------------------------------------------------------

# --- 4. Подключение эндпоинтов ---
app.include_router(api_router)

# --- 5. Запуск Uvicorn ---
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=None)
