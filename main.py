import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
import asyncio
import os 

# --- 1. Настройка логгирования ---
try:
    from data_collector.logging_setup import setup_logging
    setup_logging()
except ImportError:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.warning("Не удалось импортировать logging_setup. Используется базовый конфиг.")

logger = logging.getLogger(__name__)

# --- 2. Импорт Воркера и Роутера ---
from worker import main 
from api_routes import router as api_router

 

# --- 3. Обработчик Lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan обработчик для событий startup и shutdown.
    """
    
    # --- ИЗМЕНЕНИЕ №2: Убрана загрузка FR и улучшены логи ---
    logger.info("=======================================================")
    logger.info("🚀 [STARTUP] FastAPI запущен.")
    
    try:
        # --- Startup Logic ---
        logger.info("[STARTUP 1/2] Запускаю фоновый воркер (data_collector) для Klines/OI...")
        asyncio.create_task(main()) 
        logger.info("[STARTUP 1/2] ✅ Фоновый воркер (data_collector) успешно запущен.")

        # Проверка SECRET_TOKEN
        if not os.environ.get("SECRET_TOKEN"):
             logger.warning("[STARTUP 2/2] ⚠️  SECRET_TOKEN не установлен. Эндпоинт /internal/update-fr НЕ БУДЕТ РАБОТАТЬ.")
        else:
             logger.info("[STARTUP 2/2] ✅ SECRET_TOKEN загружен. Эндпоинт /internal/update-fr активен.")

        logger.info("=======================================================")

    except Exception as e:
        logger.critical(f"--- 💥 КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАПУСКЕ: {e} ---", exc_info=True)
    
    yield 
    
    # --- Shutdown Logic ---
    logger.info("--- 🛑 FastAPI завершает работу. ---")
    # --- КОНЕЦ ИЗМЕНЕНИЯ №2 ---


app = FastAPI(lifespan=lifespan)

app.include_router(api_router) 

if __name__ == "__main__":
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000, 
        reload=False,
        log_config=None
    )