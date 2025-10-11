import os
import warnings
import logging
import uvicorn
import asyncio
from dotenv import load_dotenv
from fastapi import FastAPI
from contextlib import asynccontextmanager

# --- Импорт наших модулей ---
# Импортируем роутер, который содержит все эндпоинты
from api_routes import router
# Импортируем фоновый обработчик
from worker import background_worker

# --- Начальная настройка ---
# Загружаем переменные окружения из .env файла
load_dotenv()
# Настраиваем базовое логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Игнорируем предупреждения, не относящиеся к нашей логике
warnings.filterwarnings("ignore", message=".*pkg_resources is deprecated.*")


# --- Управление жизненным циклом приложения ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    При старте сервера запускает фоновый обработчик (worker).
    """
    logging.info("Сервер запускается...")
    # Запускаем нашего "рабочего" в фоновом режиме
    asyncio.create_task(background_worker())
    logging.info("Сервер запущен, фоновый обработчик активен.")
    yield
    # Этот код выполнится при остановке сервера
    logging.info("Сервер останавливается.")


# --- Создание и конфигурация основного приложения FastAPI ---
# Создаем основной экземпляр приложения и передаем ему менеджер жизненного цикла
app = FastAPI(lifespan=lifespan)

# Подключаем все эндпоинты из модуля api_routes
app.include_router(router)


# --- Точка входа для запуска сервера ---
if __name__ == "__main__":
    # Эта команда запускает сервер для локальной разработки
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

