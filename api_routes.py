import logging
import os 
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Security
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from pydantic import BaseModel
from typing import List, Dict

# --- Импорты для воркера, кэша и FR ---
from worker import WORKER_LOCK_KEY 
from cache_manager import get_from_cache, redis_client
from api_utils import make_serializable

# --- Импорты из config ---
try:
    from config import (
        POST_TIMEFRAMES,
        ALLOWED_CACHE_KEYS, 
        REDIS_TASK_QUEUE_KEY,
        SECRET_TOKEN
    )
except ImportError:
    # Фоллбэки
    POST_TIMEFRAMES = ['1h', '4h', '12h', '1d']
    ALLOWED_CACHE_KEYS = ['1h', '4h', '8h', '12h', '1d', 'global_fr']
    REDIS_TASK_QUEUE_KEY = "data_collector_task_queue"
    SECRET_TOKEN = os.environ.get("SECRET_TOKEN")

# --- ИЗМЕНЕНИЕ: Нам больше не нужно импортировать run_fr_update_process здесь ---
# (Он будет вызываться только из worker.py)
# ------------------------------------------------------------------------

# Создаем объект Router
router = APIRouter()

# --- Модели данных ---
class MarketDataRequest(BaseModel):
    timeframe: str

# --- Настройка безопасности для Cron-Job ---
security = HTTPBearer()

def verify_cron_secret(credentials: HTTPBearer = Security(security)):
    """Проверяет секретный токен для Cron-Job."""
    if not SECRET_TOKEN:
        logging.error("[CRON_JOB_API] Запрос отклонен: SECRET_TOKEN не установлен на сервере (503).")
        raise HTTPException(
            status_code=503,
            detail="Сервис недоступен: Секрет для Cron-Job не настроен."
        )
    
    if credentials.credentials != SECRET_TOKEN:
        logging.warning(f"[CRON_JOB_API] Запрос отклонен: Неверный токен (403).")
        raise HTTPException(
            status_code=403,
            detail="Доступ запрещен: Неверный токен."
        )
    return True
# ----------------------------------------------------


# --- Функция-хелпер для проверки блокировки и добавления в очередь ---
# (Мы вынесли логику из schedule_market_data_job, чтобы использовать ее дважды)
def _check_lock_and_queue_task(task_name: str, log_prefix: str) -> JSONResponse:
    """
    Проверяет WORKER_LOCK_KEY и, если он свободен, 
    добавляет 'task_name' в REDIS_TASK_QUEUE_KEY.
    """
    if not redis_client:
        logging.error(f"{log_prefix} API: Redis недоступен. Не могу проверить блокировку.")
        raise HTTPException(
            status_code=503,
            detail="Сервис недоступен: Redis (для блокировки) не подключен."
        )

    lock_status = None
    try:
        lock_status = redis_client.get(WORKER_LOCK_KEY)
    except Exception as e: 
        logging.error(f"{log_prefix} API: Ошибка при проверке блокировки Redis: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Внутренняя ошибка сервера при проверке блокировки: {e}"
        )

    if lock_status:
        logging.warning(f"{log_prefix} API: Задача отклонена (409). Сборщик уже занят.")
        detail_status = lock_status
        raise HTTPException(
            status_code=409,
            detail=f"Конфликт: Сборщик уже занят. Статус: {detail_status}"
        )

    try:
        redis_client.rpush(REDIS_TASK_QUEUE_KEY, task_name)
        logging.info(f"{log_prefix} API: Задача для {task_name} успешно добавлена в очередь Redis.")
        return JSONResponse(
            status_code=202, # Accepted
            content={"message": f"Задача для {task_name} принята в очередь."}
        )
    except Exception as e:
        logging.error(f"{log_prefix} API: Не удалось добавить задачу в очередь Redis: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Внутренняя ошибка сервера при добавлении в очередь: {e}"
        )
# ----------------------------------------------------


# --- Определения Эндпоинтов ---

@router.post("/get-market-data", status_code=202)
async def schedule_market_data_job(request: MarketDataRequest):
    """
    Принимает запрос на сбор Klines/OI, проверяет блокировку 
    и добавляет в очередь Redis.
    """
    timeframe = request.timeframe
    log_prefix = f"[{timeframe.upper()}]"
    
    if timeframe not in POST_TIMEFRAMES:
        logging.error(f"{log_prefix} API: Получен недопустимый таймфрейм {timeframe} для POST-запроса.")
        raise HTTPException(status_code=400, detail=f"Таймфрейм '{timeframe}' не может быть запрошен для сбора.")
        
    logging.info(f"{log_prefix} API: Получен запрос на запуск задачи...")
    
    # --- ИЗМЕНЕНИЕ: Используем хелпер ---
    return _check_lock_and_queue_task(timeframe, log_prefix)


@router.get("/cache/{key}")
async def get_cached_data(key: str): 
    """
    Возвращает данные из кэша для указанного ключа (timeframe или global_fr).
    """
    if key not in ALLOWED_CACHE_KEYS:
        raise HTTPException(status_code=400, detail=f"Ключ '{key}' недопустим для чтения из кэша.")
    
    cached_data = get_from_cache(key)
    
    if cached_data:
        safe_data = make_serializable(cached_data)
        return JSONResponse(content=safe_data)
    else:
        raise HTTPException(status_code=404, detail=f"Ключ '{key}' пуст.")


@router.post("/api/v1/internal/update-fr", status_code=202)
async def trigger_fr_update(
    is_authenticated: bool = Depends(verify_cron_secret) # Защита
):
    """
    ЗАЩИЩЕННЫЙ Эндпоинт.
    Проверяет блокировку и добавляет задачу 'fr' в очередь Redis.
    """
    log_prefix = "[CRON_JOB_API]"
    logging.info(f"{log_prefix} Получен авторизованный запрос на обновление 'cache:global_fr'...")
    
    # --- ИЗМЕНЕНИЕ: Используем тот же хелпер, что и /get-market-data ---
    # (Больше не используем BackgroundTasks)
    return _check_lock_and_queue_task("fr", log_prefix)
    # -----------------------------------------------------------------


@router.get("/queue-status")
async def get_queue_status():
    """
    Возвращает текущее количество задач в очереди.
    """
    q_size = 0
    if redis_client:
        try:
            q_size = redis_client.llen(REDIS_TASK_QUEUE_KEY)
        except Exception as e:
            logging.error(f"[QUEUE_STATUS] Ошибка получения длины очереди Redis: {e}")
            raise HTTPException(status_code=503, detail="Не удалось получить статус очереди из Redis.")
            
    return {"tasks_in_queue": q_size}


@router.get("/health")
async def health_check():
    """Простой эндпоинт для проверки, что сервер жив."""
    return {"status": "ok"}