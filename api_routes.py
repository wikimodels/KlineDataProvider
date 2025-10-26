import logging
import os # --- Изменение №1: Добавляем os ---
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Security
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from pydantic import BaseModel
from typing import List, Dict

# --- Изменение №1: Импорты для воркера, кэша и FR ---
from worker import task_queue, WORKER_LOCK_KEY # Добавляем WORKER_LOCK_KEY
from cache_manager import get_from_cache, redis_client # Добавляем redis_client
from api_utils import make_serializable

# Импорт Cron-Job функции (fr_fetcher)
try:
    from data_collector.fr_fetcher import run_fr_update_process
except ImportError:
    logging.critical("Не удалось импортировать run_fr_update_process. Эндпоинт /update-fr не будет работать.")
    async def run_fr_update_process():
        logging.error("Заглушка run_fr_update_process вызвана. fr_fetcher.py отсутствует или неисправен.")
# ----------------------------------------------------

# Создаем объект Router
router = APIRouter()

# --- Модели данных и Конфигурация ---
class MarketDataRequest(BaseModel):
    timeframe: str

# --- Изменение №1: Обновляем список, убирая '1m', '5m' и т.д. ---
ALLOWED_TIMEFRAMES = ['1h', '4h', '8h', '12h', '1d']
# -----------------------------------------------------------

# --- Изменение №1: Настройка безопасности для Cron-Job ---
CRON_SECRET = os.environ.get("CRON_SECRET")
security = HTTPBearer()

def verify_cron_secret(credentials: HTTPBearer = Security(security)):
    """Проверяет секретный токен для Cron-Job."""
    if not CRON_SECRET:
        logging.error("[CRON_JOB_API] Запрос отклонен: CRON_SECRET не установлен на сервере (503).")
        raise HTTPException(
            status_code=503,
            detail="Сервис недоступен: Секрет для Cron-Job не настроен."
        )
    
    if credentials.credentials != CRON_SECRET:
        logging.warning(f"[CRON_JOB_API] Запрос отклонен: Неверный токен (403).")
        raise HTTPException(
            status_code=403,
            detail="Доступ запрещен: Неверный токен."
        )
    return True
# ----------------------------------------------------


# --- Определения Эндпоинтов ---

# --- Изменение №1: Обновляем /get-market-data, добавляя проверку блокировки ---
@router.post("/get-market-data", status_code=202)
async def schedule_market_data_job(request: MarketDataRequest):
    """
    Принимает запрос на сбор данных, ПРОВЕРЯЕТ БЛОКИРОВКУ,
    добавляет его в очередь и немедленно отвечает.
    """
    timeframe = request.timeframe
    log_prefix = f"[{timeframe.upper()}]"
    
    if timeframe not in ALLOWED_TIMEFRAMES:
        logging.error(f"{log_prefix} API: Получен недопустимый таймфрейм {timeframe}.")
        raise HTTPException(status_code=400, detail="Недопустимый таймфрейм.")
        
    logging.info(f"{log_prefix} API: Получен запрос на запуск задачи...")

    # Проверка Redis
    if not redis_client:
        logging.error(f"{log_prefix} API: Redis недоступен. Не могу проверить блокировку.")
        raise HTTPException(
            status_code=503,
            detail="Сервис недоступен: Redis (для блокировки) не подключен."
        )

    # Проверяем, не занят ли воркер
    try:
        lock_status = redis_client.get(WORKER_LOCK_KEY)
        if lock_status:
            logging.warning(f"{log_prefix} API: Задача отклонена (409). Сборщик уже занят.")
            raise HTTPException(
                status_code=409,
                detail=f"Конфликт: Сборщик уже занят. Статус: {lock_status.decode('utf-8')}"
            )
    except Exception as e:
        logging.error(f"{log_prefix} API: Ошибка при проверке блокировки Redis: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Внутренняя ошибка сервера при проверке блокировки: {e}"
        )

    # Добавляем задачу в очередь
    try:
        await task_queue.put(timeframe)
        logging.info(f"{log_prefix} API: Задача для {timeframe} успешно добавлена в очередь.")
        return JSONResponse(
            status_code=202, # Accepted
            content={"message": f"Задача для {timeframe} принята в очередь."}
        )
    except Exception as e:
        logging.error(f"{log_prefix} API: Не удалось добавить задачу в очередь: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Внутренняя ошибка сервера при добавлении в очередь: {e}"
        )
# --- Конец Изменения №1 ---


@router.get("/cache/{timeframe}")
async def get_cached_data(timeframe: str):
    """
    (Код этой функции не изменен, он уже использует make_serializable)
    Возвращает данные из кэша для указанного таймфрейма, предварительно
    очистив их для безопасной JSON-сериализации.
    """
    if timeframe not in ALLOWED_TIMEFRAMES:
        raise HTTPException(status_code=400, detail="Недопустимый таймфрейм.")
    
    cached_data = get_from_cache(timeframe)
    
    if cached_data:
        # Вызываем очистку (из api_utils.py)
        safe_data = make_serializable(cached_data)
        return JSONResponse(content=safe_data)
    else:
        raise HTTPException(status_code=404, detail=f"Кэш для таймфрейма '{timeframe}' пуст.")


# --- Изменение №1: Новый эндпоинт для Cron-Job ---
@router.post("/api/v1/internal/update-fr")
async def trigger_fr_update(
    background_tasks: BackgroundTasks,
    is_authenticated: bool = Depends(verify_cron_secret) # Защита
):
    """
    ЗАЩИЩЕННЫЙ Эндпоинт.
    Запускает фоновую задачу обновления 'cache:global_fr'.
    Вызывается внешним Cron-Job (например, cron-job.org).
    """
    log_prefix = "[CRON_JOB_API]"
    logging.info(f"{log_prefix} Получен авторизованный запрос на обновление 'cache:global_fr'...")
    
    # Запускаем тяжелую задачу в фоновом режиме
    background_tasks.add_task(run_fr_update_process)
    
    logging.info(f"{log_prefix} Задача обновления FR принята в очередь (BackgroundTasks).")
    return JSONResponse(
        status_code=202, # Accepted
        content={"message": "Задача обновления 'cache:global_fr' принята в очередь."}
    )
# --- Конец Изменения №1 ---


@router.get("/queue-status")
async def get_queue_status():
    """(Код этой функции не изменен)"""
    return {"tasks_in_queue": task_queue.qsize()}

@router.get("/health")
async def health_check():
    """(Код этой функции не изменен)"""
    return {"status": "ok"}
