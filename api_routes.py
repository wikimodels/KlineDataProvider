import logging
import os 
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Security
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from pydantic import BaseModel
from typing import List, Dict

# --- Импорты для воркера, кэша и FR ---
# --- Изменение №1: Убираем task_queue, оставляем WORKER_LOCK_KEY ---
from worker import WORKER_LOCK_KEY 
from cache_manager import get_from_cache, redis_client # Добавляем redis_client
from api_utils import make_serializable
# -----------------------------------------------------------------

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

# --- Изменение №2: Разделение списков разрешенных таймфреймов ---
# Таймфреймы, для которых можно ЗАПУСТИТЬ сбор (POST)
POST_TIMEFRAMES = ['1h', '4h', '12h', '1d'] # 8h убран
# Таймфреймы, из которых можно ПРОЧИТАТЬ кэш (GET)
GET_TIMEFRAMES = ['1h', '4h', '8h', '12h', '1d'] # 8h оставлен
# ------------------------------------------------------------------

# --- Изменение №1: Добавляем ключ для Redis Queue ---
REDIS_TASK_QUEUE_KEY = "data_collector_task_queue"
# -------------------------------------------------


# --- Настройка безопасности для Cron-Job ---
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

@router.post("/get-market-data", status_code=202)
async def schedule_market_data_job(request: MarketDataRequest):
    """
    Принимает запрос на сбор данных, ПРОВЕРЯЕТ БЛОКИРОВКУ,
    добавляет его в очередь (теперь в Redis) и немедленно отвечает.
    """
    timeframe = request.timeframe
    log_prefix = f"[{timeframe.upper()}]"
    
    # --- Изменение №2: Используем POST_TIMEFRAMES ---
    if timeframe not in POST_TIMEFRAMES:
        logging.error(f"{log_prefix} API: Получен недопустимый таймфрейм {timeframe} для POST-запроса.")
        raise HTTPException(status_code=400, detail=f"Таймфрейм '{timeframe}' не может быть запрошен для сбора.")
    # ---------------------------------------------
        
    logging.info(f"{log_prefix} API: Получен запрос на запуск задачи...")

    # Проверка Redis
    if not redis_client:
        logging.error(f"{log_prefix} API: Redis недоступен. Не могу проверить блокировку.")
        raise HTTPException(
            status_code=503,
            detail="Сервис недоступен: Redis (для блокировки) не подключен."
        )

    # --- ИЗМЕНЕНИЕ №1: Исправлена логика обработки блокировки ---
    
    lock_status = None # Инициализируем
    try:
        # Блок try ТЕПЕРЬ содержит *только* сам запрос к Redis
        lock_status = redis_client.get(WORKER_LOCK_KEY)
        
    except Exception as e: 
        # Этот блок ТЕПЕРЬ ловит *только* ошибки Redis (например, ConnectionError)
        logging.error(f"{log_prefix} API: Ошибка при проверке блокировки Redis: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, # Это *корректный* 500, т.к. мы не смогли проверить блокировку
            detail=f"Внутренняя ошибка сервера при проверке блокировки: {e}"
        )

    # Проверка статуса и вызов 409 вынесены ИЗ блока try
    if lock_status:
        logging.warning(f"{log_prefix} API: Задача отклонена (409). Сборщик уже занят.")
        detail_status = lock_status # Upstash-redis возвращает str
        # Это исключение (409) теперь НЕ БУДЕТ поймано блоком except Exception
        raise HTTPException(
            status_code=409,
            detail=f"Конфликт: Сборщик уже занят. Статус: {detail_status}"
        )
    # --- Конец Изменения №1 ---


    # Добавляем задачу в очередь (Этот блок try...except остается без изменений)
    try:
        # --- Изменение №1: Меняем asyncio.Queue на Redis List (rpush) ---
        redis_client.rpush(REDIS_TASK_QUEUE_KEY, timeframe)
        # -------------------------------------------------------------
        
        logging.info(f"{log_prefix} API: Задача для {timeframe} успешно добавлена в очередь Redis.")
        return JSONResponse(
            status_code=202, # Accepted
            content={"message": f"Задача для {timeframe} принята в очередь."}
        )
    except Exception as e:
        logging.error(f"{log_prefix} API: Не удалось добавить задачу в очередь Redis: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Внутренняя ошибка сервера при добавлении в очередь: {e}"
        )


@router.get("/cache/{timeframe}")
async def get_cached_data(timeframe: str):
    """
    Возвращает данные из кэша для указанного таймфрейма, предварительно
    очистив их для безопасной JSON-сериализации.
    """
    # --- Изменение №2: Используем GET_TIMEFRAMES ---
    if timeframe not in GET_TIMEFRAMES:
        raise HTTPException(status_code=400, detail=f"Таймфрейм '{timeframe}' недопустим для чтения из кэша.")
    # ---------------------------------------------
    
    cached_data = get_from_cache(timeframe)
    
    if cached_data:
        # Вызываем очистку (из api_utils.py)
        safe_data = make_serializable(cached_data)
        return JSONResponse(content=safe_data)
    else:
        raise HTTPException(status_code=404, detail=f"Кэш для таймфрейма '{timeframe}' пуст.")


# --- Новый эндпоинт для Cron-Job ---
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
# --- Конец Изменения ---


@router.get("/queue-status")
async def get_queue_status():
    """
    Возвращает текущее количество задач в очереди.
    --- Изменение №1: Читаем длину списка Redis ---
    """
    q_size = 0
    if redis_client:
        try:
            q_size = redis_client.llen(REDIS_TASK_QUEUE_KEY)
        except Exception as e:
            logging.error(f"[QUEUE_STATUS] Ошибка получения длины очереди Redis: {e}")
            raise HTTPException(status_code=503, detail="Не удалось получить статус очереди из Redis.")
            
    return {"tasks_in_queue": q_size}
    # ---------------------------------------------

@router.get("/health")
async def health_check():
    """Простой эндпоинт для проверки, что сервер жив."""
    return {"status": "ok"}
