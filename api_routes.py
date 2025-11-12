import logging
import os 
import json
from fastapi import APIRouter, HTTPException, Depends, Security
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from pydantic import BaseModel
from typing import List, Dict, Any, Optional 

# --- Импорты для воркера, кэша и FR ---
# --- ИЗМЕНЕНИЕ №1: Импортируем add_task_to_queue ---
from cache_manager import load_from_cache, get_redis_connection, add_task_to_queue, get_worker_status 
from api_utils import make_serializable

# --- Импорты из config ---
try:
    from config import (
        POST_TIMEFRAMES,
        ALLOWED_CACHE_KEYS, 
        REDIS_TASK_QUEUE_KEY,
        SECRET_TOKEN,
        WORKER_LOCK_KEY,
        WORKER_LOCK_VALUE
    )
except ImportError:
    # Фоллбэки
    POST_TIMEFRAMES = ['1h', '4h', '12h', '1d']
    ALLOWED_CACHE_KEYS = ['1h', '4h', '8h', '12h', '1d', 'global_fr']
    REDIS_TASK_QUEUE_KEY = "data_collector_task_queue"
    SECRET_TOKEN = os.environ.get("SECRET_TOKEN")
    WORKER_LOCK_KEY = "data_collector_lock"
    WORKER_LOCK_VALUE = "processing"
    
# Создаем объект Router
router = APIRouter()

class MarketDataRequest(BaseModel):
    timeframes: List[str]
    symbols: Optional[List[str]] = None

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
        logging.warning("[CRON_JOB_API] Запрос отклонен: Неверный токен (403).")
        raise HTTPException(
            status_code=403,
            detail="Доступ запрещен: Неверный токен."
        )
    return True


async def _check_lock_and_queue_task(task_payload: Dict[str, Any], log_prefix: str) -> JSONResponse:
    """Проверяет блокировку и добавляет задачу в очередь Redis."""
    
    redis_conn = await get_redis_connection() 
    
    if not redis_conn:
        logging.error(f"{log_prefix} API: Redis недоступен. Не могу проверить блокировку.")
        raise HTTPException(
            status_code=503,
            detail="Сервис недоступен: Redis (для блокировки) не подключен."
        )
        
    lock_status = None
    try:
        lock_status = await redis_conn.get(WORKER_LOCK_KEY)
    except Exception as e: 
        logging.error(f"{log_prefix} API: Ошибка при проверке блокировки Redis: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Внутренняя ошибка сервера при проверке блокировки: {e}"
        )

    # lock_status уже str.
    if lock_status and lock_status.decode('utf-8') == WORKER_LOCK_VALUE: # ИСПРАВЛЕНО: Декодируем байты
        msg = f"{log_prefix} Воркер занят. Задача не добавлена."
        logging.warning(f"{log_prefix} API: Задача отклонена (409). Сборщик уже занят.")
        return JSONResponse({"status": "worker_locked", "message": msg}, status_code=409)

    task_name = task_payload.get('timeframe', 'UNKNOWN')
    try:
        # ДОБАВЛЕНО: Проверяем размер очереди ДО добавления
        queue_size_before = await redis_conn.llen(REDIS_TASK_QUEUE_KEY)
        logging.info(f"{log_prefix} [DEBUG] Размер очереди ДО добавления: {queue_size_before}")
        
        await add_task_to_queue(task_payload.get('timeframe'), redis_conn) # ИСПРАВЛЕНО: Использование add_task_to_queue
        
        # ДОБАВЛЕНО: Проверяем размер очереди ПОСЛЕ добавления
        queue_size_after = await redis_conn.llen(REDIS_TASK_QUEUE_KEY)
        logging.info(f"{log_prefix} [DEBUG] Размер очереди ПОСЛЕ добавления: {queue_size_after}")
        logging.info(f"{log_prefix} [DEBUG] Использованный ключ очереди: '{REDIS_TASK_QUEUE_KEY}'")
        
        msg = f"{log_prefix} Задача '{task_name}' успешно добавлена в очередь."
        logging.info(msg)
        return JSONResponse({"status": "queued", "message": msg}, status_code=202)
    except Exception as e:
        logging.error(f"{log_prefix} API: Не удалось добавить задачу в очередь Redis: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Внутренняя ошибка сервера при добавлении в очередь: {e}"
        )
        
        
@router.post("/get-market-data", response_class=JSONResponse)
async def get_market_data(request: MarketDataRequest):
    """
    Основной эндпоинт. 1. Проверяет, есть ли данные в кэше. 2. Если нет, добавляет задачи в очередь.
    """
    if not request.timeframes:
        raise HTTPException(status_code=400, detail="Необходимо указать хотя бы один timeframe.")

    response_data = {}
    tasks_queued = False
    
    redis_conn = await get_redis_connection() 
    if not redis_conn:
        raise HTTPException(status_code=503, detail="Сервис недоступен: Redis не подключен.")

    # --- ИЗМЕНЕНИЕ №1 и №2: Убираем предварительную проверку worker_status ---
    # is_worker_locked = ... (УДАЛЕНО)

    for tf in request.timeframes:
        if tf not in POST_TIMEFRAMES:
            raise HTTPException(status_code=400, detail=f"Timeframe '{tf}' не поддерживается для запроса данных.")

        # --- БИЗНЕС-ЛОГИКА: Проверяем кэш ---
        cached_data = await load_from_cache(tf, redis_conn=redis_conn)
        
        if cached_data and cached_data.get('data'): # ИСПРАВЛЕНО: Проверяем на наличие данных
            # 1. Если данные есть
            if request.symbols:
                filtered_data = [
                    item for item in cached_data.get('data', []) 
                    if item.get('symbol') in request.symbols
                ]
                response_data[tf] = {"data": make_serializable(filtered_data), "audit": cached_data.get('audit', {})}
            else:
                response_data[tf] = {"data": make_serializable(cached_data.get('data', [])), "audit": cached_data.get('audit', {})}
        
        else:
            # --- ИЗМЕНЕНИЕ №1: Логика "промаха" кэша ---
            # 2. Если данных нет, ставим задачу в очередь
            # Убираем проверку is_worker_locked. API не должен решать,
            # занят ли воркер. Он должен просто поставить задачу в очередь.
            
            log_prefix = f"[API_REQUEST] Timeframe '{tf}'"
            task_payload = {"timeframe": tf}
            task_name = task_payload.get('timeframe', 'UNKNOWN')
            
            try:
                # Напрямую добавляем в очередь
                await add_task_to_queue(task_name, redis_conn)
                msg = f"{log_prefix} Задача '{task_name}' успешно добавлена в очередь."
                logging.info(msg)
                
                response_data[tf] = {"status": "queued", "message": f"Данные '{tf}' отсутствуют. Задача поставлена в очередь."}
                tasks_queued = True
                
            except Exception as e:
                logging.error(f"{log_prefix} API: Не удалось добавить задачу в очередь Redis: {e}", exc_info=True)
                # Сообщаем клиенту, что произошла ошибка
                response_data[tf] = {"status": "queue_error", "message": f"Ошибка постановки задачи '{tf}' в очередь."}
            # --- КОНЕЦ ИЗМЕНЕНИЯ №1 ---
            
    # --- ИЗМЕНЕНИЕ №2: Финальный Ответ ---
    if tasks_queued:
         # Некоторые данные отсутствовали, и задачи были поставлены в очередь.
        return JSONResponse({
            "status": "queued",
            "message": "Некоторые данные отсутствовали и были поставлены в очередь. Повторите запрос.",
            "data": response_data
        }, status_code=202)
        
    else:
        # Все данные найдены в кэше
        return JSONResponse(response_data, status_code=200)
    # --- КОНЕЦ ИЗМЕНЕНИЯ №2 ---


@router.post("/internal/update-fr", status_code=202)
async def trigger_fr_update(
    is_authenticated: bool = Depends(verify_cron_secret) 
):
    """
    ЗАЩИЩЕННЫЙ Эндпоинт. Проверяет блокировку и добавляет задачу 'global_fr' в очередь Redis.
    (Эта функция не изменилась, т.к. для Cron-задачи логика _check_lock_and_queue_task корректна)
    """
    log_prefix = "[CRON_JOB_API]"
    logging.info(f"{log_prefix} Получен авторизованный запрос на обновление 'cache:global_fr'...")
    
    task_payload = {"timeframe": "global_fr"}
    return await _check_lock_and_queue_task(task_payload, log_prefix)
    
@router.get("/get-cache/{key}", response_class=JSONResponse)
async def get_raw_cache(key: str):
    """
    Возвращает сырые данные из кэша Redis по ключу. 
    """
    if key not in ALLOWED_CACHE_KEYS:
        raise HTTPException(status_code=400, detail=f"Ключ '{key}' не разрешен.")

    redis_conn = await get_redis_connection()
    if not redis_conn:
        raise HTTPException(status_code=503, detail="Сервис недоступен: Redis не подключен.")

    data = await load_from_cache(key, redis_conn=redis_conn)
    
    if data:
        safe_data = make_serializable(data)
        return JSONResponse(content=safe_data)
    else:
        raise HTTPException(status_code=404, detail=f"Ключ '{key}' пуст.")


@router.get("/queue-status")
async def get_queue_status():
    """
    Возвращает текущее количество задач в очереди.
    """
    q_size = 0
    redis_conn = await get_redis_connection()
    
    if redis_conn:
        try:
            q_size = await redis_conn.llen(REDIS_TASK_QUEUE_KEY)
        except Exception as e:
            logging.error(f"[QUEUE_STATUS] Ошибка при чтении длины очереди: {e}", exc_info=True)
            raise HTTPException(status_code=503, detail="Не удалось получить статус очереди из Redis.")
    else:
        raise HTTPException(status_code=503, detail="Сервис недоступен: Redis не подключен.")
    
    return {"queue_size": q_size}

@router.get("/health")
@router.head("/health")
async def health_check():
    """Простой эндпоинт для проверки, что сервер жив."""
    return {"status": "ok"}