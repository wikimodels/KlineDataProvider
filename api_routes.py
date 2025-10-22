import logging
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict

# Импортируем очередь из модуля worker
from worker import task_queue
# Импортируем функцию для получения данных из кэша
from cache_manager import get_from_cache
# Импортируем нашу функцию-очиститель
from api_utils import make_serializable

# Создаем объект Router
router = APIRouter()

# --- Модели данных и Конфигурация ---
class MarketDataRequest(BaseModel):
    timeframe: str

ALLOWED_TIMEFRAMES = ['1m', '5m', '15m', '30m', '1h', '4h', '8h', '12h', '1d']

# --- Определения Эндпоинтов ---

@router.post("/get-market-data", status_code=202)
async def schedule_market_data_job(request: MarketDataRequest):
    """
    Принимает запрос на сбор данных, добавляет его в очередь и немедленно отвечает.
    """
    if request.timeframe not in ALLOWED_TIMEFRAMES:
        raise HTTPException(status_code=400, detail="Недопустимый таймфрейм.")
        
    await task_queue.put(request.timeframe)
    logging.info(f"API: Задача для таймфрейма '{request.timeframe}' добавлена в очередь.")
    
    return {"message": f"Задача для '{request.timeframe}' принята в обработку."}

@router.get("/cache/{timeframe}")
async def get_cached_data(timeframe: str):
    """
    Возвращает данные из кэша для указанного таймфрейма, предварительно
    очистив их для безопасной JSON-сериализации.
    """
    if timeframe not in ALLOWED_TIMEFRAMES:
        raise HTTPException(status_code=400, detail="Недопустимый таймфрейм.")
    
    cached_data = get_from_cache(timeframe)
    
    if cached_data:
        # --- ВОТ КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Вызываем очистку перед отправкой ---
        safe_data = make_serializable(cached_data)
        return JSONResponse(content=safe_data)
    else:
        raise HTTPException(status_code=404, detail=f"Кэш для таймфрейма '{timeframe}' пуст.")

@router.get("/queue-status")
async def get_queue_status():
    """Возвращает текущее количество задач в очереди."""
    return {"tasks_in_queue": task_queue.qsize()}

@router.get("/health")
async def health_check():
    """Простой эндпоинт для проверки, что сервер жив."""
    return {"status": "ok"}

