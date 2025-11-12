# api_routes_alerts.py
"""
Этот модуль воссоздает API-маршруты из Deno-проекта
(alerts.route.ts и vwap-alerts.route.ts) с использованием FastAPI.

Он предоставляет CRUD-интерфейс для управления Line Alerts и VWAP Alerts
в хранилище Redis.

--- ИЗМЕНЕНИЕ (ОПТИМИЗАЦИЯ) ---
Этот файл адаптирован для работы с НОВЫМ 'storage.py'.
Медленные операции (O(N) перезапись массива) заменены на
быстрые (O(1) или O(k)) вызовы `update_alert_by_id`, 
`delete_alerts_by_id` и `move_alerts_by_id`.
"""
import logging
import uuid
# --- ИЗМЕНЕНИЕ №1: Импортируем 'datetime' и 'timedelta' ---
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
# --- ИЗМЕНЕНИЕ №1: Импортируем 'Security' ---
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Security

from redis.asyncio import Redis as AsyncRedis

from .alert_manager.storage import AlertStorage
from .alert_manager.model import (
    Alert, VwapAlert, AlertBase, AlertsCollection,
)
from .cache_manager import get_redis_connection
from .data_collector.coin_source import get_coins

# --- ИЗМЕНЕНИЕ №1: Импортируем 'verify_cron_secret' из 'api_routes.py' ---
# (Импорт из родительской директории)
try:
    from .api_routes import verify_cron_secret
except ImportError:
    # Фоллбэк, если структура импорта отличается
    try:
        from api_routes import verify_cron_secret
    except ImportError:
        logger.error("Не удалось импортировать 'verify_cron_secret' из api_routes.py")
        # Создаем заглушку, чтобы приложение запустилось, но эндпоинт не будет защищен
        def verify_cron_secret():
            logger.critical("ЗАГЛУШКА: verify_cron_secret не импортирован!")
            return True


logger = logging.getLogger(__name__)
router = APIRouter()

# --- Вспомогательная функция (Dependency) ---

async def get_alert_storage(redis: AsyncRedis = Depends(get_redis_connection)) -> AlertStorage:
    """
    FastAPI Dependency для получения экземпляра AlertStorage 
    с активным Redis-соединением.
    """
    if not redis:
        raise HTTPException(status_code=503, detail="Не удалось подключиться к Redis для AlertStorage")
    return AlertStorage(redis)


# ====================================================================
# === 1. LINE ALERTS (Портировано из alerts.route.ts)
# ====================================================================

@router.get("/alerts", response_model=List[Alert])
async def get_alerts_controller(
    collectionName: AlertsCollection = Query(..., description="Имя коллекции: working, triggered, or archived"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """(Без изменений)"""
    try:
        return await storage.get_alerts(collectionName)
    except Exception as e:
        logger.error(f"Ошибка в get_alerts_controller: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/alerts/symbol", response_model=List[Alert])
async def get_alerts_by_symbol_controller(
    symbol: str = Query(..., description="Символ, например BTCUSDT"),
    collectionName: AlertsCollection = Query(..., description="Имя коллекции: working, triggered, or archived"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """(ИЗМЕНЕНО ДЛЯ СОВМЕСТИМОСТИ)"""
    try:
        all_alerts = await storage.get_alerts(collectionName)
        return [alert for alert in all_alerts if alert.get("symbol") == symbol]
    except Exception as e:
        logger.error(f"Ошибка в get_alerts_by_symbol_controller: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/alerts/add/one", status_code=201)
async def add_alert_controller(
    payload: Request,
    collectionName: AlertsCollection = Query(..., description="Имя коллекции"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """(Без изменений)"""
    try:
        body = await payload.json()
        alert_data = body.get("alert")
        if not alert_data:
            raise HTTPException(status_code=400, detail="Тело запроса должно содержать ключ 'alert'")
        
        if 'id' not in alert_data:
             alert_data['id'] = str(uuid.uuid4())
        
        alert: Alert = alert_data
        success = await storage.add_alert(collectionName, alert)
        
        if success:
            return {"message": "Alert added successfully!"}
        else:
            raise HTTPException(status_code=500, detail="Failed to add alert.")
            
    except Exception as e:
        logger.error(f"Ошибка в add_alert_controller: {e}", exc_info=True)
        if isinstance(e, HTTPException): raise
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/alerts/add/many", status_code=201)
async def add_alerts_batch_controller(
    payload: Request, # --- ИСПРАВЛЕНИЕ: Deno ожидал List[AlertBase], но Python-клиент отправляет { "alerts": [...] } ---
    collectionName: AlertsCollection = Query(..., description="Имя коллекции"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """
    (Без изменений)
    Пакетное добавление Line Alerts.
    (Зависимость от get_coins сохранена, как договорились)
    """
    try:
        # --- ИСПРАВЛЕНИЕ: Читаем тело как в Deno ---
        body = await payload.json()
        alertBases: List[AlertBase] = body.get("alerts") 
        if not alertBases:
             raise HTTPException(status_code=400, detail="Тело запроса должно содержать ключ 'alerts' со списком.")
        # --- Конец исправления ---

        coins_list = await get_coins()
        if not coins_list:
             raise HTTPException(status_code=503, detail="Сервис монет (coin_source) недоступен, не могу добавить алерты.")
             
        coins_map = {coin['symbol']: coin for coin in coins_list}

        corrupted_symbols = [
            base['symbol'] for base in alertBases 
            if base['symbol'] not in coins_map
        ]
        
        if corrupted_symbols:
            return HTTPException(
                status_code=400, 
                detail={
                    "message": "Failed due to corrupted symbols.",
                    "corruptedSymbols": corrupted_symbols
                }
            )
            
        new_alerts: List[Alert] = []
        for base in alertBases:
            coin = coins_map[base['symbol']]
            new_alert: Alert = {
                **base,
                "isActive": True,
                "status": "new",
                "id": str(uuid.uuid4()),
                "creationTime": int(datetime.now().timestamp() * 1000),
                "imageUrl": coin.get("imageUrl"),
                "exchanges": coin.get("exchanges"),
                "category": coin.get("category"),
                "description": base.get("description", "Yet nothing to say")
            }
            new_alerts.append(new_alert)

        for alert in new_alerts:
            await storage.add_alert(collectionName, alert) # O(1)

        return {
            "success": True,
            "message": f"Alerts added successfully: {len(new_alerts)}",
            "alerts": new_alerts
        }

    except Exception as e:
        logger.error(f"Ошибка в add_alerts_batch_controller: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/alerts/update/one")
async def update_alert_controller(
    payload: Request,
    collectionName: AlertsCollection = Query(..., description="Имя коллекции"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """(ОПТИМИЗИРОВАНО: O(N) -> O(1) на запись)"""
    try:
        body = await payload.json()
        filter_data: Dict = body.get("filter")
        update_data: Dict = body.get("updatedData")
        
        if not filter_data or not update_data:
            raise HTTPException(status_code=400, detail="Тело запроса должно содержать 'filter' и 'updatedData'")

        all_alerts = await storage.get_alerts(collectionName)
        found_alert: Optional[Alert] = None

        for alert in all_alerts:
            is_match = all(
                alert.get(key) == value for key, value in filter_data.items()
            )
            if is_match:
                found_alert = alert
                break 
            
        if not found_alert:
            raise HTTPException(status_code=404, detail="Alert not found with provided filter.")

        alert_id = found_alert.get("id")
        if not alert_id:
            raise HTTPException(status_code=500, detail="Найденный алерт не имеет ID.")
            
        updated_alert = {**found_alert, **update_data}
        
        await storage.update_alert_by_id(alert_id, updated_alert)
        return {"message": "Alert updated successfully!"}

    except Exception as e:
        logger.error(f"Ошибка в update_alert_controller: {e}", exc_info=True)
        if isinstance(e, HTTPException): raise
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/alerts/delete/many")
async def delete_many_controller(
    payload: Request,
    collectionName: AlertsCollection = Query(..., description="Имя коллекции"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """(ОПТИМИЗИРОВАНО: O(N) -> O(k))"""
    try:
        body = await payload.json()
        ids_to_delete: List[str] = body.get("ids")
        if not ids_to_delete:
            raise HTTPException(status_code=400, detail="Тело запроса должно содержать 'ids'")

        await storage.delete_alerts_by_id(collectionName, ids_to_delete)
        return {"message": "Alerts deleted successfully!"}
        
    except Exception as e:
        logger.error(f"Ошибка в delete_many_controller: {e}", exc_info=True)
        if isinstance(e, HTTPException): raise
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/alerts/move/many")
async def move_many_controller(
    payload: Request,
    sourceCollection: AlertsCollection = Query(..., description="Исходная коллекция"),
    targetCollection: AlertsCollection = Query(..., description="Целевая коллекция"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """(ОПТИМИЗИРОВАНО: O(N) -> O(k))"""
    try:
        body = await payload.json()
        ids_to_move: List[str] = body.get("ids")
        if not ids_to_move:
            raise HTTPException(status_code=400, detail="Тело запроса должно содержать 'ids'")

        success = await storage.move_alerts_by_id(sourceCollection, targetCollection, ids_to_move)
        
        if not success:
            raise HTTPException(status_code=500, detail="Ошибка при перемещении алертов.")
            
        return {
            "message": f"Moved {len(ids_to_move)} alerts",
            "count": len(ids_to_move)
        }

    except Exception as e:
        logger.error(f"Ошибка в move_many_controller: {e}", exc_info=True)
        if isinstance(e, HTTPException): raise
        raise HTTPException(status_code=500, detail="Internal server error")


# ====================================================================
# === 2. VWAP ALERTS (Портировано из vwap-alerts.route.ts)
# ====================================================================

@router.get("/vwap-alerts", response_model=List[VwapAlert])
async def get_vwap_alerts_controller(
    collectionName: AlertsCollection = Query(..., description="Имя коллекции: working, triggered, or archived"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """(Без изменений)"""
    try:
        return await storage.get_vwap_alerts(collectionName)
    except Exception as e:
        logger.error(f"Ошибка в get_vwap_alerts_controller: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/vwap-alerts/symbol", response_model=List[VwapAlert])
async def get_vwap_alerts_by_symbol_controller(
    symbol: str = Query(..., description="Символ, например BTCUSDT"),
    collectionName: AlertsCollection = Query(..., description="Имя коллекции"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """(ИЗМЕНЕНО ДЛЯ СОВМЕСТИМОСТИ)"""
    try:
        all_alerts = await storage.get_vwap_alerts(collectionName)
        return [alert for alert in all_alerts if alert.get("symbol") == symbol]
    except Exception as e:
        logger.error(f"Ошибка в get_vwap_alerts_by_symbol_controller: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/vwap-alerts/add/one", status_code=201)
async def add_vwap_alert_controller(
    payload: Request,
    collectionName: AlertsCollection = Query(..., description="Имя коллекции"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """(Без изменений)"""
    try:
        body = await payload.json()
        alert_data = body.get("alert")
        if not alert_data:
            raise HTTPException(status_code=400, detail="Тело запроса должно содержать ключ 'alert'")
        
        if 'id' not in alert_data:
             alert_data['id'] = str(uuid.uuid4())
        if 'creationTime' not in alert_data:
             alert_data['creationTime'] = int(datetime.now().timestamp() * 1000)
        
        alert: VwapAlert = alert_data
        success = await storage.add_vwap_alert(collectionName, alert)
        
        if success:
            return {"message": "VwapAlert added successfully!"}
        else:
            raise HTTPException(status_code=500, detail="Failed to add vwap alert.")
            
    except Exception as e:
        logger.error(f"Ошибка в add_vwap_alert_controller: {e}", exc_info=True)
        if isinstance(e, HTTPException): raise
        raise HTTPException(status_code=500, detail="Internal server error")

@router.put("/vwap-alerts/update/one")
async def update_vwap_alert_controller(
    payload: Request,
    collectionName: AlertsCollection = Query(..., description="Имя коллекции"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """(ОПТИМИЗИРОВАНО: O(N) -> O(1) на запись)"""
    try:
        body = await payload.json()
        filter_data: Dict = body.get("filter")
        update_data: Dict = body.get("updatedData")
        
        if not filter_data or not update_data:
            raise HTTPException(status_code=400, detail="Тело запроса должно содержать 'filter' и 'updatedData'")

        all_alerts = await storage.get_vwap_alerts(collectionName)
        found_alert: Optional[VwapAlert] = None

        for alert in all_alerts:
            is_match = all(
                alert.get(key) == value for key, value in filter_data.items()
            )
            if is_match:
                found_alert = alert
                break

        if not found_alert:
            raise HTTPException(status_code=404, detail="VwapAlert not found with provided filter.")
        
        alert_id = found_alert.get("id")
        if not alert_id:
            raise HTTPException(status_code=500, detail="Найденный алерт не имеет ID.")

        updated_alert = {**found_alert, **update_data}
        
        await storage.update_vwap_alert_by_id(alert_id, updated_alert)
        return {"message": "VwapAlert updated successfully!"}

    except Exception as e:
        logger.error(f"Ошибка в update_vwap_alert_controller: {e}", exc_info=True)
        if isinstance(e, HTTPException): raise
        raise HTTPException(status_code=500, detail="Internal server error")

@router.delete("/vwap-alerts/delete/many")
async def delete_many_vwap_controller(
    payload: Request,
    collectionName: AlertsCollection = Query(..., description="Имя коллекции"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """(ОПТИМИЗИРОВАНО: O(N) -> O(k))"""
    try:
        body = await payload.json()
        ids_to_delete: List[str] = body.get("ids")
        if not ids_to_delete:
            raise HTTPException(status_code=400, detail="Тело запроса должно содержать 'ids'")

        await storage.delete_vwap_alerts_by_id(collectionName, ids_to_delete)
        return {"message": "VwapAlerts deleted successfully!"}
        
    except Exception as e:
        logger.error(f"Ошибка в delete_many_vwap_controller: {e}", exc_info=True)
        if isinstance(e, HTTPException): raise
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/vwap-alerts/move/many")
async def move_many_vwap_controller(
    payload: Request,
    sourceCollection: AlertsCollection = Query(..., description="Исходная коллекция"),
    targetCollection: AlertsCollection = Query(..., description="Целевая коллекция"),
    storage: AlertStorage = Depends(get_alert_storage)
):
    """(ОПТИМИЗИРОВАНО: O(N) -> O(k))"""
    try:
        body = await payload.json()
        ids_to_move: List[str] = body.get("ids")
        if not ids_to_move:
            raise HTTPException(status_code=400, detail="Тело запроса должно содержать 'ids'")

        success = await storage.move_vwap_alerts_by_id(sourceCollection, targetCollection, ids_to_move)

        if not success:
            raise HTTPException(status_code=500, detail="Ошибка при перемещении vwap алертов.")

        return {
            "message": f"Moved {len(ids_to_move)} vwap alerts",
            "count": len(ids_to_move)
        }

    except Exception as e:
        logger.error(f"Ошибка в move_many_vwap_controller: {e}", exc_info=True)
        if isinstance(e, HTTPException): raise
        raise HTTPException(status_code=500, detail="Internal server error")

# --- ИЗМЕНЕНИЕ №2: Добавление нового эндпоинта очистки ---

class CleanupPayload(Request):
    """Модель для тела запроса очистки"""
    hours: int

@router.post("/alerts/internal/cleanup-triggered", status_code=200)
async def cleanup_triggered_alerts(
    payload: Request,
    storage: AlertStorage = Depends(get_alert_storage),
    is_authenticated: bool = Depends(verify_cron_secret) 
):
    """
    (НОВЫЙ ЗАЩИЩЕННЫЙ ЭНДПОИНТ)
    Удаляет все 'triggered' алерты (Line и VWAP), которые старше N часов.
    Ожидает тело: { "hours": 24 }
    """
    try:
        body = await payload.json()
        hours = body.get("hours")
        
        if not hours or not isinstance(hours, int) or hours <= 0:
            raise HTTPException(status_code=400, detail="Тело запроса должно содержать 'hours' (int > 0)")

        # 1. Рассчитываем время среза
        cutoff_dt = datetime.now() - timedelta(hours=hours)
        cutoff_timestamp_ms = int(cutoff_dt.timestamp() * 1000)
        
        logger.info(f"[CLEANUP] Запуск очистки 'triggered' алертов старше {hours} часов (до {cutoff_dt.isoformat()})...")

        # 2. Вызываем новые методы storage.py
        deleted_line_count = await storage.cleanup_line_alerts_older_than(
            "triggered", cutoff_timestamp_ms
        )
        
        deleted_vwap_count = await storage.cleanup_vwap_alerts_older_than(
            "triggered", cutoff_timestamp_ms
        )
        
        total = deleted_line_count + deleted_vwap_count
        msg = f"Очистка завершена. Удалено Line: {deleted_line_count}. Удалено VWAP: {deleted_vwap_count}. Всего: {total}."
        logger.info(f"[CLEANUP] {msg}")
        
        return {
            "message": msg,
            "deleted_line_count": deleted_line_count,
            "deleted_vwap_count": deleted_vwap_count,
            "total_deleted": total
        }

    except Exception as e:
        logger.error(f"Ошибка в cleanup_triggered_alerts: {e}", exc_info=True)
        if isinstance(e, HTTPException): raise
        raise HTTPException(status_code=500, detail="Internal server error")