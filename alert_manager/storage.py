"""
Этот модуль заменяет MongoDB операторы (LineAlertOperator, VwapAlertOperator)
используя Redis JSON для хранения и управления алертами.

(АСИНХРОННАЯ ВЕРСИЯ)

--- ИЗМЕНЕНИЕ (ОПТИМИЗАЦИЯ) ---
Старая версия хранила все алерты в ОДНОМ JSON-массиве, что приводило
к O(N) операциям при обновлении/удалении (чтение всего массива, 
фильтрация в Python, перезапись всего массива).

Новая версия использует гибридный подход для O(1) операций:
1.  Каждый алерт хранится в своем JSON-ключе (например, "alert:line:<uuid>").
2.  Коллекции ("working", "triggered") - это Redis Sets (например, "index:line:working"),
    хранящие <uuid> алертов.
"""
import logging
import json
from typing import List, Dict, Any, Optional, Literal

from redis.asyncio import Redis as AsyncRedis

# --- ИЗМЕНЕНИЕ №1: Замена относительного импорта на АБСОЛЮТНЫЙ ---
from alert_manager.model import (
    Alert, VwapAlert, AlertsCollection
)
# --- КОНЕЦ ИЗМЕНЕНИЯ №1 ---

logger = logging.getLogger(__name__)

# --- Префиксы для Ключей ---
IDX_LINE = "index:line"
IDX_VWAP = "index:vwap"
DATA_LINE = "alert:line"
DATA_VWAP = "alert:vwap"


class AlertStorage:
    """
    Класс для управления хранилищем алертов (Line и VWAP) в Redis.
    (АСИНХРОННАЯ ВЕРСИЯ - O(1) ОПТИМИЗИРОВАННАЯ)
    """
    def __init__(self, redis_conn: AsyncRedis):
        if not redis_conn:
            raise RuntimeError("Не удалось инициализировать AlertStorage: 'redis_conn' не предоставлен.")
        self.redis = redis_conn
        
    def _get_index_key(self, collection_name: AlertsCollection, alert_type: Literal["line", "vwap"]) -> str:
        """Получает ключ ИНДЕКСА (Set)"""
        if alert_type == "line":
            return f"{IDX_LINE}:{collection_name}"
        elif alert_type == "vwap":
            return f"{IDX_VWAP}:{collection_name}"
        raise ValueError(f"Неизвестный тип алерта: {alert_type}")

    def _get_data_key(self, alert_id: str, alert_type: Literal["line", "vwap"]) -> str:
        """Получает ключ ДАННЫХ (JSON)"""
        if alert_type == "line":
            return f"{DATA_LINE}:{alert_id}"
        elif alert_type == "vwap":
            return f"{DATA_VWAP}:{alert_id}"
        raise ValueError(f"Неизвестный тип алерта: {alert_type}")


    # --- Общие Хелперы (Новые) ---

    async def _add_alert_internal(
        self, 
        collection_name: AlertsCollection, 
        alert: Dict[str, Any], 
        alert_type: Literal["line", "vwap"]
    ) -> bool:
        """
        (Новая внутренняя функция)
        Атомарно добавляет алерт:
        1. Сохраняет JSON данные
        2. Добавляет ID в Set индекса
        """
        alert_id = alert.get("id")
        if not alert_id:
            logger.error(f"Не удалось добавить {alert_type} алерт: отсутствует 'id'")
            return False
            
        index_key = self._get_index_key(collection_name, alert_type)
        data_key = self._get_data_key(alert_id, alert_type)
        
        try:
            # Используем пайплайн для атомарности
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.json().set(data_key, "$", alert)
                pipe.sadd(index_key, alert_id)
                await pipe.execute()
            return True
        except Exception as e:
            logger.error(f"Не удалось добавить {alert_type} алерт ({alert_id}) в {index_key}: {e}", exc_info=True)
            return False

    async def _get_alerts_internal(
        self, 
        collection_name: AlertsCollection, 
        alert_type: Literal["line", "vwap"]
    ) -> List[Dict[str, Any]]:
        """
        (Новая внутренняя функция)
        1. Получает все ID из Set индекса
        2. Получает все JSON-данные для этих ID (через MGET)
        """
        index_key = self._get_index_key(collection_name, alert_type)
        
        try:
            alert_ids = await self.redis.smembers(index_key)
            if not alert_ids:
                return []
            
            alert_ids_str = [aid.decode('utf-8') for aid in alert_ids]
            data_keys = [self._get_data_key(aid, alert_type) for aid in alert_ids_str]
            results_json = await self.redis.json().mget(data_keys, "$")
            
            alerts = []
            for res in results_json:
                if res and isinstance(res, list) and len(res) > 0:
                    alerts.append(res[0])
            
            return alerts
            
        except Exception as e:
            logger.error(f"Не удалось получить {alert_type} алерты из {index_key}: {e}", exc_info=True)
            return []

    # --- Line Alerts (Публичные методы) ---

    async def get_alerts(self, collection_name: AlertsCollection) -> List[Alert]:
        """
        Получает *все* Line Alerts из указанной коллекции (O(N), но оптимизировано).
        """
        return await self._get_alerts_internal(collection_name, "line") # type: ignore

    async def add_alert(self, collection_name: AlertsCollection, alert: Alert) -> bool:
        """
        Добавляет один Line Alert в коллекцию (O(1)).
        """
        return await self._add_alert_internal(collection_name, alert, "line")

    async def update_alert_by_id(self, alert_id: str, update_data: Dict[str, Any]) -> bool:
        """
        (Новый метод) Обновляет один Line Alert по ID (O(1)).
        """
        data_key = self._get_data_key(alert_id, "line")
        try:
            await self.redis.json().set(data_key, "$", update_data)
            return True
        except Exception as e:
            logger.error(f"Не удалось обновить Line Alert ({alert_id}): {e}", exc_info=True)
            return False

    async def delete_alerts_by_id(self, collection_name: AlertsCollection, ids_to_delete: List[str]) -> bool:
        """
        (Новый метод) Удаляет несколько Line Alerts по ID (O(N) в Redis, но атомарно).
        """
        if not ids_to_delete:
            return True
        
        index_key = self._get_index_key(collection_name, "line")
        data_keys = [self._get_data_key(aid, "line") for aid in ids_to_delete]
        
        try:
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.srem(index_key, *ids_to_delete) # Удаляем из индекса
                pipe.delete(*data_keys) # Удаляем сами данные
                await pipe.execute()
            return True
        except Exception as e:
            logger.error(f"Не удалось удалить Line Alerts из {index_key}: {e}", exc_info=True)
            return False

    async def move_alerts_by_id(
        self, 
        source_collection: AlertsCollection, 
        target_collection: AlertsCollection, 
        ids_to_move: List[str]
    ) -> bool:
        """
        (Новый метод) Перемещает Line Alerts между коллекциями (O(N) в Redis).
        """
        if not ids_to_move:
            return True
            
        source_key = self._get_index_key(source_collection, "line")
        target_key = self._get_index_key(target_collection, "line")
        
        try:
            async with self.redis.pipeline(transaction=True) as pipe:
                for alert_id in ids_to_move:
                    pipe.smove(source_key, target_key, alert_id)
                await pipe.execute()
            return True
        except Exception as e:
            logger.error(f"Не удалось переместить Line Alerts из {source_key} в {target_key}: {e}", exc_info=True)
            return False


    # --- VWAP Alerts (Публичные методы) ---

    async def get_vwap_alerts(self, collection_name: AlertsCollection) -> List[VwapAlert]:
        """
        Получает *все* VWAP Alerts из указанной коллекции (O(N), оптимизировано).
        """
        return await self._get_alerts_internal(collection_name, "vwap") # type: ignore

    async def add_vwap_alert(self, collection_name: AlertsCollection, alert: VwapAlert) -> bool:
        """
        Добавляет один VWAP Alert в коллекцию (O(1)).
        """
        return await self._add_alert_internal(collection_name, alert, "vwap")

    async def update_vwap_alert_by_id(self, alert_id: str, update_data: Dict[str, Any]) -> bool:
        """
        (Новый метод) Обновляет один VWAP Alert по ID (O(1)).
        """
        data_key = self._get_data_key(alert_id, "vwap")
        try:
            await self.redis.json().set(data_key, "$", update_data)
            return True
        except Exception as e:
            logger.error(f"Не удалось обновить VWAP Alert ({alert_id}): {e}", exc_info=True)
            return False

    async def delete_vwap_alerts_by_id(self, collection_name: AlertsCollection, ids_to_delete: List[str]) -> bool:
        """
        (Новый метод) Удаляет несколько VWAP Alerts по ID (O(N) в Redis, атомарно).
        """
        if not ids_to_delete:
            return True
        
        index_key = self._get_index_key(collection_name, "vwap")
        data_keys = [self._get_data_key(aid, "vwap") for aid in ids_to_delete]
        
        try:
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.srem(index_key, *ids_to_delete)
                pipe.delete(*data_keys)
                await pipe.execute()
            return True
        except Exception as e:
            logger.error(f"Не удалось удалить VWAP Alerts из {index_key}: {e}", exc_info=True)
            return False

    async def move_vwap_alerts_by_id(
        self, 
        source_collection: AlertsCollection, 
        target_collection: AlertsCollection, 
        ids_to_move: List[str]
    ) -> bool:
        """
        (Новый метод) Перемещает VWAP Alerts между коллекциями (O(N) в Redis).
        """
        if not ids_to_move:
            return True
            
        source_key = self._get_index_key(source_collection, "vwap")
        target_key = self._get_index_key(target_collection, "vwap")
        
        try:
            async with self.redis.pipeline(transaction=True) as pipe:
                for alert_id in ids_to_move:
                    pipe.smove(source_key, target_key, alert_id)
                await pipe.execute()
            return True
        except Exception as e:
            logger.error(f"Не удалось переместить VWAP Alerts из {source_key} в {target_key}: {e}", exc_info=True)
            return False

    
    async def _get_alerts_to_cleanup_internal(
        self, 
        collection_name: AlertsCollection, 
        alert_type: Literal["line", "vwap"],
        cutoff_timestamp_ms: int
    ) -> List[str]:
        """
        (Новый хелпер)
        1. Получает все алерты
        2. Фильтрует по 'activationTime'
        3. Возвращает список ID для удаления
        """
        try:
            all_alerts = await self._get_alerts_internal(collection_name, alert_type)
            
            ids_to_delete: List[str] = []
            
            for alert in all_alerts:
                alert_id = alert.get("id")
                activation_time = alert.get("activationTime", 0) 
                
                if not alert_id:
                    continue
                    
                if activation_time < cutoff_timestamp_ms:
                    ids_to_delete.append(alert_id)
                    
            return ids_to_delete
            
        except Exception as e:
            logger.error(f"Ошибка при поиске устаревших алертов ({alert_type}): {e}", exc_info=True)
            return []

    async def cleanup_line_alerts_older_than(self, collection_name: AlertsCollection, cutoff_timestamp_ms: int) -> int:
        """
        (Новый метод)
        Удаляет Line Alerts из коллекции, которые старше cutoff_timestamp_ms.
        Возвращает количество удаленных.
        """
        ids_to_delete = await self._get_alerts_to_cleanup_internal("triggered", "line", cutoff_timestamp_ms)
        if not ids_to_delete:
            return 0
            
        await self.delete_alerts_by_id(collection_name, ids_to_delete)
        return len(ids_to_delete)

    async def cleanup_vwap_alerts_older_than(self, collection_name: AlertsCollection, cutoff_timestamp_ms: int) -> int:
        """
        (Новый метод)
        Удаляет VWAP Alerts из коллекции, которые старше cutoff_timestamp_ms.
        Возвращает количество удаленных.
        """
        ids_to_delete = await self._get_alerts_to_cleanup_internal("triggered", "vwap", cutoff_timestamp_ms)
        if not ids_to_delete:
            return 0
            
        await self.delete_vwap_alerts_by_id(collection_name, ids_to_delete)
        return len(ids_to_delete)