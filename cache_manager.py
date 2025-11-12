import logging
import json
import gzip  # <-- ИЗМЕНЕНИЕ №1 (Уже было)
from datetime import datetime
from typing import Dict, Any, Optional
from redis.asyncio import Redis as AsyncRedis
from urllib.parse import urlparse

# Импортируем ВСЁ из config.py
from config import (
    UPSTASH_REDIS_URL,
    UPSTASH_REDIS_TOKEN,
    REDIS_TASK_QUEUE_KEY,
    WORKER_LOCK_KEY,
    WORKER_LOCK_VALUE
)

logger = logging.getLogger(__name__)
_redis_pool: Optional[AsyncRedis] = None


async def get_redis_connection() -> Optional[AsyncRedis]:
    """Подключение к Redis - используем URL + TOKEN отдельно"""
    global _redis_pool
    
    if _redis_pool is None:
        try:
            # Парсим URL чтобы достать хост и порт
            parsed = urlparse(UPSTASH_REDIS_URL)
            
            _redis_pool = AsyncRedis(
                host=parsed.hostname,
                port=parsed.port or 6379,
                password=UPSTASH_REDIS_TOKEN,  # <-- Токен отдельно!
                ssl=True,  # Upstash всегда требует SSL
                decode_responses=False,
                socket_connect_timeout=30,
                socket_timeout=30,                 
                socket_keepalive=True
            )
            await _redis_pool.ping()
            logger.info("✅ Подключено к Redis")
            return _redis_pool
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Redis: {e}")
            _redis_pool = None
            return None
    
    return _redis_pool


async def check_redis_health() -> bool:
    """Проверяет доступность Redis."""
    conn = await get_redis_connection()
    if conn:
        try:
            return await conn.ping()
        except Exception:
            return False
    return False


async def load_from_cache(key: str, redis_conn: AsyncRedis) -> Optional[Dict[str, Any]]:
    """Загружает данные из Redis по ключу. Декодирует JSON."""
    cache_key = f"cache:{key}"
    data_bytes = await redis_conn.get(cache_key)
    
    if data_bytes:
        try:
            # --- ИЗМЕНЕНИЕ №3: Сначала распаковываем --- (Уже было)
            data_bytes = gzip.decompress(data_bytes)
            # ----------------------------------------
            data_str = data_bytes.decode('utf-8')
            return json.loads(data_str)
        except (IOError, gzip.BadGzipFile, json.JSONDecodeError, UnicodeDecodeError) as e:
            # --- ИЗМЕНЕНИЕ №3: Обработка, если данные не сжаты (старый кэш) --- (Уже было)
            logger.warning(f"[CACHE] Не удалось распаковать gzip для {cache_key} (возможно, старый кэш? Ошибка: {e}). Попытка прочитать как обычный JSON...")
            try:
                # Попытка прочитать как обычный (не сжатый) JSON
                data_str = data_bytes.decode('utf-8')
                return json.loads(data_str)
            except Exception as e_inner:
                logger.error(f"[CACHE] Ошибка десериализации ключа {cache_key} (даже как fallback): {e_inner}")
                return None
            # -----------------------------------------------------------------
    return None


async def save_to_cache(redis_conn: AsyncRedis, key: str, data: Dict[str, Any], expiry_seconds: Optional[int] = None) -> bool:
    """Сохраняет данные в Redis."""
    cache_key = f"cache:{key}"
    
    if 'audit' not in data:
        # --- ИЗМЕНЕНИЕ №1: Корректный подсчет count ---
        data_content = data.get('data')
        count = 0
        if isinstance(data_content, (list, dict)):
            count = len(data_content)
        # --- КОНЕЦ ИЗМЕНЕНИЯ №1 ---
            
        data['audit'] = {
            "timestamp": int(datetime.now().timestamp() * 1000),
            "source": "data_collector",
            # --- ИЗМЕНЕНИЕ №1: Используем новую переменную ---
            "count": count
        }
        
    try:
        data_json = json.dumps(data)
        data_bytes = data_json.encode('utf-8')
        
        # --- ИЗМЕНЕНИЕ №2: Сжимаем данные перед отправкой --- (Уже было)
        compressed_data = gzip.compress(data_bytes)
        # -------------------------------------------------
        
        if expiry_seconds:
            result = await redis_conn.set(cache_key, compressed_data, ex=expiry_seconds)
        else:
            result = await redis_conn.set(cache_key, compressed_data)
            
        # --- ИЗМЕНЕНИЕ №1: 'count' теперь будет корректным ---
        logger.info(f"[CACHE] Успешно сохранено {data['audit']['count']} записей в {cache_key} (Сжато: {len(data_bytes)} -> {len(compressed_data)} байт).")
        return result
    except Exception as e:
        logger.error(f"[CACHE] Ошибка при сохранении ключа {cache_key} в Redis: {e}", exc_info=True)
        return False


async def clear_queue(redis_conn: AsyncRedis, queue_key: str):
    """Очищает очередь задач."""
    await redis_conn.delete(queue_key)
    
    
async def get_worker_status(redis_conn: AsyncRedis) -> Optional[bytes]:
    """Возвращает статус блокировки воркера (bytes или None)."""
    return await redis_conn.get(WORKER_LOCK_KEY)


async def check_if_task_is_running(timeframe: str, redis_conn: AsyncRedis) -> bool:
    """Проверяет, выполняется ли задача для данного timeframe."""
    lock_status = await get_worker_status(redis_conn)
    return lock_status and lock_status.decode('utf-8') == WORKER_LOCK_VALUE


async def add_task_to_queue(timeframe: str, redis_conn: AsyncRedis) -> bool:
    """Добавляет задачу с заданным timeframe в очередь Redis."""
    task_payload = {"timeframe": timeframe}
    task_json = json.dumps(task_payload)
    
    try:
        await redis_conn.rpush(REDIS_TASK_QUEUE_KEY, task_json.encode('utf-8'))
        return True
    except Exception as e:
        logger.error(f"[QUEUE] Не удалось добавить задачу '{timeframe}' в очередь: {e}", exc_info=True)
        return False