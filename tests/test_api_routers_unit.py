import pytest
from httpx import AsyncClient
from unittest.mock import patch, AsyncMock, MagicMock
import json
import os
import base64
import zlib 
import redis # Для класса ошибки ConnectionError

# 1. Импортируем FakeRedis
from fakeredis import FakeRedis

# 2. Импортируем константы
from config import REDIS_TASK_QUEUE_KEY, WORKER_LOCK_KEY
# (SECRET_TOKEN будет 'test_secret_for_tests' из conftest.py)

# (НЕ импортируем 'app' отсюда)


@pytest.mark.asyncio
async def test_schedule_market_data_job_success(test_client: AsyncClient, mock_redis: FakeRedis):
    """Тест: Успешный POST /get-market-data (202 Accepted)."""
    timeframe = "1h"
    mock_redis.delete(WORKER_LOCK_KEY)
    initial_queue_len = mock_redis.llen(REDIS_TASK_QUEUE_KEY)
    
    response = await test_client.post("/get-market-data", json={"timeframe": timeframe})

    assert response.status_code == 202
    assert response.json() == {"message": f"Задача для {timeframe} принята в очередь."}
    
    final_queue_len = mock_redis.llen(REDIS_TASK_QUEUE_KEY)
    assert final_queue_len == initial_queue_len + 1
    added_task = mock_redis.lindex(REDIS_TASK_QUEUE_KEY, -1)
    assert added_task == timeframe


@pytest.mark.asyncio
async def test_schedule_market_data_job_invalid_timeframe_post(test_client: AsyncClient):
    """Тест: schedule_market_data_job с неверным таймфреймом для POST."""
    response = await test_client.post("/get-market-data", json={"timeframe": "8h"})
    assert response.status_code == 400
    assert "не может быть запрошен для сбора" in response.json()["detail"]


@pytest.mark.asyncio
async def test_schedule_market_data_job_with_lock(test_client: AsyncClient, mock_redis: FakeRedis):
    """Тест: /get-market-data (Klines) отклонен (409), если воркер занят."""
    timeframe = "1h"
    mock_redis.set(WORKER_LOCK_KEY, "BUSY_PROCESSING")

    response = await test_client.post("/get-market-data", json={"timeframe": timeframe})

    assert response.status_code == 409
    assert "Конфликт: Сборщик уже занят" in response.json()["detail"]
    assert mock_redis.llen(REDIS_TASK_QUEUE_KEY) == 0


@pytest.mark.asyncio
async def test_get_cached_data_success(test_client: AsyncClient, mock_redis: FakeRedis):
    """Тест: get_cached_data с верным таймфреймом и существующим кэшем."""
    timeframe = "1h"
    cache_key = f"cache:{timeframe}"
    cached_data = {"BTC/USDT": [{"time": 123, "value": 456}]}
    
    json_str = json.dumps(cached_data)
    compressed_data = zlib.compress(json_str.encode('utf-8'))
    base64_compressed = base64.b64encode(compressed_data).decode('utf-8')
    mock_redis.set(cache_key, base64_compressed)

    with patch('api_routes.make_serializable', side_effect=lambda x: x):
        response = await test_client.get(f"/cache/{timeframe}")

    assert response.status_code == 200
    assert response.json() == cached_data


@pytest.mark.asyncio
async def test_get_cached_data_invalid_timeframe_get(test_client: AsyncClient):
    """Тест: get_cached_data с неверным ключом."""
    response = await test_client.get("/cache/invalid_key")
    assert response.status_code == 400
    assert "недопустим для чтения из кэша" in response.json()["detail"]


@pytest.mark.asyncio
async def test_get_cached_data_not_found(test_client: AsyncClient, mock_redis: FakeRedis):
    """Тест: get_cached_data, когда кэш пуст."""
    timeframe = "1h"
    cache_key = f"cache:{timeframe}"
    mock_redis.delete(cache_key)

    response = await test_client.get(f"/cache/{timeframe}")

    assert response.status_code == 404
    assert "пуст" in response.json()["detail"]

# ---
# --- ИЗМЕНЕННЫЕ ТЕСТЫ ДЛЯ /update-fr ---
# ---

@pytest.mark.asyncio
async def test_trigger_fr_update_success(test_client: AsyncClient, mock_redis: FakeRedis):
    """
    Тест: Успешный POST /api/v1/internal/update-fr (202 Accepted).
    Проверяет, что задача 'fr' добавлена в очередь Redis.
    """
    # 1. Убедимся, что воркер свободен и очередь пуста
    mock_redis.delete(WORKER_LOCK_KEY)
    initial_queue_len = mock_redis.llen(REDIS_TASK_QUEUE_KEY)
    
    # 2. Используем токен, заданный в conftest.py
    test_token = "test_secret_for_tests"
    headers = {"Authorization": f"Bearer {test_token}"}
    
    response = await test_client.post(
        "/api/v1/internal/update-fr",
        headers=headers
    )

    # 3. Проверяем, что задача принята
    assert response.status_code == 202
    assert "Задача для fr принята в очередь" in response.json()["message"]
    
    # 4. Проверяем, что в очередь добавлена строка "fr"
    final_queue_len = mock_redis.llen(REDIS_TASK_QUEUE_KEY)
    assert final_queue_len == initial_queue_len + 1
    added_task = mock_redis.lindex(REDIS_TASK_QUEUE_KEY, -1)
    assert added_task == "fr"


@pytest.mark.asyncio
async def test_trigger_fr_update_conflict_lock(test_client: AsyncClient, mock_redis: FakeRedis):
    """
    Тест: /update-fr (FR) отклонен (409), если воркер занят.
    (Проверяет, что ОБА эндпоинта используют один и тот же лок)
    """
    # 1. "Занимаем" воркер (например, задачей Klines)
    mock_redis.set(WORKER_LOCK_KEY, "BUSY_PROCESSING_KLINES")
    
    # 2. Используем правильный токен
    test_token = "test_secret_for_tests"
    headers = {"Authorization": f"Bearer {test_token}"}

    response = await test_client.post(
        "/api/v1/internal/update-fr",
        headers=headers
    )

    # 3. Проверяем, что воркер ответил 409
    assert response.status_code == 409
    assert "Конфликт: Сборщик уже занят" in response.json()["detail"]
    assert mock_redis.llen(REDIS_TASK_QUEUE_KEY) == 0


@pytest.mark.asyncio
async def test_trigger_fr_update_invalid_secret(test_client: AsyncClient):
    """Тест: trigger_fr_update с неверной аутентификацией."""
    response = await test_client.post(
        "/api/v1/internal/update-fr",
        headers={"Authorization": "Bearer wrong_secret"}
    )
    assert response.status_code == 403
    assert "Неверный токен" in response.json()["detail"]


@pytest.mark.asyncio
async def test_trigger_fr_update_missing_auth_header(test_client: AsyncClient):
    """Тест: trigger_fr_update без заголовка аутентификации."""
    response = await test_client.post("/api/v1/internal/update-fr")
    assert response.status_code == 403

# ---
# --- (Остальные тесты без изменений) ---
# ---

@pytest.mark.asyncio
async def test_queue_status_success(test_client: AsyncClient, mock_redis: FakeRedis):
    """Тест: queue_status, когда Redis доступен и в очереди есть элементы."""
    mock_redis.rpush(REDIS_TASK_QUEUE_KEY, "1h", "4h", "12h")
    
    response = await test_client.get("/queue-status")

    assert response.status_code == 200
    assert response.json() == {"tasks_in_queue": 3}


@pytest.mark.asyncio
async def test_queue_status_empty(test_client: AsyncClient, mock_redis: FakeRedis):
    """Тест: queue_status, когда очередь пуста."""
    mock_redis.delete(REDIS_TASK_QUEUE_KEY)
    
    response = await test_client.get("/queue-status")

    assert response.status_code == 200
    assert response.json() == {"tasks_in_queue": 0}


@pytest.mark.asyncio
async def test_health_check(test_client: AsyncClient):
    """Тест: эндпоинт health check."""
    response = await test_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_schedule_market_data_job_valid_timeframes(test_client: AsyncClient, mock_redis: FakeRedis):
    """Тест: schedule_market_data_job со всеми верными POST таймфреймами."""
    valid_timeframes = ["1h", "4h", "12h", "1d"]
    
    for tf in valid_timeframes:
        mock_redis.delete(WORKER_LOCK_KEY)
        initial_queue_len = mock_redis.llen(REDIS_TASK_QUEUE_KEY)
        
        response = await test_client.post("/get-market-data", json={"timeframe": tf})
        
        assert response.status_code == 202, f"Failed for timeframe {tf}: {response.text}"
        final_queue_len = mock_redis.llen(REDIS_TASK_QUEUE_KEY)
        assert final_queue_len == initial_queue_len + 1, f"Queue not updated for {tf}"


@pytest.mark.asyncio
async def test_get_cached_data_valid_timeframes(test_client: AsyncClient, mock_redis: FakeRedis):
    """Тест: get_cached_data со всеми верными GET таймфреймами."""
    valid_timeframes = ["1h", "4h", "8h", "12h", "1d", "global_fr"]
    
    for tf in valid_timeframes:
        cache_key = f"cache:{tf}"
        cached_data = {f"SYMBOL_{tf}": [{"time": 100, "value": 200}]}
        
        json_str = json.dumps(cached_data)
        compressed_data = zlib.compress(json_str.encode('utf-8'))
        base64_compressed = base64.b64encode(compressed_data).decode('utf-8')
        mock_redis.set(cache_key, base64_compressed)

        with patch('api_routes.make_serializable', side_effect=lambda x: x):
            response = await test_client.get(f"/cache/{tf}")

        assert response.status_code == 200, f"Failed for timeframe {tf}: {response.text}"
        assert response.json() == cached_data
        mock_redis.delete(cache_key)

@pytest.mark.asyncio
async def test_post_get_market_data_redis_rpush_fails(test_client: AsyncClient, mock_redis: FakeRedis):
    """
    Тест: Что, если Redis падает в момент добавления задачи в очередь (rpush)?
    """
    mock_redis.delete(WORKER_LOCK_KEY)
    
    with patch.object(mock_redis, 'rpush', side_effect=redis.exceptions.ConnectionError("Redis down!")):
        
        response = await test_client.post("/get-market-data", json={"timeframe": "1h"})
    
    assert response.status_code == 500
    assert "Внутренняя ошибка сервера" in response.json()["detail"]
    assert mock_redis.llen(REDIS_TASK_QUEUE_KEY) == 0