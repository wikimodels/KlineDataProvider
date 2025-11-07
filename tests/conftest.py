# tests/conftest.py

import pytest
import fakeredis
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, MagicMock
import os

# --- 1. Патчим os.environ В ПЕРВУЮ ОЧЕРЕДЬ ---
# Это нужно сделать ДО того, как 'api_routes' будет импортирован
# (даже неявно, через другие патчи)
patch.dict(os.environ, {"SECRET_TOKEN": "test_secret_for_tests"}).start()

# --- 2. Настраиваем fakeredis ---
fake_redis_server = fakeredis.FakeServer()
mock_redis_client = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)

# --- 3. Патчим redis_client ВЕЗДЕ ---
# Теперь, когда 'api_routes' импортируется здесь, он увидит
# уже пропатченный 'SECRET_TOKEN' из os.environ.
patch('cache_manager.redis_client', mock_redis_client).start()
patch('worker.redis_client', mock_redis_client).start()
patch('api_routes.redis_client', mock_redis_client).start()

# --- 4. Импортируем app (теперь можно без 'with') ---
# Он импортирует 'api_routes', который уже был загружен 
# (в шаге 3) с правильным SECRET_TOKEN.
from main import app

# --- 5. Создаем фикстуры ---

@pytest.fixture(autouse=True)
def clear_redis_before_each_test():
    """Очищает redis перед каждым тестом."""
    mock_redis_client.flushall()
    yield
    mock_redis_client.flushall()

@pytest.fixture(scope="module")
def test_client():
    """Создает AsyncClient (TestClient) для отправки запросов к 'app'."""
    transport = ASGITransport(app=app)
    client = AsyncClient(transport=transport, base_url="http://test")
    yield client

@pytest.fixture
def mock_redis():
    """Возвращает наш мок-клиент."""
    return mock_redis_client