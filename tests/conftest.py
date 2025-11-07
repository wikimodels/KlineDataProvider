# tests/conftest.py

import pytest
import fakeredis
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, MagicMock
import os

# --- 1. Настраиваем fakeredis ---
# (Патч os.environ УБРАН ОТСЮДА)
fake_redis_server = fakeredis.FakeServer()
mock_redis_client = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)

# --- 2. Патчим redis_client ВЕЗДЕ ---
patch('cache_manager.redis_client', mock_redis_client).start()
patch('worker.redis_client', mock_redis_client).start()
patch('api_routes.redis_client', mock_redis_client).start()

# --- 3. Импортируем app (БЕЗ патча os.environ) ---
# (Он импортирует 'api_routes', который будет использовать
# настоящий .env SECRET_TOKEN, если он есть, или None,
# но это будет исправлено для test_client в фикстуре ниже)
from main import app

# --- 4. Создаем фикстуры ---

@pytest.fixture(autouse=True)
def clear_redis_before_each_test():
    """Очищает redis перед каждым тестом."""
    mock_redis_client.flushall()
    yield
    mock_redis_client.flushall()

@pytest.fixture(scope="module")
def test_client():
    """
    Создает AsyncClient (TestClient) для отправки запросов к 'app'.
    --- ИЗМЕНЕНИЕ: Патч SECRET_TOKEN теперь ВНУТРИ этой фикстуры ---
    """
    # Этот патч применяется только во время работы фикстуры test_client,
    # не затрагивая другие тесты (например, integration)
    with patch.dict(os.environ, {"SECRET_TOKEN": "test_secret_for_tests"}):
        
        # Перезагружаем 'api_routes', чтобы он увидел
        # пропатченный 'test_secret_for_tests'
        from config import SECRET_TOKEN as patched_token
        from api_routes import router
        
        # Убедимся, что модуль 'api_routes' действительно 
        # использует мок-токен
        router.dependencies.clear() # Очищаем старые зависимости
        
        transport = ASGITransport(app=app)
        client = AsyncClient(transport=transport, base_url="http://test")
        yield client
    
    # После завершения теста, os.environ вернется 
    # к своему исходному состоянию (с реальным токеном из .env)

@pytest.fixture
def mock_redis():
    """Возвращает наш мок-клиент."""
    return mock_redis_client