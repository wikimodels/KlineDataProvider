# tests/conftest.py

import pytest
import fakeredis
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, MagicMock
import os
import importlib # <-- 1. Добавляем importlib

# --- 1. Настраиваем fakeredis ---
fake_redis_server = fakeredis.FakeServer()
mock_redis_client = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)

# --- 2. Патчим redis_client ВЕЗДЕ ---
patch('cache_manager.redis_client', mock_redis_client).start()
patch('worker.redis_client', mock_redis_client).start()
patch('api_routes.redis_client', mock_redis_client).start()

# --- 3. Импортируем app (БЕЗ патча os.environ) ---
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
    --- (ИЗМЕНЕНИЕ) Патч SECRET_TOKEN теперь ВНУТРИ этой фикстуры ---
    """
    # Этот патч применяется только во время работы фикстуры test_client,
    # не затрагивая другие тесты (например, integration)
    with patch.dict(os.environ, {"SECRET_TOKEN": "test_secret_for_tests"}):
        
        # --- 2. (ИСПРАВЛЕНИЕ) Принудительно перезагружаем модули ---
        # (Чтобы они "увидели" пропатченный os.environ)
        import config
        import api_routes
        importlib.reload(config)
        importlib.reload(api_routes)
        
        # (Переназначаем зависимости FastAPI)
        app.dependency_overrides = {}
        app.include_router(api_routes.router)
        # --- Конец Исправления ---

        transport = ASGITransport(app=app)
        client = AsyncClient(transport=transport, base_url="http://test")
        yield client
    
    # --- 3. (Очистка) Возвращаем все к исходному состоянию ---
    # (Это нужно, чтобы integration-тесты увидели .env)
    with patch.dict(os.environ):
        if "SECRET_TOKEN" in os.environ:
             del os.environ["SECRET_TOKEN"] # Удаляем мок
        
        # (Перезагружаем модули с реальными .env)
        import config
        import api_routes
        importlib.reload(config)
        importlib.reload(api_routes)
        app.dependency_overrides = {}
        app.include_router(api_routes.router)


@pytest.fixture
def mock_redis():
    """Возвращает наш мок-клиент."""
    return mock_redis_client