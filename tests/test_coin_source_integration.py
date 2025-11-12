import pytest
import os

# Импортируем тестируемую функцию
from data_collector.coin_source import get_coins_from_api
from config import COIN_SIFTER_BASE_URL, COIN_PROCESSING_LIMIT

# Помечаем тест как "slow" и пропускаем его по умолчанию.
# Запуск: pytest -m slow
# Причина: он зависит от внешнего API и .env
@pytest.mark.slow
@pytest.mark.skipif(
    os.environ.get("RUN_SLOW_TESTS") != "1",
    reason="Тест пропущен. (Запускает реальный запрос к API)."
)
@pytest.mark.asyncio
async def test_real_api_call_integration():
    """
    Интеграционный тест: делает РЕАЛЬНЫЙ запрос к API.
    
    Проверяет:
    1. Что .env настроен (URL, TOKEN).
    2. Что API доступно.
    3. Что структура ответа правильная (содержит 'symbols').
    """
    print(f"\n[INFO] Запускаю РЕАЛЬНЫЙ запрос к {COIN_SIFTER_BASE_URL}...")
    
    result = await get_coins_from_api()
    
    # Если тест упал (вернул None), .env или API не в порядке
    assert result is not None, "API вернуло None. Проверьте .env (URL/TOKEN) и доступность API."
    
    # Проверяем, что это список
    assert isinstance(result, list)
    
    # Проверяем, что лимит (100) применился
    assert len(result) <= COIN_PROCESSING_LIMIT
    
    # Проверяем структуру первой монеты (если она есть)
    if len(result) > 0:
        coin = result[0]
        assert "symbol" in coin
        assert "exchanges" in coin
        assert isinstance(coin["symbol"], str)
        assert isinstance(coin["exchanges"], list)
        
    print(f"[INFO] Успех. API вернуло и обработало {len(result)} монет.")