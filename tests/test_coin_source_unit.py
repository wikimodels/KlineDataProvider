import pytest
import respx
from httpx import Response
from typing import List, Dict

from data_collector.coin_source import get_coins_from_api
from config import (
    COIN_SIFTER_BASE_URL,
    COIN_SIFTER_ENDPOINT_PATH,
    COIN_PROCESSING_LIMIT,
    COIN_SIFTER_API_TOKEN
)

FULL_API_URL = f"{COIN_SIFTER_BASE_URL.rstrip('/')}/{COIN_SIFTER_ENDPOINT_PATH.lstrip('/')}"

def _generate_mock_coins(count: int) -> List[Dict]:
    return [
        {"symbol": f"COIN{i}USDT", "exchanges": ["binance"]}
        for i in range(count)
    ]

@pytest.mark.asyncio
async def test_get_coins_success_and_limit_applied(respx_mock):
    """
    Тест: API возвращает 200 OK и >100 монет.
    """
    mock_data = {"count": 300, "symbols": _generate_mock_coins(300)}
    
    # --- ИСПРАВЛЕНИЕ: respx_mock.get(...) *НЕ* в 'with' ---
    # Мы настраиваем мок, а 'respx_mock' (фикстура) уже активна.
    respx_mock.get(FULL_API_URL).mock(return_value=Response(200, json=mock_data))
    
    result = await get_coins_from_api()
    # ------------------------------------------------------

    assert result is not None
    assert len(result) == COIN_PROCESSING_LIMIT
    assert result[0]["symbol"] == "COIN0USDT"
    assert result[99]["symbol"] == "COIN99USDT"

@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [401, 403])
async def test_get_coins_api_auth_error(respx_mock, caplog, status_code):
    """
    Тест: API возвращает 401 или 403 (неверный токен).
    """
    # --- ИСПРАВЛЕНИЕ: убираем 'with' ---
    respx_mock.get(FULL_API_URL).mock(return_value=Response(status_code))
    
    result = await get_coins_from_api()
    # ----------------------------------

    assert result is None
    assert f"Ошибка {status_code} (Unauthorized/Forbidden)" in caplog.text

@pytest.mark.asyncio
async def test_get_coins_api_ok_but_missing_key(respx_mock, caplog):
    """
    Тест: API возвращает 200 OK, но в JSON нет ключа 'symbols'.
    """
    mock_data = {"count": 10, "data": _generate_mock_coins(10)}
    
    # --- ИСПРАВЛЕНИЕ: убираем 'with' ---
    respx_mock.get(FULL_API_URL).mock(return_value=Response(200, json=mock_data))
    
    result = await get_coins_from_api()
    # ----------------------------------

    assert result is None
    assert "ключ 'symbols' отсутствует" in caplog.text

@pytest.mark.asyncio
async def test_get_coins_api_sends_auth_header(respx_mock):
    """
    Тест: Проверяем, что наш запрос отправляет 'X-Auth-Token'.
    """
    mock_route = respx_mock.get(FULL_API_URL).mock(
        return_value=Response(200, json={"count": 0, "symbols": []})
    )
    
    await get_coins_from_api()
    
    # Теперь это должно работать, т.к. respx перехватит httpx
    assert mock_route.called
    sent_headers = mock_route.calls[0].request.headers
    
    assert "x-auth-token" in sent_headers
    assert sent_headers["x-auth-token"] == COIN_SIFTER_API_TOKEN