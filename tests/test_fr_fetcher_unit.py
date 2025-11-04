import pytest
from unittest.mock import patch

# Импортируем тестируемую функцию
from data_collector.fr_fetcher import run_fr_update_process


@pytest.mark.asyncio
@patch('data_collector.fr_fetcher.save_to_cache')
@patch('data_collector.fr_fetcher.fetch_funding_rates')
@patch('data_collector.fr_fetcher.get_coins_from_api')
async def test_run_fr_update_success(mock_get_coins, mock_fetch_rates, mock_save_cache, caplog):
    """
    Тест: Успешный запуск. Получили монеты -> Собрали FR -> Сохранили в кэш.
    """
    # 1. Настройка моков
    mock_coins = [{'symbol': 'BTCUSDT', 'exchanges': ['binance']}]
    mock_fr_data = {'BTCUSDT': [{'openTime': 123, 'fundingRate': 0.01}]}

    mock_get_coins.return_value = mock_coins
    mock_fetch_rates.return_value = mock_fr_data

    # 2. Вызов
    await run_fr_update_process()

    # 3. Проверка
    mock_get_coins.assert_called_once()
    mock_fetch_rates.assert_called_once_with(mock_coins)
    mock_save_cache.assert_called_once_with('global_fr', mock_fr_data)
    assert "[CRON_JOB] Успех!" in caplog.text


@pytest.mark.asyncio
@patch('data_collector.fr_fetcher.save_to_cache')
@patch('data_collector.fr_fetcher.fetch_funding_rates')
@patch('data_collector.fr_fetcher.get_coins_from_api')
async def test_run_fr_update_no_coins(mock_get_coins, mock_fetch_rates, mock_save_cache, caplog):
    """
    Тест: API монет не вернуло данные (None или []).
    """
    # 1. Настройка моков
    mock_get_coins.return_value = []  # или None — оба случая обрабатываются

    # 2. Вызов
    await run_fr_update_process()

    # 3. Проверка
    mock_get_coins.assert_called_once()
    mock_fetch_rates.assert_not_called()
    mock_save_cache.assert_not_called()
    assert "Не удалось получить монеты из API" in caplog.text


@pytest.mark.asyncio
@patch('data_collector.fr_fetcher.save_to_cache')
@patch('data_collector.fr_fetcher.fetch_funding_rates')
@patch('data_collector.fr_fetcher.get_coins_from_api')
async def test_run_fr_update_fetch_failed(mock_get_coins, mock_fetch_rates, mock_save_cache, caplog):
    """
    Тест: Сборщик FR вернул None (критическая ошибка).
    """
    # 1. Настройка моков
    mock_coins = [{'symbol': 'BTCUSDT', 'exchanges': ['binance']}]
    mock_get_coins.return_value = mock_coins
    mock_fetch_rates.return_value = None  # Имитация критической ошибки

    # 2. Вызов
    await run_fr_update_process()

    # 3. Проверка
    mock_get_coins.assert_called_once()
    mock_fetch_rates.assert_called_once_with(mock_coins)
    mock_save_cache.assert_not_called()
    assert "Сбор FR вернул None" in caplog.text


@pytest.mark.asyncio
@patch('data_collector.fr_fetcher.save_to_cache')
@patch('data_collector.fr_fetcher.fetch_funding_rates')
@patch('data_collector.fr_fetcher.get_coins_from_api')
async def test_run_fr_update_fetch_empty(mock_get_coins, mock_fetch_rates, mock_save_cache, caplog):
    """
    Тест: Сборщик FR вернул пустой словарь {}.
    """
    # 1. Настройка моков
    mock_coins = [{'symbol': 'BTCUSDT', 'exchanges': ['binance']}]
    mock_get_coins.return_value = mock_coins
    mock_fetch_rates.return_value = {}  # Пустой результат

    # 2. Вызов
    await run_fr_update_process()

    # 3. Проверка
    mock_get_coins.assert_called_once()
    mock_fetch_rates.assert_called_once_with(mock_coins)
    mock_save_cache.assert_not_called()
    assert "Сбор FR вернул пустой словарь" in caplog.text