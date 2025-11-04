import pytest
from unittest.mock import patch, MagicMock, AsyncMock

# Импортируем *только* те функции, которые мы хотим тестировать
from worker import _process_single_timeframe_task, _process_4h_and_8h_task

# --- Мокируем зависимости на уровне модуля ---

@pytest.fixture(autouse=True)
def mock_dependencies():
    """
    Этот фикстур мокирует все внешние зависимости,
    которые импортируются и используются в worker.py
    """
    # Мокируем функции, которые 'worker.py' импортирует
    with patch('worker.get_coins_from_api', AsyncMock()) as mock_get_coins, \
         patch('worker.get_from_cache', MagicMock()) as mock_get_cache, \
         patch('worker.save_to_cache', MagicMock()) as mock_save_cache, \
         patch('worker.data_collector.fetch_market_data', AsyncMock()) as mock_fetch_data, \
         patch('worker.generate_and_save_8h_cache', AsyncMock()) as mock_gen_8h:
        
        # Передаем моки в 'yield', чтобы они были доступны в тестах
        yield {
            "get_coins": mock_get_coins,
            "get_cache": mock_get_cache,
            "save_cache": mock_save_cache,
            "fetch_data": mock_fetch_data,
            "gen_8h": mock_gen_8h
        }

# --- Тесты для _process_single_timeframe_task ---

@pytest.mark.asyncio
async def test_single_task_success(mock_dependencies):
    """
    Тест: Успешный запуск _process_single_timeframe_task (например, '1h').
    """
    # 1. Настройка моков
    mock_coins = [{'symbol': 'BTCUSDT', 'exchanges': ['binance']}]
    mock_market_data = {'data': [{'symbol': 'BTCUSDT', 'data': [1,2,3]}]}
    
    mock_dependencies["get_coins"].return_value = mock_coins
    mock_dependencies["get_cache"].return_value = {} # 'global_fr'
    mock_dependencies["fetch_data"].return_value = mock_market_data
    
    # 2. Вызов
    await _process_single_timeframe_task('1h', {})
    
    # 3. Проверка
    mock_dependencies["get_coins"].assert_called_once()
    mock_dependencies["fetch_data"].assert_called_once_with(
        mock_coins, '1h', prefetched_fr_data={}
    )
    # Убедимся, что мы сохранили НЕобогащенные данные
    mock_dependencies["save_cache"].assert_called_once_with('1h', mock_market_data)

@pytest.mark.asyncio
async def test_single_task_no_coins(mock_dependencies, caplog):
    """
    Тест: _process_single_timeframe_task не получает монет из API.
    """
    # 1. Настройка моков
    mock_dependencies["get_coins"].return_value = [] # Нет монет
    
    # 2. Вызов
    await _process_single_timeframe_task('1h', {})
    
    # 3. Проверка
    mock_dependencies["get_coins"].assert_called_once()
    # Сбор и сохранение не должны вызываться
    mock_dependencies["fetch_data"].assert_not_called()
    mock_dependencies["save_cache"].assert_not_called()
    assert "Из API не получено ни одной монеты" in caplog.text

# --- Тесты для _process_4h_and_8h_task ---

@pytest.mark.asyncio
async def test_4h_8h_task_success(mock_dependencies):
    """
    Тест: Успешный запуск _process_4h_and_8h_task.
    Проверяем, что он сохраняет cache:4h и вызывает генератор 8h.
    """
    # 1. Настройка моков
    mock_coins = [{'symbol': 'BTCUSDT', 'exchanges': ['binance']}]
    mock_master_data = {'data': [{'symbol': 'BTCUSDT', 'data': [1,2,3]}]} # Данные 4h
    
    mock_dependencies["get_coins"].return_value = mock_coins
    mock_dependencies["get_cache"].return_value = {} # 'global_fr'
    mock_dependencies["fetch_data"].return_value = mock_master_data
    
    # 2. Вызов
    await _process_4h_and_8h_task({})
    
    # 3. Проверка
    # Монеты получены 1 раз
    mock_dependencies["get_coins"].assert_called_once()
    # Данные 4h получены 1 раз
    mock_dependencies["fetch_data"].assert_called_once_with(
        mock_coins, '4h', prefetched_fr_data={}
    )
    # cache:4h сохранен (с НЕобогащенными данными)
    mock_dependencies["save_cache"].assert_called_once_with('4h', mock_master_data)
    # Генератор 8h вызван (с НЕобогащенными данными)
    mock_dependencies["gen_8h"].assert_called_once_with(mock_master_data, mock_coins)

@pytest.mark.asyncio
async def test_4h_8h_task_no_coins(mock_dependencies, caplog):
    """
    Тест: _process_4h_and_8h_task не получает монет из API.
    """
    # 1. Настройка моков
    mock_dependencies["get_coins"].return_value = None # Нет монет
    
    # 2. Вызов
    await _process_4h_and_8h_task({})
    
    # 3. Проверка
    mock_dependencies["get_coins"].assert_called_once()
    # Ничего больше не должно вызываться
    mock_dependencies["fetch_data"].assert_not_called()
    mock_dependencies["save_cache"].assert_not_called()
    mock_dependencies["gen_8h"].assert_not_called()
    assert "Из API не получено ни одной монеты" in caplog.text