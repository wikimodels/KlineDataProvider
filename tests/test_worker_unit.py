import pytest
from unittest.mock import patch, MagicMock, AsyncMock

# Импортируем *только* те функции, которые мы хотим тестировать
from worker import _process_single_timeframe_task, _process_4h_and_8h_task, _process_task

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
         patch('worker.generate_and_save_8h_cache', AsyncMock()) as mock_gen_8h, \
         patch('worker.run_fr_update_process', AsyncMock()) as mock_run_fr: # <-- НОВЫЙ МОК
        
        # Передаем моки в 'yield', чтобы они были доступны в тестах
        yield {
            "get_coins": mock_get_coins,
            "get_cache": mock_get_cache,
            "save_cache": mock_save_cache,
            "fetch_data": mock_fetch_data,
            "gen_8h": mock_gen_8h,
            "run_fr": mock_run_fr # <-- НОВЫЙ МОК
        }

# --- Тесты для _process_single_timeframe_task ---
# (Эти тесты не изменились)
@pytest.mark.asyncio
async def test_single_task_success(mock_dependencies):
    """
    Тест: Успешный запуск _process_single_timeframe_task (например, '1h').
    """
    mock_coins = [{'symbol': 'BTCUSDT', 'exchanges': ['binance']}]
    mock_market_data = {'data': [{'symbol': 'BTCUSDT', 'data': [1,2,3]}]}
    
    mock_dependencies["get_coins"].return_value = mock_coins
    mock_dependencies["get_cache"].return_value = {} # 'global_fr'
    mock_dependencies["fetch_data"].return_value = mock_market_data
    
    await _process_single_timeframe_task('1h', {})
    
    mock_dependencies["get_coins"].assert_called_once()
    mock_dependencies["fetch_data"].assert_called_once_with(
        mock_coins, '1h', prefetched_fr_data={}
    )
    mock_dependencies["save_cache"].assert_called_once_with('1h', mock_market_data)

@pytest.mark.asyncio
async def test_single_task_no_coins(mock_dependencies, caplog):
    """
    Тест: _process_single_timeframe_task не получает монет из API.
    """
    mock_dependencies["get_coins"].return_value = [] # Нет монет
    
    await _process_single_timeframe_task('1h', {})
    
    mock_dependencies["get_coins"].assert_called_once()
    mock_dependencies["fetch_data"].assert_not_called()
    mock_dependencies["save_cache"].assert_not_called()
    assert "Из API не получено ни одной монеты" in caplog.text

# --- Тесты для _process_4h_and_8h_task ---
# (Эти тесты не изменились)
@pytest.mark.asyncio
async def test_4h_8h_task_success(mock_dependencies):
    """
    Тест: Успешный запуск _process_4h_and_8h_task.
    """
    mock_coins = [{'symbol': 'BTCUSDT', 'exchanges': ['binance']}]
    mock_master_data = {'data': [{'symbol': 'BTCUSDT', 'data': [1,2,3]}]} # Данные 4h
    
    mock_dependencies["get_coins"].return_value = mock_coins
    mock_dependencies["get_cache"].return_value = {} # 'global_fr'
    mock_dependencies["fetch_data"].return_value = mock_master_data
    
    await _process_4h_and_8h_task({})
    
    mock_dependencies["get_coins"].assert_called_once()
    mock_dependencies["fetch_data"].assert_called_once_with(
        mock_coins, '4h', prefetched_fr_data={}
    )
    mock_dependencies["save_cache"].assert_called_once_with('4h', mock_master_data)
    mock_dependencies["gen_8h"].assert_called_once_with(mock_master_data, mock_coins)

@pytest.mark.asyncio
async def test_4h_8h_task_no_coins(mock_dependencies, caplog):
    """
    Тест: _process_4h_and_8h_task не получает монет из API.
    """
    mock_dependencies["get_coins"].return_value = None # Нет монет
    
    await _process_4h_and_8h_task({})
    
    mock_dependencies["get_coins"].assert_called_once()
    mock_dependencies["fetch_data"].assert_not_called()
    mock_dependencies["save_cache"].assert_not_called()
    mock_dependencies["gen_8h"].assert_not_called()
    assert "Из API не получено ни одной монеты" in caplog.text

# ---
# --- НОВЫЕ ТЕСТЫ для роутера _process_task ---
# ---

@pytest.mark.asyncio
# Патчим функции, которые _process_task вызывает (а не те, что мы уже замокали в фикстуре)
@patch('worker._load_global_fr_cache')
@patch('worker.run_fr_update_process')
@patch('worker._process_single_timeframe_task')
@patch('worker._process_4h_and_8h_task')
async def test_process_task_routes_to_fr(
    mock_process_4h, mock_process_1h, mock_run_fr, mock_load_fr
):
    """Тест: _process_task('fr') вызывает run_fr_update_process."""
    
    await _process_task("fr")
    
    # Должен вызвать FR и НЕ вызывать Klines/OI
    mock_run_fr.assert_called_once()
    mock_load_fr.assert_not_called() # Кэш FR не нужен для сбора FR
    mock_process_1h.assert_not_called()
    mock_process_4h.assert_not_called()


@pytest.mark.asyncio
@patch('worker._load_global_fr_cache')
@patch('worker.run_fr_update_process')
@patch('worker._process_single_timeframe_task')
@patch('worker._process_4h_and_8h_task')
async def test_process_task_routes_to_1h(
    mock_process_4h, mock_process_1h, mock_run_fr, mock_load_fr
):
    """Тест: _process_task('1h') вызывает _process_single_timeframe_task."""
    
    # 1. Настройка моков
    mock_load_fr.return_value = {"BTC": "fr_data"} # Имитируем загрузку кэша
    
    # 2. Вызов
    await _process_task("1h")
    
    # 3. Проверка
    # Должен вызвать Klines/OI и НЕ вызывать FR
    mock_run_fr.assert_not_called()
    mock_load_fr.assert_called_once() # Кэш FR НУЖЕН для Klines
    mock_process_1h.assert_called_once_with("1h", {"BTC": "fr_data"})
    mock_process_4h.assert_not_called()


@pytest.mark.asyncio
@patch('worker._load_global_fr_cache')
@patch('worker.run_fr_update_process')
@patch('worker._process_single_timeframe_task')
@patch('worker._process_4h_and_8h_task')
async def test_process_task_routes_to_4h(
    mock_process_4h, mock_process_1h, mock_run_fr, mock_load_fr
):
    """Тест: _process_task('4h') вызывает _process_4h_and_8h_task."""
    
    mock_load_fr.return_value = {} # Имитируем загрузку кэша
    
    await _process_task("4h")
    
    mock_run_fr.assert_not_called()
    mock_load_fr.assert_called_once() 
    mock_process_1h.assert_not_called()
    mock_process_4h.assert_called_once_with({})