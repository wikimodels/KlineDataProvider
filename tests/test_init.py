# tests/test_init.py
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from data_collector import fetch_market_data

@pytest.mark.asyncio
async def test_prefetched_fr_without_klines_returns_formatted():
    """Если нет задач Klines/OI, но есть prefetched FR - должен вернуть отформатированные данные"""
    with patch('data_collector.task_builder.prepare_tasks', return_value=[]) as mock_prepare, \
         patch('data_collector.data_processing.merge_data', return_value={'BTCUSDT': {'fr': []}}) as mock_merge, \
         patch('data_collector.data_processing.format_final_structure', return_value={'BTCUSDT': {'fr_data': []}}) as mock_format:

        coins = [{'symbol': 'BTCUSDT'}]
        prefetched_fr = {'BTCUSDT': [{'fr': 0.01}]}
        
        result = await fetch_market_data(coins, '4h', prefetched_fr_data=prefetched_fr)
        
        mock_prepare.assert_called_once_with(coins, '4h', prefetched_fr)
        mock_merge.assert_called_once()
        mock_format.assert_called_once_with({'BTCUSDT': {'fr': []}}, coins, '4h')
        assert 'BTCUSDT' in result

@pytest.mark.asyncio
async def test_skip_formatting_returns_raw_merged_data():
    """При skip_formatting=True должен вернуть сырую merged_data без форматирования"""
    with patch('data_collector.task_builder.prepare_tasks', return_value=[]), \
         patch('data_collector.data_processing.merge_data', return_value={'raw': 'data'}), \
         patch('data_collector.data_processing.format_final_structure') as mock_format:

        coins = []
        # ВАЖНО: передаем prefetched_fr_data, чтобы не попасть в ветку "пустой запрос"
        prefetched_fr = {'BTCUSDT': []}
        result = await fetch_market_data(coins, '4h', prefetched_fr_data=prefetched_fr, skip_formatting=True)
        
        mock_format.assert_not_called()
        assert result == {'raw': 'data'}

@pytest.mark.asyncio
async def test_empty_prefetched_fr_logs_warning():
    """Если prefetched_fr_data пустой - должен написать warning"""
    with patch('data_collector.task_builder.prepare_tasks', return_value=[]), \
         patch('data_collector.data_processing.merge_data', return_value={}), \
         patch('data_collector.data_processing.format_final_structure', return_value={}), \
         patch('data_collector.logger') as mock_logger:

        coins = []
        # Передаем пустой dict, чтобы попасть в ветку логирования
        await fetch_market_data(coins, '4h', prefetched_fr_data={})

        mock_logger.warning.assert_called()
        # ИСПРАВЛЕНО: проверяем правильное сообщение для этой ветки
        assert 'Нет задач Klines/OI и нет предзагруженных FR' in mock_logger.warning.call_args[0][0]

@pytest.mark.asyncio
async def test_gather_exceptions_increment_failed_counter():
    """Если задачи падают в gather - failed_tasks_count должен инкрементироваться"""
    mock_task = {
        'fetch_strategy': AsyncMock(return_value=(None, Exception('Test error'))),
        'parser': MagicMock(__name__='mock_parser'),
        'task_info': {'symbol': 'BTCUSDT', 'data_type': 'klines', 'exchange': 'binance', 'url': 'test'}
    }

    with patch('data_collector.task_builder.prepare_tasks', return_value=[mock_task]), \
         patch('aiohttp.ClientSession') as mock_session, \
         patch('data_collector.data_processing.merge_data', return_value={}), \
         patch('data_collector.data_processing.format_final_structure', return_value={}), \
         patch('data_collector.logger') as mock_logger:

        # Мокаем семафор
        mock_semaphore = AsyncMock()
        
        await fetch_market_data([{'symbol': 'BTCUSDT'}], '4h')
        
        # Проверяем, что error лог был вызван (failed_tasks_count++)
        mock_logger.error.assert_called()
        # Ищем сообщение с ошибкой парсинга
        error_calls = [str(call) for call in mock_logger.error.call_args_list]
        assert any('PARSER: Ошибка парсинга' in call for call in error_calls)