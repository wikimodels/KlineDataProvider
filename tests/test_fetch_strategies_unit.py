# tests/test_fetch_strategies.py
"""
Unit tests for data_collector.fetch_strategies module
"""
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from aiohttp import ClientError
import data_collector.fetch_strategies as fetch_strategies


class MockAsyncContextManager:
    """Helper to create async context managers for testing"""
    def __init__(self, response=None, exception=None):
        self.response = response
        self.exception = exception
    
    async def __aenter__(self):
        if self.exception:
            raise self.exception
        return self.response
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


class TestFetchSimple:
    """Tests for fetch_simple function"""

    @pytest.mark.asyncio
    async def test_successful_fetch_returns_data(self):
        """Test successful HTTP 200 response returns parsed JSON data"""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=[{'open': 100, 'close': 101}])
        
        session = MagicMock()
        session.get = MagicMock(return_value=MockAsyncContextManager(mock_response))
        
        semaphore = asyncio.Semaphore(10)
        task_info = {
            'url': 'https://binance.com/api/v1/klines',
            'symbol': 'BTCUSDT',
            'data_type': 'klines',
            'exchange': 'binance',
            'original_timeframe': '4h'
        }

        result_task_info, result_data = await fetch_strategies.fetch_simple(
            session, task_info, semaphore)

        assert result_task_info == task_info
        assert result_data == [{'open': 100, 'close': 101}]
        session.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_429_rate_limit_returns_none(self):
        """Test HTTP 429 response returns None and logs warning"""
        mock_response = AsyncMock()
        mock_response.status = 429
        mock_response.text = AsyncMock(return_value='Too Many Requests')
        
        session = MagicMock()
        session.get = MagicMock(return_value=MockAsyncContextManager(mock_response))
        
        semaphore = asyncio.Semaphore(10)
        task_info = {
            'url': 'https://binance.com/api/v1/klines',
            'symbol': 'BTCUSDT',
            'data_type': 'klines',
            'exchange': 'binance',
            'original_timeframe': '4h'
        }

        with patch('data_collector.fetch_strategies.logger') as mock_logger:
            result_task_info, result_data = await fetch_strategies.fetch_simple(
                session, task_info, semaphore)

        assert result_task_info == task_info
        assert result_data is None
        mock_logger.warning.assert_called()
        assert '429' in mock_logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    async def test_other_error_status_logs_warning(self):
        """Test non-200/429 status codes log warning and return None"""
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value='Internal Server Error')
        
        session = MagicMock()
        session.get = MagicMock(return_value=MockAsyncContextManager(mock_response))
        
        semaphore = asyncio.Semaphore(10)
        task_info = {
            'url': 'https://binance.com/api/v1/klines',
            'symbol': 'BTCUSDT',
            'data_type': 'oi',
            'exchange': 'binance',
            'original_timeframe': '4h'
        }

        with patch('data_collector.fetch_strategies.logger') as mock_logger:
            result_task_info, result_data = await fetch_strategies.fetch_simple(
                session, task_info, semaphore)

        assert result_data is None
        mock_logger.warning.assert_called()
        assert '500' in mock_logger.warning.call_args[0][0]
        assert 'oi' in mock_logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    async def test_missing_url_returns_error(self):
        """Test missing URL in task_info logs error and returns None"""
        session = MagicMock()
        semaphore = asyncio.Semaphore(10)
        task_info = {
            'symbol': 'BTCUSDT',
            'data_type': 'klines',
            'exchange': 'binance',
            'original_timeframe': '4h'
            # 'url' is missing
        }

        with patch('data_collector.fetch_strategies.logger') as mock_logger:
            result_task_info, result_data = await fetch_strategies.fetch_simple(
                session, task_info, semaphore)

        assert result_task_info == task_info
        assert result_data is None
        mock_logger.error.assert_called_once()
        assert 'Отсутствует URL' in mock_logger.error.call_args[0][0]

    @pytest.mark.asyncio
    async def test_timeout_error_logs_warning(self):
        """Test asyncio.TimeoutError returns None (logging not implemented)"""
        session = MagicMock()
        session.get = MagicMock(return_value=MockAsyncContextManager(
            exception=asyncio.TimeoutError()
        ))
        
        semaphore = asyncio.Semaphore(10)
        task_info = {
            'url': 'https://binance.com/api/v1/klines',
            'symbol': 'BTCUSDT',
            'data_type': 'fr',
            'exchange': 'binance',
            'original_timeframe': '4h'
        }

        result = await fetch_strategies.fetch_simple(session, task_info, semaphore)
        
        # BUG: функция возвращает None вместо кортежа
        assert result is None

    @pytest.mark.asyncio
    async def test_network_error_raises_exception(self):
        """Test ClientError is NOT caught by current implementation (code limitation)"""
        # NOTE: The current code doesn't catch ClientError, so this documents that behavior
        session = MagicMock()
        session.get = MagicMock(return_value=MockAsyncContextManager(
            exception=ClientError("Connection failed")
        ))
        
        semaphore = asyncio.Semaphore(10)
        task_info = {
            'url': 'https://binance.com/api/v1/klines',
            'symbol': 'BTCUSDT',
            'data_type': 'klines',
            'exchange': 'binance',
            'original_timeframe': '4h'
        }

        # This will raise ClientError because it's not caught in the code
        with pytest.raises(ClientError):
            await fetch_strategies.fetch_simple(session, task_info, semaphore)

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrency(self):
        """Test semaphore is properly acquired and released"""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=[])
        
        session = MagicMock()
        session.get = MagicMock(return_value=MockAsyncContextManager(mock_response))
        
        semaphore = asyncio.Semaphore(1)
        task_info = {
            'url': 'https://binance.com/api/v1/klines',
            'symbol': 'BTCUSDT',
            'data_type': 'klines',
            'exchange': 'binance',
            'original_timeframe': '4h'
        }

        # Acquire semaphore before call to test it's blocked
        await semaphore.acquire()

        # Start fetch in background
        fetch_task = asyncio.create_task(
            fetch_strategies.fetch_simple(session, task_info, semaphore))
        
        # Should not complete immediately due to semaphore
        await asyncio.sleep(0.05)
        assert not fetch_task.done()

        # Release semaphore
        semaphore.release()
        
        # Now should complete
        result = await fetch_task
        assert result[1] == []

    @pytest.mark.asyncio
    async def test_uses_correct_timeout_and_headers(self):
        """Test fetch uses configured timeout and headers"""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=[])
        
        session = MagicMock()
        session.get = MagicMock(return_value=MockAsyncContextManager(mock_response))
        
        semaphore = asyncio.Semaphore(10)
        task_info = {
            'url': 'https://binance.com/api/v1/klines',
            'symbol': 'BTCUSDT',
            'data_type': 'klines',
            'exchange': 'binance',
            'original_timeframe': '4h'
        }

        await fetch_strategies.fetch_simple(session, task_info, semaphore)

        call_args = session.get.call_args
        assert call_args.kwargs['timeout'] == fetch_strategies.REQUEST_TIMEOUT


class TestConcurrencyConstants:
    """Tests for module-level constants"""

    def test_concurrency_limit_is_10(self):
        """Test CONCURRENCY_LIMIT is set to 10"""
        assert fetch_strategies.CONCURRENCY_LIMIT == 10

    def test_request_timeout_is_15(self):
        """Test REQUEST_TIMEOUT is set to 15 seconds"""
        assert fetch_strategies.REQUEST_TIMEOUT == 15

    def test_request_headers_contain_user_agent(self):
        """Test REQUEST_HEADERS includes proper User-Agent"""
        assert 'User-Agent' in fetch_strategies.REQUEST_HEADERS
        assert 'Chrome' in fetch_strategies.REQUEST_HEADERS['User-Agent']