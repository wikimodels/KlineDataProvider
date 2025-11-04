import unittest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import asyncio
import aiohttp
from typing import Dict, Any, Tuple, Optional, List

# Import the module to test
try:
    from data_collector import fetch_strategies
except ImportError:
    # For direct file execution
    import fetch_strategies


class TestFetchStrategies(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_logger = Mock()
        # Patch the logger in the module
        patcher = patch.object(fetch_strategies, 'logger', self.mock_logger)
        self.addCleanup(patcher.stop)
        patcher.start()
        
        # Create mock task_info
        self.task_info = {
            "url": "https://example.com/api",
            "symbol": "BTC/USDT",
            "data_type": "klines",
            "exchange": "binance",
            "original_timeframe": "1h"
        }
        
        # Create mock semaphore
        self.semaphore = asyncio.Semaphore(10)

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_simple_success(self, mock_get):
        """Test fetch_simple with successful response."""
        # Setup mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=[{"data": "test"}])
        mock_get.return_value.__aenter__.return_value = mock_response
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_simple(session, self.task_info, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info, self.task_info)
        self.assertEqual(data, [{"data": "test"}])

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_simple_missing_url(self, mock_get):
        """Test fetch_simple when URL is missing from task_info."""
        task_info_no_url = self.task_info.copy()
        task_info_no_url["url"] = None
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_simple(session, task_info_no_url, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info, task_info_no_url)
        self.assertIsNone(data)
        self.mock_logger.error.assert_called()

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_simple_429_status(self, mock_get):
        """Test fetch_simple with 429 Too Many Requests status."""
        mock_response = AsyncMock()
        mock_response.status = 429
        mock_response.text = AsyncMock(return_value="Too Many Requests")
        mock_get.return_value.__aenter__.return_value = mock_response
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_simple(session, self.task_info, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info, self.task_info)
        self.assertIsNone(data)
        self.mock_logger.warning.assert_called()

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_simple_error_status(self, mock_get):
        """Test fetch_simple with error status codes."""
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        mock_get.return_value.__aenter__.return_value = mock_response
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_simple(session, self.task_info, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info, self.task_info)
        self.assertIsNone(data)
        self.mock_logger.warning.assert_called()

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_simple_oi_fr_error_status(self, mock_get):
        """Test fetch_simple with error status codes for oi/fr data types."""
        task_info_oi = self.task_info.copy()
        task_info_oi["data_type"] = "oi"
        
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        mock_get.return_value.__aenter__.return_value = mock_response
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_simple(session, task_info_oi, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info, task_info_oi)
        self.assertIsNone(data)
        self.mock_logger.warning.assert_called()

    async def test_fetch_simple_timeout_error(self):
        """Test fetch_simple with asyncio timeout error."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.side_effect = asyncio.TimeoutError("Request timeout")
            
            async with aiohttp.ClientSession() as session:
                result = await fetch_strategies.fetch_simple(session, self.task_info, self.semaphore)
            
            task_info, data = result
            self.assertEqual(task_info, self.task_info)
            self.assertIsNone(data)
            self.mock_logger.error.assert_called()

    async def test_fetch_simple_client_error(self):
        """Test fetch_simple with aiohttp client error."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.side_effect = aiohttp.ClientError("Connection error")
            
            async with aiohttp.ClientSession() as session:
                result = await fetch_strategies.fetch_simple(session, self.task_info, self.semaphore)
            
            task_info, data = result
            self.assertEqual(task_info, self.task_info)
            self.assertIsNone(data)
            self.mock_logger.error.assert_called()

    async def test_fetch_simple_general_exception(self):
        """Test fetch_simple with general exception."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.side_effect = Exception("Unexpected error")
            
            async with aiohttp.ClientSession() as session:
                result = await fetch_strategies.fetch_simple(session, self.task_info, self.semaphore)
            
            task_info, data = result
            self.assertEqual(task_info, self.task_info)
            self.assertIsNone(data)
            self.mock_logger.error.assert_called()

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_bybit_paginated_success_single_page(self, mock_get):
        """Test fetch_bybit_paginated with successful response on single page."""
        # Setup mock response for single page
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [{"data": "test1"}, {"data": "test2"}],
                "nextPageCursor": None
            }
        })
        mock_get.return_value.__aenter__.return_value = mock_response
        
        task_info = self.task_info.copy()
        task_info["url"] = "https://api.bybit.com/v5/market/funding/history?symbol=BTCUSDT"
        task_info["data_type"] = "fr"
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_bybit_paginated(session, task_info, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info["symbol"], "BTC/USDT")
        self.assertEqual(len(data), 2)

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_bybit_paginated_success_multiple_pages(self, mock_get):
        """Test fetch_bybit_paginated with successful response on multiple pages."""
        # Setup mock responses for multiple pages
        responses = [
            {
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": [{"data": f"test{i}"} for i in range(100)],
                    "nextPageCursor": "cursor2" if i < 1 else None
                }
            }
            for i in range(2)
        ]
        
        async def mock_json():
            return responses.pop(0)
        
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = mock_json
        mock_get.return_value.__aenter__.return_value = mock_response
        
        task_info = self.task_info.copy()
        task_info["url"] = "https://api.bybit.com/v5/market/funding/history?symbol=BTCUSDT"
        task_info["data_type"] = "fr"
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_bybit_paginated(session, task_info, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info["symbol"], "BTC/USDT")
        # Should have 200 records (100 from each page)
        self.assertEqual(len(data), 200)

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_bybit_paginated_missing_url(self, mock_get):
        """Test fetch_bybit_paginated when URL is missing from task_info."""
        task_info_no_url = self.task_info.copy()
        task_info_no_url["url"] = None
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_bybit_paginated(session, task_info_no_url, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info, task_info_no_url)
        self.assertIsNone(data)
        self.mock_logger.error.assert_called()

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_bybit_paginated_api_error(self, mock_get):
        """Test fetch_bybit_paginated when Bybit API returns error."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "retCode": 10001,
            "retMsg": "Invalid request"
        })
        mock_get.return_value.__aenter__.return_value = mock_response
        
        task_info = self.task_info.copy()
        task_info["url"] = "https://api.bybit.com/v5/market/funding/history?symbol=BTCUSDT"
        task_info["data_type"] = "fr"
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_bybit_paginated(session, task_info, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info["symbol"], "BTC/USDT")
        self.assertIsNone(data)
        self.mock_logger.error.assert_called()

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_bybit_paginated_429_status(self, mock_get):
        """Test fetch_bybit_paginated with 429 Too Many Requests status."""
        mock_response = AsyncMock()
        mock_response.status = 429
        mock_response.text = AsyncMock(return_value="Too Many Requests")
        mock_get.return_value.__aenter__.return_value = mock_response
        
        task_info = self.task_info.copy()
        task_info["url"] = "https://api.bybit.com/v5/market/funding/history?symbol=BTCUSDT"
        task_info["data_type"] = "fr"
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_bybit_paginated(session, task_info, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info["symbol"], "BTC/USDT")
        self.assertIsNone(data)
        self.mock_logger.warning.assert_called()

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_bybit_paginated_error_status(self, mock_get):
        """Test fetch_bybit_paginated with error status codes."""
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        mock_get.return_value.__aenter__.return_value = mock_response
        
        task_info = self.task_info.copy()
        task_info["url"] = "https://api.bybit.com/v5/market/funding/history?symbol=BTCUSDT"
        task_info["data_type"] = "fr"
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_bybit_paginated(session, task_info, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info["symbol"], "BTC/USDT")
        self.assertIsNone(data)
        self.mock_logger.warning.assert_called()

    async def test_fetch_bybit_paginated_timeout_error(self):
        """Test fetch_bybit_paginated with asyncio timeout error."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.side_effect = asyncio.TimeoutError("Request timeout")
            
            task_info = self.task_info.copy()
            task_info["url"] = "https://api.bybit.com/v5/market/funding/history?symbol=BTCUSDT"
            task_info["data_type"] = "fr"
            
            async with aiohttp.ClientSession() as session:
                result = await fetch_strategies.fetch_bybit_paginated(session, task_info, self.semaphore)
            
            task_info, data = result
            self.assertEqual(task_info["symbol"], "BTC/USDT")
            self.assertIsNone(data)
            self.mock_logger.error.assert_called()

    async def test_fetch_bybit_paginated_client_error(self):
        """Test fetch_bybit_paginated with aiohttp client error."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.side_effect = aiohttp.ClientError("Connection error")
            
            task_info = self.task_info.copy()
            task_info["url"] = "https://api.bybit.com/v5/market/funding/history?symbol=BTCUSDT"
            task_info["data_type"] = "fr"
            
            async with aiohttp.ClientSession() as session:
                result = await fetch_strategies.fetch_bybit_paginated(session, task_info, self.semaphore)
            
            task_info, data = result
            self.assertEqual(task_info["symbol"], "BTC/USDT")
            self.assertIsNone(data)
            self.mock_logger.error.assert_called()

    async def test_fetch_bybit_paginated_general_exception(self):
        """Test fetch_bybit_paginated with general exception."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.side_effect = Exception("Unexpected error")
            
            task_info = self.task_info.copy()
            task_info["url"] = "https://api.bybit.com/v5/market/funding/history?symbol=BTCUSDT"
            task_info["data_type"] = "fr"
            
            async with aiohttp.ClientSession() as session:
                result = await fetch_strategies.fetch_bybit_paginated(session, task_info, self.semaphore)
            
            task_info, data = result
            self.assertEqual(task_info["symbol"], "BTC/USDT")
            self.assertIsNone(data)
            self.mock_logger.error.assert_called()

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_bybit_paginated_max_pages_reached(self, mock_get):
        """Test fetch_bybit_paginated when max pages limit is reached."""
        # Mock response that always has next cursor to trigger max pages
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [{"data": "test"}],
                "nextPageCursor": "next_cursor"
            }
        })
        mock_get.return_value.__aenter__.return_value = mock_response
        
        task_info = self.task_info.copy()
        task_info["url"] = "https://api.bybit.com/v5/market/funding/history?symbol=BTCUSDT"
        task_info["data_type"] = "fr"
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_bybit_paginated(session, task_info, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info["symbol"], "BTC/USDT")
        # Should have collected data from 10 pages (max limit)
        self.assertEqual(len(data), 10)  # 1 record per page * 10 pages
        self.mock_logger.warning.assert_called()

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_simple_with_oi_data_type(self, mock_get):
        """Test fetch_simple with oi data type and error status."""
        task_info_oi = self.task_info.copy()
        task_info_oi["data_type"] = "oi"
        
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        mock_get.return_value.__aenter__.return_value = mock_response
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_simple(session, task_info_oi, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info, task_info_oi)
        self.assertIsNone(data)
        # Should log as warning for oi/fr
        self.mock_logger.warning.assert_called()

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_simple_with_fr_data_type(self, mock_get):
        """Test fetch_simple with fr data type and error status."""
        task_info_fr = self.task_info.copy()
        task_info_fr["data_type"] = "fr"
        
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        mock_get.return_value.__aenter__.return_value = mock_response
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_simple(session, task_info_fr, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info, task_info_fr)
        self.assertIsNone(data)
        # Should log as warning for oi/fr
        self.mock_logger.warning.assert_called()

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_bybit_paginated_with_oi_data_type(self, mock_get):
        """Test fetch_bybit_paginated with oi data type and error status."""
        task_info_oi = self.task_info.copy()
        task_info_oi["url"] = "https://api.bybit.com/v5/market/open-interest?symbol=BTCUSDT&category=linear"
        task_info_oi["data_type"] = "oi"
        
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        mock_get.return_value.__aenter__.return_value = mock_response
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_bybit_paginated(session, task_info_oi, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info, task_info_oi)
        self.assertIsNone(data)
        self.mock_logger.warning.assert_called()

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_bybit_paginated_with_fr_data_type(self, mock_get):
        """Test fetch_bybit_paginated with fr data type and error status."""
        task_info_fr = self.task_info.copy()
        task_info_fr["url"] = "https://api.bybit.com/v5/market/funding/history?symbol=BTCUSDT"
        task_info_fr["data_type"] = "fr"
        
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        mock_get.return_value.__aenter__.return_value = mock_response
        
        async with aiohttp.ClientSession() as session:
            result = await fetch_strategies.fetch_bybit_paginated(session, task_info_fr, self.semaphore)
        
        task_info, data = result
        self.assertEqual(task_info, task_info_fr)
        self.assertIsNone(data)
        self.mock_logger.warning.assert_called()


if __name__ == '__main__':
    unittest.main()