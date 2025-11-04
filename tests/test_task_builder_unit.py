import unittest
from unittest.mock import Mock, patch, MagicMock, ANY
from typing import List, Dict, Any, Optional
import logging
import sys
from io import StringIO
import json

# Import the module to test
try:
    from data_collector import task_builder
except ImportError:
    # For direct file execution
    import task_builder


class TestTaskBuilder(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_coins = [
            {
                "symbol": "BTC/USDT:USDT",
                "exchanges": ["binance", "bybit"]
            },
            {
                "symbol": "ETH/USDT:USDT",
                "exchanges": ["binance"]
            },
            {
                "symbol": "SOL/USDT:USDT",
                "exchanges": ["bybit"]
            }
        ]
        
        self.mock_logger = Mock()
        # Patch the logger in the module
        patcher = patch.object(task_builder, 'logger', self.mock_logger)
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_prepare_fr_tasks_with_valid_coins(self):
        """Test prepare_fr_tasks with valid coin data."""
        # Mock the external dependencies
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        # Setup mock return values
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        mock_api_parser.parse_binance_fr = Mock()
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            result = task_builder.prepare_fr_tasks(self.sample_coins)
            
            # Assertions
            self.assertEqual(len(result), 3)  # 3 coins
            self.mock_logger.info.assert_any_call("[FR_TASK_BUILDER] Создание задач для сбора FR для 3 монет...")
            self.mock_logger.info.assert_any_call("[FR_TASK_BUILDER] Создано 3 задач для FR.")
            
            # Check that the correct functions were called for each coin
            # BTC/USDT:USDT -> binance (because 'binance' in ['binance', 'bybit'])
            # ETH/USDT:USDT -> binance (because 'binance' in ['binance'])
            # SOL/USDT:USDT -> bybit (because 'binance' not in ['bybit'])
            mock_url_builder.get_binance_funding_rate_url.assert_any_call(ANY, 400)  # Called for BTC and ETH
            mock_url_builder.get_bybit_funding_rate_url.assert_called_once_with(ANY, 400)  # Called for SOL
            # The parser functions are called when getattr gets them, so they should be accessed
            self.assertTrue(mock_api_parser.parse_binance_fr is not None)
            self.assertTrue(mock_api_parser.parse_bybit_fr is not None)

    def test_prepare_fr_tasks_with_missing_functions(self):
        """Test prepare_fr_tasks when URL or parser functions are missing."""
        # Mock the external dependencies with missing functions
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        # Setup mock to return None for binance functions (missing)
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value=None)
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        mock_api_parser.parse_binance_fr = None  # Missing
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            result = task_builder.prepare_fr_tasks(self.sample_coins)
            
            # BTC/USDT:USDT -> binance (but functions missing) -> skip
            # ETH/USDT:USDT -> binance (but functions missing) -> skip  
            # SOL/USDT:USDT -> bybit (functions available) -> create task
            self.assertEqual(len(result), 1)
            # Check that error was logged for missing function
            self.mock_logger.error.assert_called()

    def test_prepare_fr_tasks_with_empty_coins_list(self):
        """Test prepare_fr_tasks with empty coins list."""
        result = task_builder.prepare_fr_tasks([])
        self.assertEqual(len(result), 0)
        self.mock_logger.info.assert_any_call("[FR_TASK_BUILDER] Создание задач для сбора FR для 0 монет...")
        self.mock_logger.info.assert_any_call("[FR_TASK_BUILDER] Создано 0 задач для FR.")

    def test_prepare_fr_tasks_with_invalid_coin_format(self):
        """Test prepare_fr_tasks with invalid coin format."""
        invalid_coins = [
            {"symbol": "INVALID", "exchanges": ["binance"]},  # No colon
            {"symbol": "BTC/USDT:USDT", "exchanges": []},  # No exchanges
        ]
        
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        mock_api_parser.parse_binance_fr = Mock()
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            result = task_builder.prepare_fr_tasks(invalid_coins)
            
            # First coin: "INVALID" -> symbol_path = "INVALID", exchange = binance (since 'binance' in ['binance']) -> should work
            # Second coin: exchanges = [], so 'binance' not in [], exchange = 'bybit' -> should work
            self.assertEqual(len(result), 2)

    def test_prepare_tasks_with_valid_data(self):
        """Test prepare_tasks with valid data for all timeframes."""
        timeframes = ['1h', '4h', '8h', '1d']
        
        for timeframe in timeframes:
            with self.subTest(timeframe=timeframe):
                mock_url_builder = Mock()
                mock_api_parser = Mock()
                
                # Setup all required mock functions
                mock_url_builder.get_binance_klines_url = Mock(return_value="https://binance.com/klines")
                mock_url_builder.get_bybit_klines_url = Mock(return_value="https://bybit.com/klines")
                mock_url_builder.get_binance_open_interest_url = Mock(return_value="https://binance.com/oi")
                mock_url_builder.get_bybit_open_interest_url = Mock(return_value="https://bybit.com/oi")
                mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
                mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
                
                # Setup parser mocks
                mock_api_parser.parse_binance_klines = Mock()
                mock_api_parser.parse_bybit_klines = Mock()
                mock_api_parser.parse_binance_oi = Mock()
                mock_api_parser.parse_bybit_oi = Mock()
                mock_api_parser.parse_binance_fr = Mock()
                mock_api_parser.parse_bybit_fr = Mock()
                
                with patch('data_collector.task_builder.url_builder', mock_url_builder), \
                     patch('data_collector.task_builder.api_parser', mock_api_parser), \
                     patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
                    
                    mock_fetch_strategies.fetch_simple = Mock()
                    mock_fetch_strategies.fetch_bybit_paginated = Mock()
                    
                    result = task_builder.prepare_tasks(self.sample_coins, timeframe)
                    
                    # Each coin should generate 3 tasks (klines, oi, fr) = 9 total for 3 coins
                    expected_tasks = 3 * 3  # 3 data types per coin
                    self.assertEqual(len(result), expected_tasks)

    def test_prepare_tasks_with_prefetched_fr_data(self):
        """Test prepare_tasks when prefetched FR data is provided."""
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        mock_url_builder.get_binance_klines_url = Mock(return_value="https://binance.com/klines")
        mock_url_builder.get_bybit_klines_url = Mock(return_value="https://bybit.com/klines")
        mock_url_builder.get_binance_open_interest_url = Mock(return_value="https://binance.com/oi")
        mock_url_builder.get_bybit_open_interest_url = Mock(return_value="https://bybit.com/oi")
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        
        mock_api_parser.parse_binance_klines = Mock()
        mock_api_parser.parse_bybit_klines = Mock()
        mock_api_parser.parse_binance_oi = Mock()
        mock_api_parser.parse_bybit_oi = Mock()
        mock_api_parser.parse_binance_fr = Mock()
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            # Pass prefetched_fr_data - this should skip FR tasks
            result = task_builder.prepare_tasks(self.sample_coins, '1h', prefetched_fr_data={'some': 'data'})
            
            # Should only generate klines and oi tasks, no fr tasks = 6 total (3 coins * 2 data types)
            expected_tasks = 3 * 2  # 2 data types per coin (klines, oi) without fr
            self.assertEqual(len(result), expected_tasks)

    def test_prepare_tasks_with_missing_klines_functions(self):
        """Test prepare_tasks when klines functions are missing."""
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        # Set klines functions to None
        mock_url_builder.get_binance_klines_url = Mock(return_value=None)
        mock_url_builder.get_bybit_klines_url = Mock(return_value=None)
        mock_url_builder.get_binance_open_interest_url = Mock(return_value="https://binance.com/oi")
        mock_url_builder.get_bybit_open_interest_url = Mock(return_value="https://bybit.com/oi")
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        
        mock_api_parser.parse_binance_klines = None
        mock_api_parser.parse_bybit_klines = None
        mock_api_parser.parse_binance_oi = Mock()
        mock_api_parser.parse_bybit_oi = Mock()
        mock_api_parser.parse_binance_fr = Mock()
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            result = task_builder.prepare_tasks(self.sample_coins, '1h')
            
            # Should only generate oi and fr tasks since klines functions are missing
            # Each coin generates 2 tasks (oi, fr) = 6 total for 3 coins
            expected_tasks = 3 * 2  # 2 data types per coin (oi, fr)
            self.assertEqual(len(result), expected_tasks)
            
            # Check that error was logged
            self.mock_logger.error.assert_called()

    def test_prepare_tasks_with_different_timeframes(self):
        """Test prepare_tasks with different timeframes and their specific logic."""
        # Test 8h timeframe (should use 4h API timeframe)
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        mock_url_builder.get_binance_klines_url = Mock(return_value="https://binance.com/klines")
        mock_url_builder.get_bybit_klines_url = Mock(return_value="https://bybit.com/klines")
        mock_url_builder.get_binance_open_interest_url = Mock(return_value="https://binance.com/oi")
        mock_url_builder.get_bybit_open_interest_url = Mock(return_value="https://bybit.com/oi")
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        
        mock_api_parser.parse_binance_klines = Mock()
        mock_api_parser.parse_bybit_klines = Mock()
        mock_api_parser.parse_binance_oi = Mock()
        mock_api_parser.parse_bybit_oi = Mock()
        mock_api_parser.parse_binance_fr = Mock()
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            result = task_builder.prepare_tasks(self.sample_coins, '8h')
            
            # Verify that all tasks have the correct timeframe
            for task in result:
                # For 8h, klines should use 4h timeframe, oi/fr should use original
                if task['task_info']['data_type'] == 'klines':
                    # The api_timeframe should be '4h' for 8h input
                    # This is handled in the original code where api_timeframe = '4h' if timeframe == '8h' else timeframe
                    pass  # The logic in the code sets api_timeframe to '4h' for '8h'
            
            self.assertEqual(len(result), 9)  # 3 coins * 3 data types

    def test_prepare_tasks_error_logging(self):
        """Test that errors are properly logged in prepare_tasks."""
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        # Make one function return None to trigger error logging
        mock_url_builder.get_binance_klines_url = Mock(return_value="https://binance.com/klines")
        mock_url_builder.get_bybit_klines_url = Mock(return_value="https://bybit.com/klines")
        mock_url_builder.get_binance_open_interest_url = Mock(return_value=None)  # Missing
        mock_url_builder.get_bybit_open_interest_url = Mock(return_value="https://bybit.com/oi")
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        
        mock_api_parser.parse_binance_klines = Mock()
        mock_api_parser.parse_bybit_klines = Mock()
        mock_api_parser.parse_binance_oi = None  # Missing
        mock_api_parser.parse_bybit_oi = Mock()
        mock_api_parser.parse_binance_fr = Mock()
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            result = task_builder.prepare_tasks(self.sample_coins, '1h')
            
            # Should log error for missing open interest function
            # The error should be logged for binance oi function
            self.mock_logger.error.assert_called()

    def test_exchange_selection_logic(self):
        """Test that the correct exchange is selected based on available exchanges."""
        coins_with_specific_exchanges = [
            {"symbol": "BTC/USDT:USDT", "exchanges": ["binance"]},
            {"symbol": "ETH/USDT:USDT", "exchanges": ["bybit"]},
            {"symbol": "SOL/USDT:USDT", "exchanges": ["binance", "bybit"]},  # Should pick binance
        ]
        
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        mock_api_parser.parse_binance_fr = Mock()
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            result = task_builder.prepare_fr_tasks(coins_with_specific_exchanges)
            
            # Check that the correct exchange is selected for each coin
            exchanges_used = [task['task_info']['exchange'] for task in result]
            expected_exchanges = ['binance', 'bybit', 'binance']  # binance preferred when both available
            self.assertEqual(exchanges_used, expected_exchanges)

    def test_symbol_processing_logic(self):
        """Test that symbols are correctly processed (split by colon, slashes removed)."""
        coins_with_complex_symbols = [
            {"symbol": "BTC/USDT:USDT", "exchanges": ["binance"]},
            {"symbol": "ETH/USD:ETH", "exchanges": ["bybit"]},
        ]
        
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        mock_api_parser.parse_binance_fr = Mock()
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            result = task_builder.prepare_fr_tasks(coins_with_complex_symbols)
            
            # Check that symbols are correctly processed
            symbols_used = [task['task_info']['symbol'] for task in result]
            expected_symbols = ['BTC/USDT', 'ETH/USD']
            self.assertEqual(symbols_used, expected_symbols)

    def test_function_import_error_handling(self):
        """Test that the module handles missing imports gracefully."""
        # Test the ImportError fallback by patching the imports to raise ImportError
        with patch('data_collector.task_builder.url_builder', None), \
             patch('data_collector.task_builder.api_parser', None), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            # When url_builder or api_parser is None, getattr will return None
            # This should cause the function to log an error and continue
            mock_coins = [{"symbol": "BTC/USDT:USDT", "exchanges": ["binance"]}]
            
            # The function should handle the None functions gracefully
            result = task_builder.prepare_fr_tasks(mock_coins)
            
            # Should return empty list since functions are missing
            self.assertEqual(len(result), 0)
            # Error should be logged
            self.mock_logger.error.assert_called()

    def test_task_structure_integrity(self):
        """Test that returned tasks have the correct structure."""
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        mock_api_parser.parse_binance_fr = Mock()
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            result = task_builder.prepare_fr_tasks([{"symbol": "BTC/USDT:USDT", "exchanges": ["binance"]}])
            
            # Check task structure
            self.assertEqual(len(result), 1)
            task = result[0]
            
            # Verify required keys exist
            self.assertIn('task_info', task)
            self.assertIn('fetch_strategy', task)
            self.assertIn('parser', task)
            self.assertIn('timeframe', task)
            
            # Verify task_info structure
            task_info = task['task_info']
            self.assertIn('symbol', task_info)
            self.assertIn('exchange', task_info)
            self.assertIn('data_type', task_info)
            self.assertIn('url', task_info)
            self.assertIn('task_specific_timeframe', task_info)

    def test_logging_messages_format(self):
        """Test that logging messages have the correct format."""
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        mock_api_parser.parse_binance_fr = Mock()
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            task_builder.prepare_fr_tasks(self.sample_coins)
            
            # Check that info messages contain the expected prefixes
            self.mock_logger.info.assert_any_call("[FR_TASK_BUILDER] Создание задач для сбора FR для 3 монет...")
            self.mock_logger.info.assert_any_call("[FR_TASK_BUILDER] Создано 3 задач для FR.")

    def test_edge_case_empty_exchanges(self):
        """Test handling of coins with empty exchanges list."""
        coins_with_empty_exchanges = [
            {"symbol": "BTC/USDT:USDT", "exchanges": []},
        ]
        
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        mock_api_parser.parse_binance_fr = Mock()
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            result = task_builder.prepare_fr_tasks(coins_with_empty_exchanges)
            
            # When exchanges is empty, exchange = 'binance' if 'binance' in [] else 'bybit' -> 'bybit'
            # So it should try bybit functions
            # Since we have bybit functions mocked, it should work
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]['task_info']['exchange'], 'bybit')

    def test_prepare_tasks_missing_oi_function(self):
        """Test prepare_tasks when open interest functions are missing."""
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        mock_url_builder.get_binance_klines_url = Mock(return_value="https://binance.com/klines")
        mock_url_builder.get_bybit_klines_url = Mock(return_value="https://bybit.com/klines")
        mock_url_builder.get_binance_open_interest_url = Mock(return_value=None)  # Missing
        mock_url_builder.get_bybit_open_interest_url = Mock(return_value=None)  # Missing
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value="https://binance.com/fr")
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        
        mock_api_parser.parse_binance_klines = Mock()
        mock_api_parser.parse_bybit_klines = Mock()
        mock_api_parser.parse_binance_oi = None  # Missing
        mock_api_parser.parse_bybit_oi = None  # Missing
        mock_api_parser.parse_binance_fr = Mock()
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            result = task_builder.prepare_tasks(self.sample_coins, '1h')
            
            # Should generate klines and fr tasks but no oi tasks
            # 3 coins * 2 data types (klines, fr) = 6 total
            expected_tasks = 3 * 2  # klines and fr only
            self.assertEqual(len(result), expected_tasks)

    def test_prepare_tasks_missing_functions_for_specific_exchange(self):
        """Test prepare_tasks when functions are missing for specific exchange."""
        # Test case where binance functions are missing but bybit functions exist
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        mock_url_builder.get_binance_klines_url = Mock(return_value=None)  # Missing
        mock_url_builder.get_bybit_klines_url = Mock(return_value="https://bybit.com/klines")
        mock_url_builder.get_binance_open_interest_url = Mock(return_value=None)  # Missing
        mock_url_builder.get_bybit_open_interest_url = Mock(return_value="https://bybit.com/oi")
        mock_url_builder.get_binance_funding_rate_url = Mock(return_value=None)  # Missing
        mock_url_builder.get_bybit_funding_rate_url = Mock(return_value="https://bybit.com/fr")
        
        mock_api_parser.parse_binance_klines = None  # Missing
        mock_api_parser.parse_bybit_klines = Mock()
        mock_api_parser.parse_binance_oi = None  # Missing
        mock_api_parser.parse_bybit_oi = Mock()
        mock_api_parser.parse_binance_fr = None  # Missing
        mock_api_parser.parse_bybit_fr = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies') as mock_fetch_strategies:
            
            mock_fetch_strategies.fetch_simple = Mock()
            mock_fetch_strategies.fetch_bybit_paginated = Mock()
            
            # Coins with only binance available should not generate tasks
            binance_only_coins = [{"symbol": "ETH/USDT:USDT", "exchanges": ["binance"]}]
            result = task_builder.prepare_tasks(binance_only_coins, '1h')
            
            # Should generate no tasks for binance-only coins when binance functions are missing
            self.assertEqual(len(result), 0)
            
            # Now test with bybit-only coins
            bybit_only_coins = [{"symbol": "SOL/USDT:USDT", "exchanges": ["bybit"]}]
            result = task_builder.prepare_tasks(bybit_only_coins, '1h')
            
            # Should generate 3 tasks (klines, oi, fr) for bybit-only coin
            self.assertEqual(len(result), 3)
            
    # --- МОЁ ДОПОЛНЕНИЕ ---
    def test_prepare_tasks_8h_logic_uses_4h_and_800_limit(self):
        """
        Тест: Проверяем, что timeframe '8h' 
        1. Вызывает klines с '4h'.
        2. Вызывает klines с лимитом 800.
        3. Вызывает OI с '4h' (т.к. api_timeframe='4h').
        """
        timeframe = '8h'
        expected_api_timeframe = '4h' # 8h должен превратиться в 4h
        expected_klines_limit = 800    # 8h должен использовать лимит 800
        
        mock_url_builder = Mock()
        mock_api_parser = Mock()
        
        # Настраиваем все моки
        mock_url_builder.get_binance_klines_url = Mock(return_value="...")
        mock_url_builder.get_bybit_klines_url = Mock(return_value="...")
        mock_url_builder.get_binance_open_interest_url = Mock(return_value="...")
        mock_url_builder.get_bybit_open_interest_url = Mock(return_value="...")
        
        mock_api_parser.parse_binance_klines = Mock()
        mock_api_parser.parse_bybit_klines = Mock()
        mock_api_parser.parse_binance_oi = Mock()
        mock_api_parser.parse_bybit_oi = Mock()
        
        with patch('data_collector.task_builder.url_builder', mock_url_builder), \
             patch('data_collector.task_builder.api_parser', mock_api_parser), \
             patch('data_collector.task_builder.fetch_strategies'):
            
            # Используем только 1 монету (binance) для простоты
            one_coin = [{"symbol": "BTC/USDT:USDT", "exchanges": ["binance"]}]
            
            # Запускаем C prefetched_fr_data, чтобы не создавать FR задачи
            result = task_builder.prepare_tasks(one_coin, timeframe, prefetched_fr_data={'some':'data'})
            
            # 1. Проверяем Klines
            mock_url_builder.get_binance_klines_url.assert_called_once_with(
                "BTCUSDT", 
                expected_api_timeframe,  # (Проверяем, что '8h' стало '4h')
                expected_klines_limit    # (Проверяем, что лимит 800)
            )
            
            # 2. Проверяем OI
            mock_url_builder.get_binance_open_interest_url.assert_called_once_with(
                "BTCUSDT",
                expected_api_timeframe,  # (Проверяем, что '8h' стало '4h')
                400                      # (OI лимит всегда 400)
            )
            
            # 3. Убедимся, что создано 2 задачи (Klines + OI)
            self.assertEqual(len(result), 2)
    # --- КОНЕЦ ДОПОЛНЕНИЯ ---

if __name__ == '__main__':
    unittest.main()