"""
Unit tests for data_collector.task_builder module
"""
import pytest
from unittest.mock import patch, MagicMock, call
from data_collector import task_builder


class TestCalculateOiLimit:
    """Tests for _calculate_oi_limit helper function"""

    def test_1h_timeframe_returns_400(self):
        """Test OI limit calculation for 1h timeframe"""
        assert task_builder._calculate_oi_limit('1h') == 400

    def test_4h_timeframe_returns_180(self):
        """Test OI limit calculation for 4h timeframe (720/4)"""
        assert task_builder._calculate_oi_limit('4h') == 180

    def test_8h_timeframe_returns_90(self):
        """Test OI limit calculation for 8h timeframe (720/8)"""
        assert task_builder._calculate_oi_limit('8h') == 90

    def test_12h_timeframe_returns_60(self):
        """Test OI limit calculation for 12h timeframe (720/12)"""
        assert task_builder._calculate_oi_limit('12h') == 60

    def test_1d_timeframe_returns_30(self):
        """Test OI limit calculation for 1d timeframe"""
        assert task_builder._calculate_oi_limit('1d') == 30

    def test_unknown_timeframes_fallback_to_30(self):
        """Test OI limit calculation for unknown timeframes uses default"""
        assert task_builder._calculate_oi_limit('1m') == 30
        assert task_builder._calculate_oi_limit('5m') == 30
        assert task_builder._calculate_oi_limit('unknown') == 30


class TestPrepareFrTasks:
    """Tests for prepare_fr_tasks function"""

    @patch('data_collector.task_builder.logger')
    @patch('data_collector.task_builder.fetch_strategies')
    @patch('data_collector.task_builder.api_parser')
    @patch('data_collector.task_builder.url_builder')
    def test_binance_coin_creates_correct_task(self, mock_url_builder, mock_api_parser,
                                               mock_fetch_strategies, mock_logger):
        """Test FR task creation for Binance coin"""
        mock_url_builder.get_binance_funding_rate_url = MagicMock(
            return_value='https://binance.com/fr'
        )
        mock_api_parser.parse_binance_fr = MagicMock()
        mock_fetch_strategies.fetch_simple = MagicMock()

        coins = [{'symbol': 'BTCUSDT:binance', 'exchanges': ['binance']}]

        tasks = task_builder.prepare_fr_tasks(coins)

        assert len(tasks) == 1
        task = tasks[0]
        assert task['task_info']['symbol'] == 'BTCUSDT'
        assert task['task_info']['exchange'] == 'binance'
        assert task['task_info']['data_type'] == 'fr'
        assert task['timeframe'] == '1h'
        assert task['fetch_strategy'] == mock_fetch_strategies.fetch_simple
        assert task['parser'] == mock_api_parser.parse_binance_fr
        mock_url_builder.get_binance_funding_rate_url.assert_called_once_with('BTCUSDT', 400)

    @patch('data_collector.task_builder.logger')
    @patch('data_collector.task_builder.fetch_strategies')
    @patch('data_collector.task_builder.api_parser')
    @patch('data_collector.task_builder.url_builder')
    def test_bybit_coin_uses_paginated_strategy(self, mock_url_builder, mock_api_parser,
                                                mock_fetch_strategies, mock_logger):
        """Test FR task creation for Bybit coin uses paginated fetch"""
        mock_url_builder.get_bybit_funding_rate_url = MagicMock(
            return_value='https://bybit.com/fr'
        )
        mock_api_parser.parse_bybit_fr = MagicMock()
        mock_fetch_strategies.fetch_bybit_paginated = MagicMock()

        coins = [{'symbol': 'BTCUSDT:bybit', 'exchanges': ['bybit']}]

        tasks = task_builder.prepare_fr_tasks(coins)

        assert len(tasks) == 1
        task = tasks[0]
        assert task['task_info']['exchange'] == 'bybit'
        assert task['fetch_strategy'] == mock_fetch_strategies.fetch_bybit_paginated
        mock_url_builder.get_bybit_funding_rate_url.assert_called_once_with('BTCUSDT', 400)

    @patch('data_collector.task_builder.logger')
    @patch('data_collector.task_builder.url_builder')
    def test_missing_functions_logs_error(self, mock_url_builder, mock_logger):
        """Test error logging when URL/parser functions are missing"""
        mock_url_builder.get_binance_funding_rate_url = None
        mock_url_builder.get_bybit_funding_rate_url = None

        coins = [{'symbol': 'BTCUSDT:binance', 'exchanges': ['binance']}]

        tasks = task_builder.prepare_fr_tasks(coins)

        assert len(tasks) == 0
        mock_logger.error.assert_called()
        error_msg = mock_logger.error.call_args[0][0]
        assert 'get_binance_funding_rate_url' in error_msg

    @patch('data_collector.task_builder.logger')
    @patch('data_collector.task_builder.fetch_strategies')
    @patch('data_collector.task_builder.api_parser')
    @patch('data_collector.task_builder.url_builder')
    def test_multiple_coins_mixed_exchanges(self, mock_url_builder, mock_api_parser,
                                            mock_fetch_strategies, mock_logger):
        """Test FR task creation for multiple coins with different exchanges"""
        mock_url_builder.get_binance_funding_rate_url = MagicMock(
            return_value='https://binance.com/fr'
        )
        mock_url_builder.get_bybit_funding_rate_url = MagicMock(
            return_value='https://bybit.com/fr'
        )
        mock_api_parser.parse_binance_fr = MagicMock()
        mock_api_parser.parse_bybit_fr = MagicMock()
        mock_fetch_strategies.fetch_simple = MagicMock()
        mock_fetch_strategies.fetch_bybit_paginated = MagicMock()

        coins = [
            {'symbol': 'BTCUSDT:binance', 'exchanges': ['binance']},
            {'symbol': 'ETHUSDT:bybit', 'exchanges': ['bybit']},
            {'symbol': 'BNBUSDT:binance', 'exchanges': ['binance']}
        ]

        tasks = task_builder.prepare_fr_tasks(coins)

        assert len(tasks) == 3
        assert mock_url_builder.get_binance_funding_rate_url.call_count == 2
        mock_url_builder.get_bybit_funding_rate_url.assert_called_once()


class TestPrepareTasks:
    """Tests for prepare_tasks function"""

    @patch('data_collector.task_builder.logger')
    @patch('data_collector.task_builder._calculate_oi_limit')
    @patch('data_collector.task_builder.fetch_strategies')
    @patch('data_collector.task_builder.api_parser')
    @patch('data_collector.task_builder.url_builder')
    def test_4h_timeframe_creates_all_task_types(self, mock_url_builder, mock_api_parser,
                                                 mock_fetch_strategies, mock_calculate_oi, mock_logger):
        """Test task creation for 4h timeframe includes klines, oi, fr"""
        mock_url_builder.get_binance_klines_url = MagicMock(
            return_value='https://binance.com/klines'
        )
        mock_url_builder.get_binance_open_interest_url = MagicMock(
            return_value='https://binance.com/oi'
        )
        mock_url_builder.get_binance_funding_rate_url = MagicMock(
            return_value='https://binance.com/fr'
        )

        mock_api_parser.parse_binance_klines = MagicMock()
        mock_api_parser.parse_binance_oi = MagicMock()
        mock_api_parser.parse_binance_fr = MagicMock()

        mock_fetch_strategies.fetch_simple = MagicMock()
        mock_calculate_oi.return_value = 180

        coins = [{'symbol': 'BTCUSDT:binance', 'exchanges': ['binance']}]

        tasks = task_builder.prepare_tasks(coins, '4h')

        assert len(tasks) == 3
        # Verify all task types are present
        data_types = {task['task_info']['data_type'] for task in tasks}
        assert data_types == {'klines', 'oi', 'fr'}
        # Verify timeframe is consistent
        for task in tasks:
            assert task['timeframe'] == '4h'

    @patch('data_collector.task_builder.logger')
    @patch('data_collector.task_builder._calculate_oi_limit')
    @patch('data_collector.task_builder.fetch_strategies')
    @patch('data_collector.task_builder.api_parser')
    @patch('data_collector.task_builder.url_builder')
    def test_8h_timeframe_uses_4h_api(self, mock_url_builder, mock_api_parser,
                                      mock_fetch_strategies, mock_calculate_oi, mock_logger):
        """Test that 8h timeframe uses 4h API calls"""
        mock_url_builder.get_bybit_klines_url = MagicMock(
            return_value='https://bybit.com/klines'
        )
        mock_api_parser.parse_bybit_klines = MagicMock()
        mock_fetch_strategies.fetch_bybit_paginated = MagicMock()
        mock_calculate_oi.return_value = 180

        coins = [{'symbol': 'BTCUSDT:bybit', 'exchanges': ['bybit']}]

        tasks = task_builder.prepare_tasks(coins, '8h')

        # Verify API timeframe is 4h (not 8h)
        mock_url_builder.get_bybit_klines_url.assert_called_once()
        call_args = mock_url_builder.get_bybit_klines_url.call_args
        assert call_args[0][1] == '4h'  # timeframe argument

    @patch('data_collector.task_builder.logger')
    @patch('data_collector.task_builder._calculate_oi_limit')
    @patch('data_collector.task_builder.fetch_strategies')
    @patch('data_collector.task_builder.api_parser')
    @patch('data_collector.task_builder.url_builder')
    def test_klines_limit_for_different_timeframes(self, mock_url_builder, mock_api_parser,
                                                   mock_fetch_strategies, mock_calculate_oi, mock_logger):
        """Test klines limit is 800 for 4h/8h and 400 otherwise"""
        mock_url_builder.get_binance_klines_url = MagicMock(
            return_value='https://binance.com/klines'
        )
        mock_api_parser.parse_binance_klines = MagicMock()
        mock_fetch_strategies.fetch_simple = MagicMock()

        coins = [{'symbol': 'BTCUSDT:binance', 'exchanges': ['binance']}]

        # Test 4h timeframe (should use 800)
        task_builder.prepare_tasks(coins, '4h')
        call_args = mock_url_builder.get_binance_klines_url.call_args
        assert call_args[0][2] == 800  # limit argument

        # Reset mock
        mock_url_builder.get_binance_klines_url.reset_mock()

        # Test 1h timeframe (should use 400)
        task_builder.prepare_tasks(coins, '1h')
        call_args = mock_url_builder.get_binance_klines_url.call_args
        assert call_args[0][2] == 400  # limit argument

    @patch('data_collector.task_builder.logger')
    @patch('data_collector.task_builder._calculate_oi_limit')
    @patch('data_collector.task_builder.fetch_strategies')
    @patch('data_collector.task_builder.api_parser')
    @patch('data_collector.task_builder.url_builder')
    def test_prefetched_fr_skips_fr_task_creation(self, mock_url_builder, mock_api_parser,
                                                  mock_fetch_strategies, mock_calculate_oi, mock_logger):
        """Test FR tasks are skipped when prefetched data is provided"""
        mock_url_builder.get_binance_klines_url = MagicMock(
            return_value='https://binance.com/klines'
        )
        mock_url_builder.get_binance_open_interest_url = MagicMock(
            return_value='https://binance.com/oi'
        )
        # Note: NOT mocking funding rate URL since it should NOT be called

        mock_api_parser.parse_binance_klines = MagicMock()
        mock_api_parser.parse_binance_oi = MagicMock()

        mock_fetch_strategies.fetch_simple = MagicMock()
        mock_calculate_oi.return_value = 180

        coins = [{'symbol': 'BTCUSDT:binance', 'exchanges': ['binance']}]
        prefetched_fr = {'some': 'data'}

        tasks = task_builder.prepare_tasks(coins, '4h', prefetched_fr_data=prefetched_fr)

        # Should have only klines and oi (no FR)
        assert len(tasks) == 2
        data_types = {task['task_info']['data_type'] for task in tasks}
        assert data_types == {'klines', 'oi'}
        mock_url_builder.get_binance_funding_rate_url.assert_not_called()

    @patch('data_collector.task_builder.logger')
    @patch('data_collector.task_builder._calculate_oi_limit')
    @patch('data_collector.task_builder.fetch_strategies')
    @patch('data_collector.task_builder.api_parser')
    @patch('data_collector.task_builder.url_builder')
    def test_missing_klines_functions_skips_klines(self, mock_url_builder, mock_api_parser,
                                                   mock_fetch_strategies, mock_calculate_oi, mock_logger):
        """Test that missing klines URL/parser functions are logged and skipped"""
        mock_url_builder.get_binance_klines_url = None  # Missing function
        mock_url_builder.get_binance_open_interest_url = MagicMock(
            return_value='https://binance.com/oi'
        )
        mock_url_builder.get_binance_funding_rate_url = MagicMock(
            return_value='https://binance.com/fr'
        )

        mock_api_parser.parse_binance_oi = MagicMock()
        mock_api_parser.parse_binance_fr = MagicMock()

        mock_fetch_strategies.fetch_simple = MagicMock()
        mock_calculate_oi.return_value = 180

        coins = [{'symbol': 'BTCUSDT:binance', 'exchanges': ['binance']}]

        tasks = task_builder.prepare_tasks(coins, '4h')

        # Should have only oi and fr (no klines)
        assert len(tasks) == 2
        data_types = {task['task_info']['data_type'] for task in tasks}
        assert data_types == {'oi', 'fr'}
        # Check that the error was logged for missing klines functions
        expected_error_msg = "[4H_TASK_BUILDER] Не найден url/parser klines для binance (BTCUSDT)"
        mock_logger.error.assert_called_with(expected_error_msg)

    @patch('data_collector.task_builder.logger')
    @patch('data_collector.task_builder._calculate_oi_limit')
    @patch('data_collector.task_builder.fetch_strategies')
    @patch('data_collector.task_builder.api_parser')
    @patch('data_collector.task_builder.url_builder')
    def test_multiple_coins_all_tasks_created(self, mock_url_builder, mock_api_parser,
                                              mock_fetch_strategies, mock_calculate_oi, mock_logger):
        """Test task creation for multiple coins includes all types for each coin"""
        mock_url_builder.get_binance_klines_url = MagicMock(return_value='kline_url')
        mock_url_builder.get_binance_open_interest_url = MagicMock(return_value='oi_url')
        mock_url_builder.get_binance_funding_rate_url = MagicMock(return_value='fr_url')

        mock_api_parser.parse_binance_klines = MagicMock()
        mock_api_parser.parse_binance_oi = MagicMock()
        mock_api_parser.parse_binance_fr = MagicMock()

        mock_fetch_strategies.fetch_simple = MagicMock()
        mock_calculate_oi.return_value = 180

        coins = [
            {'symbol': 'BTCUSDT:binance', 'exchanges': ['binance']},
            {'symbol': 'ETHUSDT:binance', 'exchanges': ['binance']}
        ]

        tasks = task_builder.prepare_tasks(coins, '4h')

        # Should have 3 task types * 2 coins = 6 tasks
        assert len(tasks) == 6
        # Group tasks by symbol
        tasks_by_symbol = {}
        for task in tasks:
            sym = task['task_info']['symbol']
            if sym not in tasks_by_symbol:
                tasks_by_symbol[sym] = set()
            tasks_by_symbol[sym].add(task['task_info']['data_type'])

        assert 'BTCUSDT' in tasks_by_symbol
        assert 'ETHUSDT' in tasks_by_symbol
        assert tasks_by_symbol['BTCUSDT'] == {'klines', 'oi', 'fr'}
        assert tasks_by_symbol['ETHUSDT'] == {'klines', 'oi', 'fr'}

    @patch('data_collector.task_builder.logger')
    @patch('data_collector.task_builder._calculate_oi_limit')
    @patch('data_collector.task_builder.fetch_strategies')
    @patch('data_collector.task_builder.api_parser')
    @patch('data_collector.task_builder.url_builder')
    def test_mixed_exchanges_tasks_created_correctly(self, mock_url_builder, mock_api_parser,
                                                     mock_fetch_strategies, mock_calculate_oi, mock_logger):
        """Test task creation for coins on different exchanges"""
        # Binance
        mock_url_builder.get_binance_klines_url = MagicMock(return_value='binance_kline_url')
        mock_url_builder.get_binance_open_interest_url = MagicMock(return_value='binance_oi_url')
        mock_url_builder.get_binance_funding_rate_url = MagicMock(return_value='binance_fr_url')
        # Bybit
        mock_url_builder.get_bybit_klines_url = MagicMock(return_value='bybit_kline_url')
        mock_url_builder.get_bybit_open_interest_url = MagicMock(return_value='bybit_oi_url')
        mock_url_builder.get_bybit_funding_rate_url = MagicMock(return_value='bybit_fr_url')

        mock_api_parser.parse_binance_klines = MagicMock()
        mock_api_parser.parse_binance_oi = MagicMock()
        mock_api_parser.parse_binance_fr = MagicMock()
        mock_api_parser.parse_bybit_klines = MagicMock()
        mock_api_parser.parse_bybit_oi = MagicMock()
        mock_api_parser.parse_bybit_fr = MagicMock()

        mock_fetch_strategies.fetch_simple = MagicMock()  # For Binance
        mock_fetch_strategies.fetch_bybit_paginated = MagicMock()  # For Bybit
        mock_calculate_oi.return_value = 180

        coins = [
            {'symbol': 'BTCUSDT:binance', 'exchanges': ['binance']},
            {'symbol': 'ETHUSDT:bybit', 'exchanges': ['bybit']}
        ]

        tasks = task_builder.prepare_tasks(coins, '4h')

        assert len(tasks) == 6  # 3 types * 2 coins
        # Check exchanges
        exchanges = {task['task_info']['exchange'] for task in tasks}
        assert exchanges == {'binance', 'bybit'}

        # Check strategies
        strategies = {task['fetch_strategy'] for task in tasks}
        assert mock_fetch_strategies.fetch_simple in strategies
        assert mock_fetch_strategies.fetch_bybit_paginated in strategies