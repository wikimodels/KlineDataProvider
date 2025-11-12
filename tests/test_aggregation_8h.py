# tests/test_aggregation_8h.py
"""
Обширные юнит-тесты для модуля data_collector.aggregation_8h
"""
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock, call
from data_collector import aggregation_8h


class TestIs8hCloseTimeMs:
    """Тесты для функции _is_8h_close_time_ms"""

    def test_on_8h_boundary_00(self):
        """Тест на границе 00:00 UTC"""
        # 1704067200000 - 01.01.2024 00:00:00 UTC
        assert aggregation_8h._is_8h_close_time_ms(1704067200000 - 1) is True

    def test_on_8h_boundary_08(self):
        """Тест на границе 08:00 UTC"""
        # 1704096000000 - 01.01.2024 08:00:00 UTC
        assert aggregation_8h._is_8h_close_time_ms(1704096000000 - 1) is True

    def test_on_8h_boundary_16(self):
        """Тест на границе 16:00 UTC"""
        # 1704124800000 - 01.01.2024 16:00:00 UTC
        assert aggregation_8h._is_8h_close_time_ms(1704124800000 - 1) is True

    def test_not_on_8h_boundary(self):
        """Тест не на границе 8h"""
        # 1704074400000 - 01.01.2024 02:00:00 UTC
        assert aggregation_8h._is_8h_close_time_ms(1704074400000 - 1) is False
        # 1704081600000 - 01.01.2024 04:00:00 UTC
        assert aggregation_8h._is_8h_close_time_ms(1704081600000 - 1) is False

    def test_edge_cases(self):
        """Тест краевых случаев"""
        # В оригинальной функции (close_time_ms + 1) % eight_hours_ms == 0
        # Если close_time_ms = -1, то (-1 + 1) = 0, 0 % N == 0 -> True
        # Это поведение оригинальной функции.
        assert aggregation_8h._is_8h_close_time_ms(-1) is True
        # close_time_ms = 0: (0 + 1) = 1, 1 % 28800000 != 0 -> False
        assert aggregation_8h._is_8h_close_time_ms(0) is False
        # close_time_ms = 28799999: (28799999 + 1) = 28800000, 28800000 % 28800000 == 0 -> True
        assert aggregation_8h._is_8h_close_time_ms(28800000 - 1) is True  # 07:59:59.999 UTC
        # close_time_ms = 57599999: (57599999 + 1) = 57600000, 57600000 % 28800000 == 0 -> True
        assert aggregation_8h._is_8h_close_time_ms(28800000 * 2 - 1) is True  # 15:59:59.999 UTC


class TestAggregateKlines4hTo8h:
    """Тесты для функции _aggregate_klines_4h_to_8h"""

    def test_successful_aggregation(self):
        """Тест успешной агрегации двух свечей"""
        candle1 = {
            "openPrice": 100.0,
            "highPrice": 105.0,
            "lowPrice": 99.0,
            "closePrice": 103.0,
            "volume": 100.0,
            "volumeDelta": 10.0,
            "openTime": 1704067200000,  # 00:00
            "closeTime": 1704081600000 - 1  # 03:59:59.999
        }
        candle2 = {
            "openPrice": 103.0,
            "highPrice": 107.0,
            "lowPrice": 102.0,
            "closePrice": 106.0,
            "volume": 150.0,
            "volumeDelta": 15.0,
            "openTime": 1704081600000,  # 04:00
            "closeTime": 1704096000000 - 1  # 07:59:59.999
        }

        result = aggregation_8h._aggregate_klines_4h_to_8h(candle1, candle2)

        expected = {
            "openPrice": 100.0,
            "highPrice": 107.0,
            "lowPrice": 99.0,
            "closePrice": 106.0,
            "volume": 250.0,
            "volumeDelta": 25.0,
            "openTime": 1704067200000,
            "closeTime": 1704096000000 - 1
        }
        assert result == expected

    def test_missing_fields_candle1(self):
        """Тест, когда в первой свече нет обязательных полей"""
        candle1 = {"highPrice": 105.0, "lowPrice": 99.0}  # Нет openPrice
        candle2 = {"closePrice": 106.0, "highPrice": 107.0, "lowPrice": 102.0, "closeTime": 1704096000000 - 1}
        result = aggregation_8h._aggregate_klines_4h_to_8h(candle1, candle2)
        assert result is None

    def test_missing_fields_candle2(self):
        """Тест, когда во второй свече нет обязательных полей"""
        candle1 = {"openPrice": 100.0, "highPrice": 105.0, "lowPrice": 99.0, "closePrice": 103.0}
        candle2 = {"highPrice": 107.0, "lowPrice": 102.0}  # Нет closePrice
        result = aggregation_8h._aggregate_klines_4h_to_8h(candle1, candle2)
        assert result is None

    def test_none_values(self):
        """Тест с None значениями в обязательных полях"""
        candle1 = {"openPrice": 100.0, "highPrice": None, "lowPrice": 99.0, "closePrice": 103.0}
        candle2 = {"closePrice": 106.0, "highPrice": 107.0, "lowPrice": 102.0, "closeTime": 1704096000000 - 1}
        result = aggregation_8h._aggregate_klines_4h_to_8h(candle1, candle2)
        assert result is None


class TestAggregateOi4hTo8h:
    """Тесты для функции _aggregate_oi_4h_to_8h"""

    def test_successful_aggregation(self):
        """Тест успешной агрегации OI"""
        candle1 = {"openInterest": 1000.0, "openTime": 1704067200000, "closeTime": 1704081600000 - 1}
        candle2 = {"openInterest": 1200.0, "openTime": 1704081600000, "closeTime": 1704096000000 - 1}

        result = aggregation_8h._aggregate_oi_4h_to_8h(candle1, candle2)

        expected = {
            "openInterest": 1200.0,
            "openTime": 1704067200000,
            "closeTime": 1704096000000 - 1
        }
        assert result == expected

    def test_missing_fields(self):
        """Тест, когда обязательные поля отсутствуют"""
        candle1 = {"openInterest": 1000.0}
        candle2 = {"openTime": 1704081600000, "closeTime": 1704096000000 - 1}
        result = aggregation_8h._aggregate_oi_4h_to_8h(candle1, candle2)
        assert result is None

    def test_none_values(self):
        """Тест с None значениями"""
        candle1 = {"openInterest": 1000.0, "openTime": 1704067200000, "closeTime": 1704081600000 - 1}
        candle2 = {"openInterest": None, "openTime": 1704081600000, "closeTime": 1704096000000 - 1}
        result = aggregation_8h._aggregate_oi_4h_to_8h(candle1, candle2)
        assert result is None


class TestAggregateFundingRates:
    """Тесты для функции _aggregate_funding_rates"""

    def test_fr_from_candle2(self):
        """Тест, когда обе свечи имеют FR, приоритет у candle2"""
        candle1 = {"fundingRate": 0.001, "openTime": 1704067200000, "closeTime": 1704081600000 - 1}
        candle2 = {"fundingRate": 0.002, "openTime": 1704081600000, "closeTime": 1704124800000 - 1}

        result = aggregation_8h._aggregate_funding_rates(candle1, candle2)

        expected = {
            "fundingRate": 0.002,
            "openTime": 1704067200000,
            "closeTime": 1704124800000 - 1
        }
        assert result == expected

    def test_fr_from_candle1_fallback(self):
        """Тест, когда candle2 не имеет FR, используем candle1"""
        candle1 = {"fundingRate": 0.001, "openTime": 1704067200000, "closeTime": 1704081600000 - 1}
        candle2 = {"openTime": 1704081600000, "closeTime": 1704096000000 - 1}  # Нет fundingRate

        result = aggregation_8h._aggregate_funding_rates(candle1, candle2)

        expected = {
            "fundingRate": 0.001,
            "openTime": 1704067200000,
            "closeTime": 1704096000000 - 1
        }
        assert result == expected

    def test_no_fr_anywhere(self):
        """Тест, когда ни одна свеча не имеет FR"""
        candle1 = {"openTime": 1704067200000, "closeTime": 1704081600000 - 1}
        candle2 = {"openTime": 1704081600000, "closeTime": 1704096000000 - 1}
        result = aggregation_8h._aggregate_funding_rates(candle1, candle2)
        assert result is None

    def test_missing_time_fields(self):
        """Тест, когда нет временных полей"""
        candle1 = {"fundingRate": 0.001}
        candle2 = {"fundingRate": 0.002}
        result = aggregation_8h._aggregate_funding_rates(candle1, candle2)
        assert result is None


class TestBuild8hCandlesFromEnd:
    """Тесты для функции _build_8h_candles_from_end"""

    @patch('data_collector.aggregation_8h.get_interval_duration_ms')
    def test_empty_input(self, mock_get_interval):
        """Тест с пустым списком свечей"""
        mock_get_interval.return_value = 14400000  # 4h
        result = aggregation_8h._build_8h_candles_from_end([], 'klines', 'BTCUSDT')
        assert result == []
        assert len(result) == 0

    @patch('data_collector.aggregation_8h.get_interval_duration_ms')
    def test_single_candle(self, mock_get_interval):
        """Тест с одной свечей"""
        mock_get_interval.return_value = 14400000  # 4h
        candles = [{"openTime": 0, "closeTime": 14400000 - 1, "openPrice": 100}]
        result = aggregation_8h._build_8h_candles_from_end(candles, 'klines', 'BTCUSDT')
        assert result == []  # Нужна пара
        assert len(result) == 0

    @patch('data_collector.aggregation_8h.get_interval_duration_ms')
    @patch('data_collector.aggregation_8h._is_8h_close_time_ms')
    def test_simple_aggregation_klines(self, mock_is_8h, mock_get_interval):
        """Тест простой агрегации для klines"""
        mock_get_interval.return_value = 14400000  # 4h
        mock_is_8h.side_effect = lambda x: x == (1704096000000 - 1)  # Только 07:59:59.999
        candles = [
            {"openTime": 1704067200000, "closeTime": 1704081600000 - 1,
             "openPrice": 100, "highPrice": 105, "lowPrice": 99,
             "closePrice": 103, "volume": 100, "volumeDelta": 10},
            {"openTime": 1704081600000, "closeTime": 1704096000000 - 1,
             "openPrice": 103, "highPrice": 107, "lowPrice": 102,
             "closePrice": 106, "volume": 150, "volumeDelta": 15}
        ]
        result = aggregation_8h._build_8h_candles_from_end(candles, 'klines', 'BTCUSDT')
        assert len(result) == 1
        assert result[0]['openPrice'] == 100
        assert result[0]['closePrice'] == 106
        assert result[0]['volume'] == 250

    @patch('data_collector.aggregation_8h.get_interval_duration_ms')
    @patch('data_collector.aggregation_8h._is_8h_close_time_ms')
    def test_simple_aggregation_oi(self, mock_is_8h, mock_get_interval):
        """Тест простой агрегации для oi"""
        mock_get_interval.return_value = 14400000  # 4h
        mock_is_8h.side_effect = lambda x: x == (1704096000000 - 1)
        candles = [
            {"openTime": 1704067200000, "closeTime": 1704081600000 - 1,
             "openInterest": 1000},
            {"openTime": 1704081600000, "closeTime": 1704096000000 - 1,
             "openInterest": 1200}
        ]
        result = aggregation_8h._build_8h_candles_from_end(candles, 'oi', 'BTCUSDT')
        assert len(result) == 1
        assert result[0]['openInterest'] == 1200

    @patch('data_collector.aggregation_8h.get_interval_duration_ms')
    @patch('data_collector.aggregation_8h._is_8h_close_time_ms')
    def test_simple_aggregation_fr(self, mock_is_8h, mock_get_interval):
        """Тест простой агрегации для fr"""
        mock_get_interval.return_value = 14400000  # 4h
        mock_is_8h.side_effect = lambda x: x == (1704096000000 - 1)
        candles = [
            {"openTime": 1704067200000, "closeTime": 1704081600000 - 1,
             "fundingRate": 0.001},
            {"openTime": 1704081600000, "closeTime": 1704096000000 - 1,
             "fundingRate": 0.002}
        ]
        result = aggregation_8h._build_8h_candles_from_end(candles, 'fr', 'BTCUSDT')
        assert len(result) == 1
        assert result[0]['fundingRate'] == 0.002

    @patch('data_collector.aggregation_8h.get_interval_duration_ms')
    @patch('data_collector.aggregation_8h._is_8h_close_time_ms')
    def test_multiple_8h_candles(self, mock_is_8h, mock_get_interval):
        """Тест построения нескольких 8h свечей из длинного списка"""
        mock_get_interval.return_value = 14400000  # 4h
        mock_is_8h.side_effect = lambda x: x in [1704096000000 - 1,
                                                    1704124800000 - 1,
                                                    1704153600000 - 1]  # 07:59, 15:59, 23:59
        
        # Добавляем полные данные для успешной агрегации
        candles = [
            {"openTime": 1704067200000, "closeTime": 1704081600000 - 1,
             "openPrice": 100, "closePrice": 101, "highPrice": 105, "lowPrice": 99,
             "volume": 100, "volumeDelta": 10},  # 0 (00-03)
            {"openTime": 1704081600000, "closeTime": 1704096000000 - 1,
             "openPrice": 101, "closePrice": 102, "highPrice": 107, "lowPrice": 100,
             "volume": 110, "volumeDelta": 11},  # 1 (04-07) - Якорь 1
            {"openTime": 1704096000000, "closeTime": 1704110400000 - 1,
             "openPrice": 102, "closePrice": 103, "highPrice": 108, "lowPrice": 101,
             "volume": 120, "volumeDelta": 12},  # 2 (08-11)
            {"openTime": 1704110400000, "closeTime": 1704124800000 - 1,
             "openPrice": 103, "closePrice": 104, "highPrice": 109, "lowPrice": 102,
             "volume": 130, "volumeDelta": 13},  # 3 (12-15) - Якорь 2
            {"openTime": 1704124800000, "closeTime": 1704139200000 - 1,
             "openPrice": 104, "closePrice": 105, "highPrice": 110, "lowPrice": 103,
             "volume": 140, "volumeDelta": 14},  # 4 (16-19)
            {"openTime": 1704139200000, "closeTime": 1704153600000 - 1,
             "openPrice": 105, "closePrice": 106, "highPrice": 111, "lowPrice": 104,
             "volume": 150, "volumeDelta": 15},  # 5 (20-23) - Якорь 3
        ]
        result = aggregation_8h._build_8h_candles_from_end(candles, 'klines', 'BTCUSDT')
        # Ожидаем 3 свечи: (0+1), (2+3), (4+5)
        assert len(result) == 3
        # Проверяем первую свечу (из c0 и c1)
        assert result[0]["openPrice"] == 100
        assert result[0]["closePrice"] == 102
        # Проверяем последнюю свечу (из c4 и c5)
        assert result[2]["openPrice"] == 104
        assert result[2]["closePrice"] == 106

    @patch('data_collector.aggregation_8h.get_interval_duration_ms')
    @patch('data_collector.aggregation_8h._is_8h_close_time_ms')
    def test_non_adjacent_candles(self, mock_is_8h, mock_get_interval):
        """Тест с разрывом между свечами - должна пропустить и искать дальше"""
        mock_get_interval.return_value = 14400000  # 4h
        mock_is_8h.side_effect = lambda x: x in [1704096000000 - 1,
                                                    1704153600000 - 1]  # 07:59 и 23:59
        
        candles = [
            {"openTime": 1704067200000, "closeTime": 1704081600000 - 1,
             "openPrice": 100, "closePrice": 101, "highPrice": 105, "lowPrice": 99,
             "volume": 100, "volumeDelta": 10},  # 0 (00-03)
            {"openTime": 1704081600000, "closeTime": 1704096000000 - 1,
             "openPrice": 101, "closePrice": 102, "highPrice": 107, "lowPrice": 100,
             "volume": 110, "volumeDelta": 11},  # 1 (04-07) - Якорь 1
            {"openTime": 1704110400000, "closeTime": 1704124800000 - 1,
             "openPrice": 103, "closePrice": 104, "highPrice": 109, "lowPrice": 102,
             "volume": 130, "volumeDelta": 13},  # 2 (12-15) - разрыв 4 часа
            {"openTime": 1704124800000, "closeTime": 1704139200000 - 1,
             "openPrice": 104, "closePrice": 105, "highPrice": 110, "lowPrice": 103,
             "volume": 140, "volumeDelta": 14},  # 3 (16-19) - смежная с c4
            {"openTime": 1704139200000, "closeTime": 1704153600000 - 1,
             "openPrice": 105, "closePrice": 106, "highPrice": 111, "lowPrice": 104,
             "volume": 150, "volumeDelta": 15},  # 4 (20-23) - Якорь 2
        ]
        result = aggregation_8h._build_8h_candles_from_end(candles, 'klines', 'BTCUSDT')
        # Ожидаем 2 свечи: (0+1) и (3+4), c2 пропущена из-за разрыва
        assert len(result) == 2
        # Первая в результе (после реверса) - из c0 и c1
        assert result[0]["openPrice"] == 100
        assert result[0]["closePrice"] == 102
        # Вторая в результе - из c3 и c4
        assert result[1]["openPrice"] == 104
        assert result[1]["closePrice"] == 106

    @patch('data_collector.aggregation_8h.get_interval_duration_ms')
    @patch('data_collector.aggregation_8h._is_8h_close_time_ms')
    def test_no_8h_anchor_found(self, mock_is_8h, mock_get_interval):
        """Тест, когда не находится якорной 8h свечи"""
        mock_get_interval.return_value = 14400000  # 4h
        mock_is_8h.return_value = False
        candles = [
            {"openTime": 1704067200000, "closeTime": 1704081600000 - 1, "openPrice": 100, "closePrice": 101},
            {"openTime": 1704081600000, "closeTime": 1704096000000 - 1, "openPrice": 101, "closePrice": 102}
        ]
        result = aggregation_8h._build_8h_candles_from_end(candles, 'klines', 'BTCUSDT')
        assert result == []  # Якорь не найден

    @patch('data_collector.aggregation_8h.get_interval_duration_ms')
    def test_invalid_data_type(self, mock_get_interval):
        """Тест с недопустимым типом данных"""
        mock_get_interval.return_value = 14400000  # 4h
        candles = [
            {"openTime": 1704067200000, "closeTime": 1704081600000 - 1, "openPrice": 100, "closePrice": 101},
            {"openTime": 1704081600000, "closeTime": 1704096000000 - 1, "openPrice": 101, "closePrice": 102}
        ]
        result = aggregation_8h._build_8h_candles_from_end(candles, 'invalid_type', 'BTCUSDT')
        assert result == []  # aggregate_func будет None


class TestGenerateAndSave8hCache:
    """Тесты для функции generate_and_save_8h_cache"""

    @patch('data_collector.aggregation_8h.save_to_cache')
    @patch('data_collector.aggregation_8h.format_final_structure')
    @patch('data_collector.aggregation_8h.merge_data')
    @patch('data_collector.aggregation_8h._build_8h_candles_from_end')
    @patch('data_collector.aggregation_8h.logger')
    async def test_full_flow_success(self, mock_logger, mock_build, mock_merge, mock_format, mock_save):
        """Тест полного успешного цикла"""
        # Подготовка данных — ТЕПЕРЬ 2 СВЕЧИ НА КАЖДЫЙ ТИП (иначе guard condition сработает)
        data_4h = {
            'BTCUSDT': {
                'klines': [
                    {'openTime': 0, 'closeTime': 14400000 - 1, 'openPrice': 100, 'closePrice': 101},
                    {'openTime': 14400000, 'closeTime': 28800000 - 1, 'openPrice': 101, 'closePrice': 102}
                ],
                'oi': [
                    {'openTime': 0, 'closeTime': 14400000 - 1, 'openInterest': 1000},
                    {'openTime': 14400000, 'closeTime': 28800000 - 1, 'openInterest': 1200}
                ],
                'fr': [
                    {'openTime': 0, 'closeTime': 14400000 - 1, 'fundingRate': 0.001},
                    {'openTime': 14400000, 'closeTime': 28800000 - 1, 'fundingRate': 0.002}
                ]
            },
            'ETHUSDT': {
                'klines': [
                    {'openTime': 0, 'closeTime': 14400000 - 1, 'openPrice': 200, 'closePrice': 201},
                    {'openTime': 14400000, 'closeTime': 28800000 - 1, 'openPrice': 201, 'closePrice': 202}
                ],
                'oi': [
                    {'openTime': 0, 'closeTime': 14400000 - 1, 'openInterest': 2000},
                    {'openTime': 14400000, 'closeTime': 28800000 - 1, 'openInterest': 2200}
                ],
                'fr': [
                    {'openTime': 0, 'closeTime': 14400000 - 1, 'fundingRate': 0.002},
                    {'openTime': 14400000, 'closeTime': 28800000 - 1, 'fundingRate': 0.003}
                ]
            }
        }
        coins_from_api = [{'symbol': 'BTCUSDT'}, {'symbol': 'ETHUSDT'}]

        # Моки - возвращаем непустые списки для klines, чтобы монеты не пропускались
        mock_build.side_effect = [
            [{'openTime': 0, 'closeTime': 28800000 - 1,
              'openPrice': 100, 'closePrice': 102}],  # BTC klines
            [{'openTime': 0, 'closeTime': 28800000 - 1,
              'openInterest': 1000}],  # BTC oi
            [{'openTime': 0, 'closeTime': 28800000 - 1,
              'fundingRate': 0.001}],  # BTC fr
            [{'openTime': 0, 'closeTime': 28800000 - 1,
              'openPrice': 200, 'closePrice': 202}],  # ETH klines
            [{'openTime': 0, 'closeTime': 28800000 - 1,
              'openInterest': 2000}],  # ETH oi
            [{'openTime': 0, 'closeTime': 28800000 - 1,
              'fundingRate': 0.002}],  # ETH fr
        ]

        # merge_data должна вернуть данные в правильной структуре
        mock_merge.return_value = {
            'BTCUSDT': {
                'klines': [{'openTime': 0, 'closeTime': 28800000 - 1,
                            'openPrice': 100, 'closePrice': 102}],
                'oi': [{'openTime': 0, 'closeTime': 28800000 - 1,
                        'openInterest': 1000}],
                'fr': [{'openTime': 0, 'closeTime': 28800000 - 1,
                        'fundingRate': 0.001}]
            },
            'ETHUSDT': {
                'klines': [{'openTime': 0, 'closeTime': 28800000 - 1,
                            'openPrice': 200, 'closePrice': 202}],
                'oi': [{'openTime': 0, 'closeTime': 28800000 - 1,
                        'openInterest': 2000}],
                'fr': [{'openTime': 0, 'closeTime': 28800000 - 1,
                        'fundingRate': 0.002}]
            }
        }

        mock_format.return_value = {'data': []}

        await aggregation_8h.generate_and_save_8h_cache(data_4h, coins_from_api)

        # Проверки — ТЕПЕРЬ ОЖИДАЕМ 6 ВЫЗОВОВ (2 монеты * 3 типа)
        assert mock_build.call_count == 6
        
        # Проверяем, что вызывалось для каждой монеты и каждого типа
        expected_calls = [
            call(data_4h['BTCUSDT']['klines'], 'klines', 'BTCUSDT'),
            call(data_4h['BTCUSDT']['oi'], 'oi', 'BTCUSDT'),
            call(data_4h['BTCUSDT']['fr'], 'fr', 'BTCUSDT'),
            call(data_4h['ETHUSDT']['klines'], 'klines', 'ETHUSDT'),
            call(data_4h['ETHUSDT']['oi'], 'oi', 'ETHUSDT'),
            call(data_4h['ETHUSDT']['fr'], 'fr', 'ETHUSDT'),
        ]
        mock_build.assert_has_calls(expected_calls, any_order=True)
        
        assert mock_merge.call_count == 1
        assert mock_format.call_count == 1
        assert mock_save.call_count == 1

    @patch('data_collector.aggregation_8h.save_to_cache')
    @patch('data_collector.aggregation_8h.format_final_structure')
    @patch('data_collector.aggregation_8h.merge_data')
    @patch('data_collector.aggregation_8h._build_8h_candles_from_end')
    @patch('data_collector.aggregation_8h.logger')
    async def test_empty_data_4h(self, mock_logger, mock_build, mock_merge, mock_format, mock_save):
        """Тест с пустыми 4h данными"""
        data_4h = {}
        coins_from_api = []

        await aggregation_8h.generate_and_save_8h_cache(data_4h, coins_from_api)

        # Проверки
        assert mock_logger.warning.called
        assert not mock_build.called  # Цикл не должен запускаться
        assert not mock_merge.called
        assert not mock_format.called
        assert not mock_save.called

    @patch('data_collector.aggregation_8h.save_to_cache')
    @patch('data_collector.aggregation_8h.format_final_structure')
    @patch('data_collector.aggregation_8h.merge_data')
    @patch('data_collector.aggregation_8h._build_8h_candles_from_end')
    @patch('data_collector.aggregation_8h.logger')
    async def test_no_klines_for_symbol(self, mock_logger, mock_build, mock_merge, mock_format, mock_save):
        """Тест, когда для символа нет 4h klines (пустой список)"""
        # Пустой список klines -> guard condition срабатывает -> _build_8h_candles_from_end НЕ вызывается
        data_4h = {
            'BTCUSDT': {
                'klines': [],  # Пустые klines -> монета пропускается
                'oi': [{'openTime': 0, 'closeTime': 14400000 - 1, 'openInterest': 1000}],
                'fr': [{'openTime': 0, 'closeTime': 14400000 - 1, 'fundingRate': 0.001}]
            }
        }
        coins_from_api = [{'symbol': 'BTCUSDT'}]

        await aggregation_8h.generate_and_save_8h_cache(data_4h, coins_from_api)

        # Проверки — _build_8h_candles_from_end НЕ должен вызываться
        assert mock_build.call_count == 0  # Пустой klines -> пропускаем всё
        
        # Проверяем, что логирование сработало
        assert mock_logger.debug.called or mock_logger.info.called
        
        # Проверяем, что downstream не вызывались
        assert not mock_merge.called
        assert not mock_format.called
        assert not mock_save.called