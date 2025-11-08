import unittest
from unittest.mock import patch
from data_collector.aggregation_8h import (
    _aggregate_4h_to_8h, 
    _aggregate_klines_4h_to_8h, 
    _aggregate_oi_4h_to_8h
)

# Примеры 4h свечей для тестов
def make_candle(open_time, **kwargs):
    candle = {
        "openTime": open_time,
        "closeTime": open_time + 4 * 3600 * 1000 - 1,
        "openPrice": 100.0,
        "highPrice": 110.0,
        "lowPrice": 90.0,
        "closePrice": 105.0,
        "volume": 1000.0,
        "volumeDelta": 100.0,
        "openInterest": 5000.0,
        "fundingRate": 0.001
    }
    candle.update(kwargs)
    return candle


class TestAggregate4hTo8h(unittest.TestCase):

    # --- (ИЗМЕНЕНИЕ) Исправлен путь: 'aggregation_8h' -> 'data_collector.aggregation_8h' ---
    @patch('data_collector.aggregation_8h.get_interval_duration_ms', return_value=4 * 3600 * 1000)
    def test_empty_input(self, mock_get_interval):
        result = _aggregate_4h_to_8h([], 'klines')
        self.assertEqual(result, [])

    # --- (ИЗМЕНЕНИЕ) Исправлен путь: 'aggregation_8h' -> 'data_collector.aggregation_8h' ---
    @patch('data_collector.aggregation_8h.get_interval_duration_ms', return_value=4 * 3600 * 1000)
    def test_single_candle(self, mock_get_interval):
        candles = [make_candle(0)]
        result = _aggregate_4h_to_8h(candles, 'klines')
        self.assertEqual(result, [])

    # --- (ИЗМЕНЕНИЕ) Исправлен путь: 'aggregation_8h' -> 'data_collector.aggregation_8h' ---
    @patch('data_collector.aggregation_8h.get_interval_duration_ms', side_effect=lambda x: 4 * 3600 * 1000 if x == '4h' else 8 * 3600 * 1000)
    def test_no_utc_aligned_candles(self, mock_get_interval):
        # 2 свечи: 04:00 + 08:00 => 04:00 не на UTC сетке
        candles = [
            make_candle(4 * 3600 * 1000),  # 04:00
            make_candle(8 * 3600 * 1000),  # 08:00
        ]
        result = _aggregate_4h_to_8h(candles, 'klines')
        self.assertEqual(result, [])

    # --- (ИЗМЕНЕНИЕ) Исправлен путь: 'aggregation_8h' -> 'data_collector.aggregation_8h' ---
    @patch('data_collector.aggregation_8h.get_interval_duration_ms', side_effect=lambda x: 4 * 3600 * 1000 if x == '4h' else 8 * 3600 * 1000)
    def test_utc_aligned_klines(self, mock_get_interval):
        # 00:00 + 04:00 => 00:00 UTC сетка
        candles = [
            make_candle(0),  # 00:00
            make_candle(4 * 3600 * 1000),  # 04:00
        ]
        result = _aggregate_4h_to_8h(candles, 'klines')
        expected = [{
            "openTime": 0,
            "openPrice": 100.0,
            "highPrice": 110.0,
            "lowPrice": 90.0,
            "closePrice": 105.0,
            "volume": 2000.0,
            "volumeDelta": 200.0,
            "closeTime": 8 * 3600 * 1000 - 1
        }]
        self.assertEqual(result, expected)

    # --- (ИЗМЕНЕНИЕ) Исправлен путь: 'aggregation_8h' -> 'data_collector.aggregation_8h' ---
    @patch('data_collector.aggregation_8h.get_interval_duration_ms', side_effect=lambda x: 4 * 3600 * 1000 if x == '4h' else 8 * 3600 * 1000)
    def test_utc_aligned_oi(self, mock_get_interval):
        # 16:00 + 20:00 => 16:00 UTC сетка
        open_time_16 = 16 * 3600 * 1000
        open_time_20 = 20 * 3600 * 1000
        candles = [
            make_candle(open_time_16),
            make_candle(open_time_20, openInterest=6000.0, closeTime=open_time_20 + 4 * 3600 * 1000 - 1)
        ]
        result = _aggregate_4h_to_8h(candles, 'oi')
        expected = [{
            "openTime": open_time_16,
            "openInterest": 6000.0,
            "closeTime": open_time_20 + 4 * 3600 * 1000 - 1
        }]
        self.assertEqual(result, expected)

    # --- (ИЗМЕНЕНИЕ) Исправлен путь: 'aggregation_8h' -> 'data_collector.aggregation_8h' ---
    @patch('data_collector.aggregation_8h.get_interval_duration_ms', side_effect=lambda x: 4 * 3600 * 1000 if x == '4h' else 8 * 3600 * 1000)
    def test_gap_in_candles(self, mock_get_interval):
        # 00:00 + 12:00 (пропуск 04:00 и 08:00) => не агрегируется
        candles = [
            make_candle(0),  # 00:00
            make_candle(12 * 3600 * 1000),  # 12:00
        ]
        result = _aggregate_4h_to_8h(candles, 'klines')
        self.assertEqual(result, [])

    # --- (ИЗМЕНЕНИЕ) Исправлен путь: 'aggregation_8h' -> 'data_collector.aggregation_8h' ---
    @patch('data_collector.aggregation_8h.get_interval_duration_ms', side_effect=lambda x: 4 * 3600 * 1000 if x == '4h' else 8 * 3600 * 1000)
    def test_none_values_in_klines(self, mock_get_interval):
        # Одна свеча с None в highPrice
        candles = [
            make_candle(0, highPrice=None),
            make_candle(4 * 3600 * 1000),
        ]
        result = _aggregate_4h_to_8h(candles, 'klines')
        self.assertEqual(result, [])

    # --- (ИЗМЕНЕНИЕ) Исправлен путь: 'aggregation_8h' -> 'data_collector.aggregation_8h' ---
    @patch('data_collector.aggregation_8h.get_interval_duration_ms', side_effect=lambda x: 4 * 3600 * 1000 if x == '4h' else 8 * 3600 * 1000)
    def test_none_values_in_oi(self, mock_get_interval):
        # Одна свеча с None в openInterest
        candles = [
            make_candle(0),
            make_candle(4 * 3600 * 1000, openInterest=None),
        ]
        result = _aggregate_4h_to_8h(candles, 'oi')
        self.assertEqual(result, [])

    # --- (ИЗМЕНЕНИЕ) Исправлен путь: 'aggregation_8h' -> 'data_collector.aggregation_8h' ---
    @patch('data_collector.aggregation_8h.get_interval_duration_ms', side_effect=lambda x: 0 if x == '4h' else 8 * 3600 * 1000)
    def test_zero_interval_duration(self, mock_get_interval):
        # get_interval_duration_ms возвращает 0
        candles = [make_candle(0), make_candle(4 * 3600 * 1000)]
        result = _aggregate_4h_to_8h(candles, 'klines')
        self.assertEqual(result, [])

    # --- (ИЗМЕНЕНИЕ) Исправлен путь: 'aggregation_8h' -> 'data_collector.aggregation_8h' ---
    @patch('data_collector.aggregation_8h.get_interval_duration_ms', side_effect=lambda x: 4 * 3600 * 1000 if x == '4h' else 8 * 3600 * 1000)
    def test_multiple_utc_aligned_candles(self, mock_get_interval):
        # 00:00+04:00, 08:00+12:00, 16:00+20:00
        # (ИЗМЕНЕНИЕ) 00:00, 08:00 и 16:00 попадают на 8h UTC сетку
        candles = [
            make_candle(0),  # UTC 8h
            make_candle(4 * 3600 * 1000),
            make_candle(8 * 3600 * 1000),  # (ИЗМЕНЕНИЕ) ТОЖЕ UTC 8h
            make_candle(12 * 3600 * 1000),
            make_candle(16 * 3600 * 1000),  # UTC 8h
            make_candle(20 * 3600 * 1000),
        ]
        result = _aggregate_4h_to_8h(candles, 'klines')
        
        # --- (ИЗМЕНЕНИЕ) 'expected' теперь ожидает 3 свечи ---
        expected = [
            {
                "openTime": 0,
                "openPrice": 100.0,
                "highPrice": 110.0,
                "lowPrice": 90.0,
                "closePrice": 105.0,
                "volume": 2000.0,
                "volumeDelta": 200.0,
                "closeTime": 8 * 3600 * 1000 - 1
            },
            # (ИЗМЕНЕНИЕ) Добавлена свеча 08:00
            {
                "openTime": 8 * 3600 * 1000,
                "openPrice": 100.0,
                "highPrice": 110.0,
                "lowPrice": 90.0,
                "closePrice": 105.0,
                "volume": 2000.0,
                "volumeDelta": 200.0,
                "closeTime": 16 * 3600 * 1000 - 1
            },
            {
                "openTime": 16 * 3600 * 1000,
                "openPrice": 100.0,
                "highPrice": 110.0,
                "lowPrice": 90.0,
                "closePrice": 105.0,
                "volume": 2000.0,
                "volumeDelta": 200.0,
                "closeTime": 24 * 3600 * 1000 - 1
            }
        ]
        # --- Конец Изменения ---
        self.assertEqual(result, expected)

    # --- (ИЗМЕНЕНИЕ) Исправлен путь: 'aggregation_8h' -> 'data_collector.aggregation_8h' ---
    @patch('data_collector.aggregation_8h.get_interval_duration_ms', side_effect=lambda x: 4 * 3600 * 1000 if x == '4h' else 8 * 3600 * 1000)
    def test_mixed_utc_aligned_and_non_aligned(self, mock_get_interval):
        # 04:00+08:00 => не UTC
        # 16:00+20:00 => UTC
        candles = [
            make_candle(4 * 3600 * 1000),
            make_candle(8 * 3600 * 1000),
            make_candle(16 * 3600 * 1000),
            make_candle(20 * 3600 * 1000),
        ]
        result = _aggregate_4h_to_8h(candles, 'klines')
        expected = [
            {
                "openTime": 16 * 3600 * 1000,
                "openPrice": 100.0,
                "highPrice": 110.0,
                "lowPrice": 90.0,
                "closePrice": 105.0,
                "volume": 2000.0,
                "volumeDelta": 200.0,
                "closeTime": 24 * 3600 * 1000 - 1
            }
        ]
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()