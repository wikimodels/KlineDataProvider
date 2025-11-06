import pytest
from unittest.mock import patch
from data_collector.aggregation_8h import _aggregate_4h_to_8h, generate_and_save_8h_cache


# ===== UNIT TESTS FOR _aggregate_4h_to_8h =====

class TestAggregate4hTo8h:
    def test_aggregate_klines_normal_case(self):
        """Test: normal aggregation of two 4h candles into one 8h candle."""
        candles_4h = [
            {
                "openTime": 0,
                "openPrice": 100,
                "highPrice": 110,
                "lowPrice": 90,
                "closePrice": 105,
                "volume": 1000,
                "volumeDelta": 100.5 # <-- 4. ДОБАВЛЯЕМ ТЕСТОВЫЕ ДАННЫЕ
            },
            {
                "openTime": 14400000,  # 4 hours in ms
                "openPrice": 105,
                "highPrice": 115,
                "lowPrice": 100,
                "closePrice": 110,
                "volume": 1200,
                "volumeDelta": -200.2 # <-- 4. ДОБАВЛЯЕМ ТЕСТОВЫЕ ДАННЫЕ
            },
        ]

        with patch("data_collector.aggregation_8h.get_interval_duration_ms") as mock_get_ms:
            mock_get_ms.side_effect = lambda tf: 14400000 if tf == "4h" else 28800000  # 8h = 28800000 ms

            result = _aggregate_4h_to_8h(candles_4h, "klines")

        assert len(result) == 1
        candle = result[0]
        assert candle["openTime"] == 0
        assert candle["closeTime"] == 28800000 - 1
        assert candle["openPrice"] == 100
        assert candle["highPrice"] == 115
        assert candle["lowPrice"] == 90
        assert candle["closePrice"] == 110
        assert candle["volume"] == 2200
        
        # --- 4. НОВЫЙ ТЕСТ: 100.5 + (-200.2) = -99.7 ---
        assert candle["volumeDelta"] == -99.7

    def test_aggregate_klines_missing_delta(self):
        """Test: aggregation handles missing volumeDelta (e.g., from Bybit)."""
        candles_4h = [
            {
                "openTime": 0, "openPrice": 100, "highPrice": 110, 
                "lowPrice": 90, "closePrice": 105, "volume": 1000,
                # No volumeDelta
            },
            {
                "openTime": 14400000, "openPrice": 105, "highPrice": 115, 
                "lowPrice": 100, "closePrice": 110, "volume": 1200,
                "volumeDelta": 50 # <-- Есть только у второй
            },
        ]

        with patch("data_collector.aggregation_8h.get_interval_duration_ms") as mock_get_ms:
            mock_get_ms.side_effect = lambda tf: 14400000 if tf == "4h" else 28800000
            result = _aggregate_4h_to_8h(candles_4h, "klines")

        assert len(result) == 1
        candle = result[0]
        assert candle["volume"] == 2200
        # --- 4. НОВЫЙ ТЕСТ: 0 + 50 = 50 ---
        assert candle["volumeDelta"] == 50

    def test_aggregate_oi_normal_case(self):
        """Test: OI aggregation — value taken from the SECOND 4h candle."""
        candles_4h = [
            {"openTime": 0, "openInterest": 1000},
            {"openTime": 14400000, "openInterest": 1500},
        ]

        with patch("data_collector.aggregation_8h.get_interval_duration_ms") as mock_get_ms:
            mock_get_ms.side_effect = lambda tf: 14400000 if tf == "4h" else 28800000

            result = _aggregate_4h_to_8h(candles_4h, "oi")

        assert len(result) == 1
        assert result[0]["openInterest"] == 1500

    def test_aggregate_skips_non_utc_grid(self):
        """Test: candles not aligned to 8h UTC grid are skipped."""
        candles_4h = [
            {"openTime": 3600000, "openPrice": 100, "highPrice": 110, "lowPrice": 90, "closePrice": 105, "volume": 1000},  # 01:00 UTC
            {"openTime": 18000000, "openPrice": 105, "highPrice": 115, "lowPrice": 100, "closePrice": 110, "volume": 1200},
        ]

        with patch("data_collector.aggregation_8h.get_interval_duration_ms") as mock_get_ms:
            mock_get_ms.side_effect = lambda tf: 14400000 if tf == "4h" else 28800000

            result = _aggregate_4h_to_8h(candles_4h, "klines")

        assert result == []

    def test_aggregate_handles_missing_second_candle(self):
        """Test: first candle on grid but no second → skipped."""
        candles_4h = [
            {"openTime": 0, "openPrice": 100, "highPrice": 110, "lowPrice": 90, "closePrice": 105, "volume": 1000},
            {"openTime": 28800000, "openPrice": 110, "highPrice": 120, "lowPrice": 105, "closePrice": 115, "volume": 1300},
        ]

        with patch("data_collector.aggregation_8h.get_interval_duration_ms") as mock_get_ms:
            mock_get_ms.side_effect = lambda tf: 14400000 if tf == "4h" else 28800000

            result = _aggregate_4h_to_8h(candles_4h, "klines")

        assert result == []

    def test_aggregate_empty_input(self):
        """Test: empty input returns empty list."""
        assert _aggregate_4h_to_8h([], "klines") == []


# ===== INTEGRATION TESTS FOR generate_and_save_8h_cache =====

@pytest.mark.asyncio
@patch("data_collector.aggregation_8h.save_to_cache")
@patch("data_collector.aggregation_8h.format_final_structure")
@patch("data_collector.aggregation_8h.merge_data")
async def test_generate_and_save_8h_cache_success(mock_merge, mock_format, mock_save):
    """Test: successful 8h cache generation and saving."""
    data_4h_enriched = {
        "data": [
            {
                "symbol": "BTCUSDT",
                "data": [
                    {"openTime": 0, "openPrice": 100, "highPrice": 110, "lowPrice": 90, "closePrice": 105, "volume": 1000, "openInterest": 1000, "fundingRate": 0.001, "volumeDelta": 100},
                    {"openTime": 14400000, "openPrice": 105, "highPrice": 115, "lowPrice": 100, "closePrice": 110, "volume": 1200, "openInterest": 1500, "fundingRate": 0.002, "volumeDelta": -200},
                ]
            }
        ]
    }
    coins_from_db_8h = [{"symbol": "BTCUSDT", "exchanges": ["binance"]}]

    # Mock helpers
    def mock_aggregate(candles, data_type):
        if data_type == "klines":
            return [{"openTime": 0, "closeTime": 28799999, "openPrice": 100, "highPrice": 115, "lowPrice": 90, "closePrice": 110, "volume": 2200, "volumeDelta": -100}]
        elif data_type == "oi":
            return [{"openTime": 0, "openInterest": 1500}]
        return []

    with patch("data_collector.aggregation_8h._aggregate_4h_to_8h", side_effect=mock_aggregate):
        await generate_and_save_8h_cache(data_4h_enriched, coins_from_db_8h)

    mock_merge.assert_called_once()
    mock_format.assert_called_once_with(mock_merge.return_value, coins_from_db_8h, "8h")
    mock_save.assert_called_once_with("8h", mock_format.return_value)


@pytest.mark.asyncio
@patch("data_collector.aggregation_8h.save_to_cache")
@patch("data_collector.aggregation_8h.format_final_structure")
@patch("data_collector.aggregation_8h.merge_data")
async def test_generate_and_save_8h_cache_no_data(mock_merge, mock_format, mock_save):
    """Test: no input data → no processing or saving."""
    await generate_and_save_8h_cache({"data": []}, [])
    mock_merge.assert_not_called()
    mock_format.assert_not_called()
    mock_save.assert_not_called()


@pytest.mark.asyncio
@patch("data_collector.aggregation_8h.save_to_cache")
@patch("data_collector.aggregation_8h.format_final_structure")
@patch("data_collector.aggregation_8h.merge_data")
async def test_generate_and_save_8h_cache_funding_rates_preserved(mock_merge, mock_format, mock_save):
    """Test: FR are extracted from 4h candles and passed to merge_data (not aggregated)."""
    data_4h_enriched = {
        "data": [
            {
                "symbol": "ETHUSDT",
                "data": [
                    {"openTime": 0, "fundingRate": 0.001, "volumeDelta": 10},
                    {"openTime": 14400000, "fundingRate": 0.002, "volumeDelta": 20},
                ]
            }
        ]
    }
    coins_from_db_8h = [{"symbol": "ETHUSDT", "exchanges": ["binance"]}]

    # Mock aggregation to return valid 8h klines & OI (so coin is NOT skipped)
    def mock_aggregate(candles, data_type):
        if data_type == "klines":
            return [{"openTime": 0, "closeTime": 28799999, "openPrice": 2000, "highPrice": 2100, "lowPrice": 1900, "closePrice": 2050, "volume": 500, "volumeDelta": 30}]
        elif data_type == "oi":
            return [{"openTime": 0, "openInterest": 8000}]
        return []

    with patch("data_collector.aggregation_8h._aggregate_4h_to_8h", side_effect=mock_aggregate):
        await generate_and_save_8h_cache(data_4h_enriched, coins_from_db_8h)

    # Ensure merge_data was called
    assert mock_merge.call_count == 1

    # Extract the argument passed to merge_data
    processed_data_8h = mock_merge.call_args[0][0]
    assert "ETHUSDT" in processed_data_8h
    fr_list = processed_data_8h["ETHUSDT"]["fr"]
    
    # --- 4. ПРОВЕРЯЕМ, ЧТО Klines/OI АГРЕГИРОВАНЫ ---
    klines_list = processed_data_8h["ETHUSDT"]["klines"]
    oi_list = processed_data_8h["ETHUSDT"]["oi"]
    
    assert len(klines_list) == 1
    assert klines_list[0]["volumeDelta"] == 30 # (Агрегировано)
    assert len(oi_list) == 1
    assert oi_list[0]["openInterest"] == 8000 # (Агрегировано)

    # Verify FR are preserved exactly as in 4h input (no aggregation)
    assert len(fr_list) == 2
    assert fr_list[0] == {"openTime": 0, "fundingRate": 0.001}
    assert fr_list[1] == {"openTime": 14400000, "fundingRate": 0.002}