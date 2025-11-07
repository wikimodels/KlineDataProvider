import pytest
from unittest.mock import patch
from data_collector.aggregation_8h import _aggregate_4h_to_8h, generate_and_save_8h_cache

# --- Константы для тестов ---
FOUR_HOURS_MS = 4 * 3600 * 1000
EIGHT_HOURS_MS = 8 * 3600 * 1000

# Вспомогательная фабрика для создания полных 4h-свечей
def make_kline_candle(open_time: int, close_time: int, **overrides):
    """Создаёт полную 4h-свечу с минимально необходимыми полями."""
    base = {
        "openTime": open_time,
        "openPrice": 100.0,
        "highPrice": 110.0,
        "lowPrice": 90.0,
        "closePrice": 105.0,
        "volume": 1000.0,
        "volumeDelta": 50.0,
        "closeTime": close_time
    }
    base.update(overrides)
    return base

def make_oi_candle(open_time: int, close_time: int, open_interest: float):
    """Создаёт 4h-свечу с Open Interest."""
    return {
        "openTime": open_time,
        "openInterest": open_interest,
        "closeTime": close_time
    }

# ============================================================================
# === ФИКСТУРА ДЛЯ МОКА get_interval_duration_ms ===
# ============================================================================

@pytest.fixture(autouse=True)
def mock_intervals():
    def mock_get_ms(tf: str) -> int:
        if tf == '4h': return FOUR_HOURS_MS
        if tf == '8h': return EIGHT_HOURS_MS
        return 0
    
    with patch("data_collector.aggregation_8h.get_interval_duration_ms", side_effect=mock_get_ms):
        yield

# ============================================================================
# === ТЕСТЫ ДЛЯ _aggregate_4h_to_8h ===
# ============================================================================

class TestAggregate4hTo8hRobust:

    def test_happy_path_klines(self):
        candle1 = make_kline_candle(
            open_time=0,
            close_time=FOUR_HOURS_MS - 1,
            openPrice=100, highPrice=110, lowPrice=90, closePrice=105, volume=10, volumeDelta=5
        )
        candle2 = make_kline_candle(
            open_time=FOUR_HOURS_MS,
            close_time=EIGHT_HOURS_MS - 1,
            openPrice=105, highPrice=120, lowPrice=100, closePrice=115, volume=20, volumeDelta=10
        )
        result = _aggregate_4h_to_8h([candle1, candle2], "klines")
        assert len(result) == 1
        agg = result[0]
        assert agg["openTime"] == 0
        assert agg["openPrice"] == 100
        assert agg["highPrice"] == 120
        assert agg["lowPrice"] == 90
        assert agg["closePrice"] == 115
        assert agg["volume"] == 30
        assert agg["volumeDelta"] == 15
        assert agg["closeTime"] == EIGHT_HOURS_MS - 1

    def test_happy_path_oi(self):
        candle1 = make_oi_candle(0, FOUR_HOURS_MS - 1, 1000)
        candle2 = make_oi_candle(FOUR_HOURS_MS, EIGHT_HOURS_MS - 1, 5000)
        result = _aggregate_4h_to_8h([candle1, candle2], "oi")
        assert len(result) == 1
        assert result[0]["openInterest"] == 5000
        assert result[0]["openTime"] == 0
        assert result[0]["closeTime"] == EIGHT_HOURS_MS - 1

    def test_skips_non_utc_grid_start(self):
        c_offgrid = make_kline_candle(
            open_time=FOUR_HOURS_MS,  # 04:00 — не на 8h-сетке
            close_time=1,
            openPrice=100, closePrice=105
        )
        c_grid = make_kline_candle(
            open_time=EIGHT_HOURS_MS,  # 08:00 — на сетке
            close_time=2,
            openPrice=100, closePrice=105
        )
        c_grid_pair = make_kline_candle(
            open_time=EIGHT_HOURS_MS + FOUR_HOURS_MS,  # 12:00
            close_time=3,
            openPrice=100, closePrice=150  # ← обязательно!
        )
        candles_4h = [c_offgrid, c_grid, c_grid_pair]
        result = _aggregate_4h_to_8h(candles_4h, "klines")
        assert len(result) == 1
        assert result[0]["openTime"] == EIGHT_HOURS_MS
        assert result[0]["closePrice"] == 150

    def test_aggregates_robustly_with_data_gaps(self):
        """
        Данные: [00:00, 08:00, 12:00] (пропуск 04:00).
        Должен пропустить 00:00 и успешно агрегировать [08:00 + 12:00].
        """
        c1 = make_kline_candle(
            open_time=0,  # 00:00
            close_time=1,
            openPrice=100, closePrice=105
        )
        c2 = make_kline_candle(
            open_time=EIGHT_HOURS_MS,  # 08:00
            close_time=2,
            openPrice=200, closePrice=205  # ← ЯВНО задаём openPrice=200
        )
        c3 = make_kline_candle(
            open_time=EIGHT_HOURS_MS + FOUR_HOURS_MS,  # 12:00
            close_time=3,
            openPrice=300, closePrice=350  # ← ЯВНО задаём значения
        )
        candles_4h = [c1, c2, c3]
        result = _aggregate_4h_to_8h(candles_4h, "klines")
        assert len(result) == 1
        assert result[0]["openTime"] == EIGHT_HOURS_MS
        assert result[0]["openPrice"] == 200   # ← Теперь совпадает
        assert result[0]["closePrice"] == 350

    def test_handles_missing_volume_delta(self):
        candle1 = make_kline_candle(
            open_time=0, close_time=1,
            volumeDelta=50
        )
        candle2 = make_kline_candle(
            open_time=FOUR_HOURS_MS, close_time=2,
            volumeDelta=None  # имитация Bybit
        )
        result = _aggregate_4h_to_8h([candle1, candle2], "klines")
        assert len(result) == 1
        assert result[0]["volumeDelta"] == 50  # 50 + 0

    def test_uses_real_close_time_from_candle2(self):
        real_close_time = EIGHT_HOURS_MS + 5000
        candle1 = make_kline_candle(open_time=0, close_time=1)
        candle2 = make_kline_candle(
            open_time=FOUR_HOURS_MS,
            close_time=real_close_time,
            openPrice=100, closePrice=100  # ← обязательно!
        )
        result = _aggregate_4h_to_8h([candle1, candle2], "klines")
        assert len(result) == 1
        assert result[0]["closeTime"] == real_close_time
        assert result[0]["closeTime"] != EIGHT_HOURS_MS - 1

    def test_skips_oi_if_candle2_oi_is_none(self):
        candle1 = make_oi_candle(0, 1, 1000)
        candle2 = {  # без openInterest
            "openTime": FOUR_HOURS_MS,
            "closeTime": 2
        }
        result = _aggregate_4h_to_8h([candle1, candle2], "oi")
        assert len(result) == 0

    def test_skips_klines_if_candle2_closeTime_is_missing(self):
        candle1 = make_kline_candle(open_time=0, close_time=1)
        candle2 = {  # без closeTime
            "openTime": FOUR_HOURS_MS,
            "openPrice": 100, "highPrice": 110, "lowPrice": 90, "closePrice": 105, "volume": 10
        }
        result = _aggregate_4h_to_8h([candle1, candle2], "klines")
        assert len(result) == 0

    def test_skips_oi_if_candle2_closeTime_is_missing(self):
        candle1 = make_oi_candle(0, 1, 1000)
        candle2 = {  # без closeTime
            "openTime": FOUR_HOURS_MS,
            "openInterest": 5000
        }
        result = _aggregate_4h_to_8h([candle1, candle2], "oi")
        assert len(result) == 0

# ============================================================================
# === ИНТЕГРАЦИОННЫЕ ТЕСТЫ ДЛЯ generate_and_save_8h_cache ===
# ============================================================================

@pytest.mark.asyncio
@patch("data_collector.aggregation_8h.save_to_cache")
@patch("data_collector.aggregation_8h.format_final_structure")
@patch("data_collector.aggregation_8h.merge_data")
async def test_generate_and_save_8h_cache_success(mock_merge, mock_format, mock_save):
    data_4h_enriched = {
        "data": [{
            "symbol": "BTCUSDT",
            "data": [
                make_kline_candle(
                    open_time=0,
                    close_time=FOUR_HOURS_MS - 1,
                    openPrice=100, highPrice=110, lowPrice=90, closePrice=105,
                    openInterest=1000, fundingRate=0.001
                ),
                make_kline_candle(
                    open_time=FOUR_HOURS_MS,
                    close_time=EIGHT_HOURS_MS - 1,
                    openPrice=105, highPrice=115, lowPrice=100, closePrice=110,
                    openInterest=1500, fundingRate=0.002
                )
            ]
        }]
    }
    coins_from_api_list = [{"symbol": "BTCUSDT", "exchanges": ["binance"]}]

    def mock_aggregate(candles, data_type):
        if data_type == "klines":
            return [{
                "openTime": 0,
                "closeTime": EIGHT_HOURS_MS - 1,
                "openPrice": 100,
                "highPrice": 115,
                "lowPrice": 90,
                "closePrice": 110,
                "volume": 2200,
                "volumeDelta": -100
            }]
        elif data_type == "oi":
            return [{"openTime": 0, "openInterest": 1500, "closeTime": EIGHT_HOURS_MS - 1}]
        return []

    with patch("data_collector.aggregation_8h._aggregate_4h_to_8h", side_effect=mock_aggregate):
        await generate_and_save_8h_cache(data_4h_enriched, coins_from_api_list)

    mock_merge.assert_called_once()
    mock_format.assert_called_once_with(mock_merge.return_value, coins_from_api_list, "8h")
    mock_save.assert_called_once_with("8h", mock_format.return_value)

@pytest.mark.asyncio
@patch("data_collector.aggregation_8h.save_to_cache")
@patch("data_collector.aggregation_8h.format_final_structure")
@patch("data_collector.aggregation_8h.merge_data")
async def test_generate_and_save_8h_cache_no_data(mock_merge, mock_format, mock_save):
    await generate_and_save_8h_cache({"data": []}, [])
    mock_merge.assert_not_called()
    mock_format.assert_not_called()
    mock_save.assert_not_called()

@pytest.mark.asyncio
@patch("data_collector.aggregation_8h.save_to_cache")
@patch("data_collector.aggregation_8h.format_final_structure")
@patch("data_collector.aggregation_8h.merge_data")
async def test_generate_and_save_8h_cache_funding_rates_preserved(mock_merge, mock_format, mock_save):
    data_4h_enriched = {
        "data": [{
            "symbol": "ETHUSDT",
            "data": [
                {**make_kline_candle(0, FOUR_HOURS_MS - 1), "fundingRate": 0.001},
                {**make_kline_candle(FOUR_HOURS_MS, EIGHT_HOURS_MS - 1), "fundingRate": 0.002}
            ]
        }]
    }
    coins_from_api_list = [{"symbol": "ETHUSDT", "exchanges": ["binance"]}]

    def mock_aggregate(candles, data_type):
        if data_type == "klines":
            return [{"openTime": 0, "closeTime": EIGHT_HOURS_MS - 1, "openPrice": 2000, "highPrice": 2100, "lowPrice": 1900, "closePrice": 2050, "volume": 500, "volumeDelta": 30}]
        elif data_type == "oi":
            return [{"openTime": 0, "openInterest": 8000, "closeTime": EIGHT_HOURS_MS - 1}]
        return []

    with patch("data_collector.aggregation_8h._aggregate_4h_to_8h", side_effect=mock_aggregate):
        await generate_and_save_8h_cache(data_4h_enriched, coins_from_api_list)

    processed_data_8h = mock_merge.call_args[0][0]
    fr_list = processed_data_8h["ETHUSDT"]["fr"]
    assert len(fr_list) == 2
    assert fr_list[0] == {"openTime": 0, "fundingRate": 0.001}
    assert fr_list[1] == {"openTime": FOUR_HOURS_MS, "fundingRate": 0.002}