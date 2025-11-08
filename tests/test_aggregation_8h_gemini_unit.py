import pytest
from unittest.mock import patch, MagicMock
from data_collector.aggregation_8h import (
    _aggregate_4h_to_8h, 
    _aggregate_klines_4h_to_8h, 
    _aggregate_oi_4h_to_8h,
    generate_and_save_8h_cache
)


# --- Константы для тестов ---
HOUR_MS = 60 * 60 * 1000
FOUR_HOURS = 4 * HOUR_MS
EIGHT_HOURS = 8 * HOUR_MS

# --- Вспомогательная функция (из test_aggregation_8h_qwen_unit.py) ---
def make_candle(open_time: int, close_time: int = None, **kwargs):
    """Создаёт 4h-свечу с заданным openTime и опциональными полями."""
    if close_time is None:
        close_time = open_time + FOUR_HOURS - 1
    base = {
        "openTime": open_time,
        "closeTime": close_time,
        "openPrice": 100.0,
        "highPrice": 110.0,
        "lowPrice": 90.0,
        "closePrice": 105.0,
        "volume": 1000.0,
        "volumeDelta": 50.0,
        "openInterest": 5000.0,
        "fundingRate": 0.001
    }
    base.update(kwargs)
    return base

# --- Авто-мок для get_interval_duration_ms ---
@pytest.fixture(autouse=True)
def mock_intervals():
    """
    Автоматически мокает get_interval_duration_ms для всех тестов
    в этом классе.
    (ИЗМЕНЕНО: Использует unittest.mock.patch и ПРАВИЛЬНЫЙ ПУТЬ)
    """
    def mock_get_ms(tf: str) -> int:
        if tf == '4h': return FOUR_HOURS
        if tf == '8h': return EIGHT_HOURS
        return 0
    
    # (ИЗМЕНЕНИЕ: Исправлен путь на 'data_collector.aggregation_8h')
    with patch(
        "data_collector.aggregation_8h.get_interval_duration_ms", 
        side_effect=mock_get_ms
    ):
        yield

# ============================================================================
# === (ИСПРАВЛЕННЫЙ) ТЕСТ-КЛАСС ДЛЯ _aggregate_4h_to_8h ===
# ============================================================================

class TestAggregate4hTo8hRobust:

    def test_happy_path_klines(self):
        """
        Тест 1: Счастливый путь. 
        Две 4h свечи (00:00, 04:00) корректно агрегируются в 8h.
        """
        c1 = make_candle(open_time=0) # 00:00 (На UTC сетке)
        c2 = make_candle(open_time=FOUR_HOURS) # 04:00 (Смежная)
        
        result = _aggregate_4h_to_8h([c1, c2], "klines")
        
        assert len(result) == 1
        agg = result[0]
        assert agg["openTime"] == 0
        assert agg["openPrice"] == c1["openPrice"]
        assert agg["closePrice"] == c2["closePrice"]
        assert agg["volumeDelta"] == c1["volumeDelta"] + c2["volumeDelta"]
        assert agg["closeTime"] == c2["closeTime"]

    def test_happy_path_oi(self):
        """
        Тест 2: Счастливый путь (OI).
        Проверяет, что OI берется из ВТОРОЙ свечи.
        """
        c1 = make_candle(open_time=EIGHT_HOURS, openInterest=1000) # 08:00
        c2 = make_candle(open_time=EIGHT_HOURS + FOUR_HOURS, openInterest=2000) # 12:00
        
        result = _aggregate_4h_to_8h([c1, c2], "oi")
        
        assert len(result) == 1
        assert result[0]["openTime"] == EIGHT_HOURS
        assert result[0]["openInterest"] == 2000 # (Из c2)
        assert result[0]["closeTime"] == c2["closeTime"]

    def test_skips_non_utc_grid_start(self):
        """
        Тест 3: Пропуск свечей не на 8h-сетке.
        Начинаем с 04:00. Он должен быть пропущен. 
        Агрегация начнется только с 08:00.
        """
        c_offgrid = make_candle(open_time=FOUR_HOURS) # 04:00 (Не на 8h UTC)
        c_grid = make_candle(open_time=EIGHT_HOURS) # 08:00 (На 8h UTC)
        c_grid_pair = make_candle(open_time=EIGHT_HOURS + FOUR_HOURS) # 12:00
        candles_4h = [c_offgrid, c_grid, c_grid_pair]

        result = _aggregate_4h_to_8h(candles_4h, "klines")
        
        assert len(result) == 1
        assert result[0]["openTime"] == EIGHT_HOURS # (Начало с 08:00)
        assert result[0]["closePrice"] == c_grid_pair["closePrice"]

    def test_skips_if_gap_in_data(self):
        """
        Тест 4: (ГЛАВНЫЙ) Устойчивость к пропускам.
        Данные: [00:00, 08:00] (пропуск 04:00).
        Должен пропустить [00:00], т.к. нет смежной свечи.
        """
        c1 = make_candle(open_time=0) # (00:00)
        c2 = make_candle(open_time=EIGHT_HOURS) # (08:00)
        candles_4h = [c1, c2]

        result = _aggregate_4h_to_8h(candles_4h, "klines")
        
        assert len(result) == 0 # 00:00 пропущен

    def test_handles_missing_volume_delta(self):
        """
        Тест 5: Обработка 'volumeDelta': None (симуляция Bybit).
        (c1) 50 + (c2) None = 50
        """
        c1 = make_candle(open_time=0, volumeDelta=50) # (Есть)
        c2 = make_candle(open_time=FOUR_HOURS, volumeDelta=None) # (Нет)
        candles_4h = [c1, c2]
        
        result = _aggregate_4h_to_8h(candles_4h, "klines")
        
        assert len(result) == 1
        # (c1_vd or 0) -> 50
        # (c2_vd or 0) -> 0
        assert result[0]["volumeDelta"] == 50 

    def test_skips_klines_if_candle2_closeTime_is_missing(self):
        """
        Тест 6: (ПРОВЕРКА БАГА) Пропускает Klines, если 'closeTime' 
        на второй свече отсутствует (исправляет KeyError).
        """
        c1 = make_candle(open_time=0)
        c2_no_time = make_candle(open_time=FOUR_HOURS)
        del c2_no_time["closeTime"] # Удаляем ключ
        
        candles_4h = [c1, c2_no_time]
        
        # (Код не должен упасть с KeyError)
        result = _aggregate_4h_to_8h(candles_4h, "klines")
        
        assert len(result) == 0 # Свеча [00:00 + 04:00] пропущена

    def test_skips_oi_if_candle2_closeTime_is_missing(self):
        """
        Тест 7: (ПРОВЕРКА БАГА) Пропускает OI, если 'closeTime' 
        на второй свече отсутствует (исправляет KeyError).
        """
        c1 = make_candle(open_time=0)
        c2_no_time = make_candle(open_time=FOUR_HOURS)
        del c2_no_time["closeTime"] # Удаляем ключ
        
        candles_4h = [c1, c2_no_time]

        # (Код не должен упасть с KeyError)
        result = _aggregate_4h_to_8h(candles_4h, "oi")
        
        assert len(result) == 0 # Свеча [00:00 + 04:00] пропущена


# ============================================================================
# === (СТАРЫЕ) ТЕСТЫ ДЛЯ generate_and_save_8h_cache (БЕЗ ИЗМЕНЕНИЙ) ===
# ============================================================================

# (ИЗМЕНЕНИЕ: Исправлен путь на 'data_collector.aggregation_8h')
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
                    {"openTime": 0, "openPrice": 100, "highPrice": 110, "lowPrice": 90, "closePrice": 105, "volume": 1000, "openInterest": 1000, "fundingRate": 0.001, "volumeDelta": 100, "closeTime": FOUR_HOURS - 1},
                    {"openTime": 14400000, "openPrice": 105, "highPrice": 115, "lowPrice": 100, "closePrice": 110, "volume": 1200, "openInterest": 1500, "fundingRate": 0.002, "volumeDelta": -200, "closeTime": EIGHT_HOURS - 1},
                ]
            }
        ]
    }
    coins_from_api_list = [{"symbol": "BTCUSDT", "exchanges": ["binance"]}]

    # Mock helpers
    def mock_aggregate(candles, data_type):
        if data_type == "klines":
            return [{"openTime": 0, "closeTime": 28799999, "openPrice": 100, "highPrice": 115, "lowPrice": 90, "closePrice": 110, "volume": 2200, "volumeDelta": -100}]
        elif data_type == "oi":
            return [{"openTime": 0, "openInterest": 1500, "closeTime": 28799999}]
        return []

    # (ИЗМЕНЕНИЕ: Исправлен путь на 'data_collector.aggregation_8h')
    with patch("data_collector.aggregation_8h._aggregate_4h_to_8h", side_effect=mock_aggregate):
        await generate_and_save_8h_cache(data_4h_enriched, coins_from_api_list)

    mock_merge.assert_called_once()
    mock_format.assert_called_once_with(mock_merge.return_value, coins_from_api_list, "8h")
    mock_save.assert_called_once_with("8h", mock_format.return_value)


# (ИЗМЕНЕНИЕ: Исправлен путь на 'data_collector.aggregation_8h')
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


# (ИЗМЕНЕНИЕ: Исправлен путь на 'data_collector.aggregation_8h')
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
                    {"openTime": 0, "fundingRate": 0.001, "volumeDelta": 10, "closeTime": FOUR_HOURS - 1},
                    {"openTime": 14400000, "fundingRate": 0.002, "volumeDelta": 20, "closeTime": EIGHT_HOURS - 1},
                ]
            }
        ]
    }
    coins_from_api_list = [{"symbol": "ETHUSDT", "exchanges": ["binance"]}]

    # Mock aggregation to return valid 8h klines & OI (so coin is NOT skipped)
    def mock_aggregate(candles, data_type):
        if data_type == "klines":
            return [{"openTime": 0, "closeTime": 28799999, "openPrice": 2000, "highPrice": 2100, "lowPrice": 1900, "closePrice": 2050, "volume": 500, "volumeDelta": 30}]
        elif data_type == "oi":
            return [{"openTime": 0, "openInterest": 8000, "closeTime": 28799999}]
        return []

    # (ИЗМЕНЕНИЕ: Исправлен путь на 'data_collector.aggregation_8h')
    with patch("data_collector.aggregation_8h._aggregate_4h_to_8h", side_effect=mock_aggregate):
        await generate_and_save_8h_cache(data_4h_enriched, coins_from_api_list)

    assert mock_merge.call_count == 1
    processed_data_8h = mock_merge.call_args[0][0]
    fr_list = processed_data_8h["ETHUSDT"]["fr"]
    klines_list = processed_data_8h["ETHUSDT"]["klines"]
    
    assert len(klines_list) == 1
    assert klines_list[0]["volumeDelta"] == 30 # (Агрегировано)

    # Verify FR are preserved exactly as in 4h input (no aggregation)
    assert len(fr_list) == 2
    assert fr_list[0] == {"openTime": 0, "fundingRate": 0.001}
    assert fr_list[1] == {"openTime": 14400000, "fundingRate": 0.002}