import pytest
from unittest.mock import patch, MagicMock
from data_collector.aggregation_8h import (
    _is_8h_close_time_ms,
    _aggregate_klines_4h_to_8h,
    _aggregate_oi_4h_to_8h,
    _aggregate_funding_rates,
    _build_8h_candles_from_end,
    generate_and_save_8h_cache
)

# --- Константы для тестов ---
HOUR_MS = 60 * 60 * 1000
FOUR_HOURS = 4 * HOUR_MS
EIGHT_HOURS = 8 * HOUR_MS

# --- Вспомогательная функция для создания свечей ---
def make_candle(open_time: int, **kwargs):
    """
    Создаёт 4h-свечу с корректным 'closeTime' для проверки смежности.
    (openTime=0, closeTime=14399999)
    (openTime=14400000, closeTime=28799999)
    """
    candle = {
        "openTime": open_time,
        "closeTime": open_time + FOUR_HOURS - 1, # (e.g., 0 + 14400000 - 1 = 14399999)
        "openPrice": 100.0,
        "highPrice": 110.0,
        "lowPrice": 90.0,
        "closePrice": 105.0,
        "volume": 1000.0,
        "volumeDelta": 50.0,
        "openInterest": 5000.0,
        "fundingRate": None # (По умолчанию None, чтобы тестировать фоллбэки)
    }
    candle.update(kwargs)
    return candle

# --- Авто-мок для get_interval_duration_ms ---
@pytest.fixture(autouse=True)
def mock_intervals():
    """
    Автоматически мокает get_interval_duration_ms для всех тестов.
    """
    def mock_get_ms(tf: str) -> int:
        if tf == '4h': return FOUR_HOURS
        if tf == '8h': return EIGHT_HOURS
        return 0
    
    with patch(
        "data_collector.aggregation_8h.get_interval_duration_ms", 
        side_effect=mock_get_ms
    ):
        yield

# ============================================================================
# === ТЕСТЫ HELPER-ФУНКЦИЙ (Вспомогательных)
# ============================================================================

class TestHelperGrid:
    def test_returns_true_for_8h_grid(self):
        """Проверяет, что 07:59:59.999 и 15:59:59.999 на 8h-сетке"""
        assert _is_8h_close_time_ms(EIGHT_HOURS - 1) == True # (07:59:59.999)
        assert _is_8h_close_time_ms(EIGHT_HOURS * 2 - 1) == True # (15:59:59.999)
        assert _is_8h_close_time_ms(EIGHT_HOURS * 3 - 1) == True # (23:59:59.999)

    def test_returns_false_for_4h_grid(self):
        """Проверяет, что 03:59:59.999 и 11:59:59.999 НЕ на 8h-сетке"""
        assert _is_8h_close_time_ms(FOUR_HOURS - 1) == False # (03:59:59.999)
        assert _is_8h_close_time_ms(EIGHT_HOURS + FOUR_HOURS - 1) == False # (11:59:59.999)

class TestHelperKlines:
    def test_klines_happy_path(self):
        """Тест: Klines агрегируются (High/Low/Volume/Delta)"""
        c1 = make_candle(0, highPrice=110, lowPrice=90, volume=10, volumeDelta=5)
        c2 = make_candle(FOUR_HOURS, highPrice=120, lowPrice=80, volume=20, volumeDelta=-2)
        
        agg = _aggregate_klines_4h_to_8h(c1, c2)
        
        assert agg["openTime"] == 0
        assert agg["closeTime"] == c2["closeTime"]
        assert agg["openPrice"] == c1["openPrice"]
        assert agg["closePrice"] == c2["closePrice"]
        assert agg["highPrice"] == 120 # max(110, 120)
        assert agg["lowPrice"] == 80 # min(90, 80)
        assert agg["volume"] == 30 # 10 + 20
        assert agg["volumeDelta"] == 3 # 5 + (-2)

    def test_klines_handles_none_delta(self):
        """Тест: Klines корректно обрабатывает volumeDelta=None (как 0)"""
        c1 = make_candle(0, volumeDelta=10)
        c2 = make_candle(FOUR_HOURS, volumeDelta=None) # Bybit
        
        agg = _aggregate_klines_4h_to_8h(c1, c2)
        assert agg["volumeDelta"] == 10 # 10 + 0

    def test_klines_returns_none_if_data_missing(self):
        """Тест: Klines возвращает None, если не хватает ключей"""
        c1 = make_candle(0)
        c2_ok = make_candle(FOUR_HOURS)
        c2_bad = make_candle(FOUR_HOURS)
        del c2_bad["closeTime"] # Отсутствует ключ
        
        assert _aggregate_klines_4h_to_8h(c1, c2_ok) is not None
        assert _aggregate_klines_4h_to_8h(c1, c2_bad) is None

class TestHelperOI:
    def test_oi_takes_c2(self):
        """Тест: OI (openInterest) берется из c2"""
        c1 = make_candle(0, openInterest=1000)
        c2 = make_candle(FOUR_HOURS, openInterest=9999)
        
        agg = _aggregate_oi_4h_to_8h(c1, c2)
        assert agg["openInterest"] == 9999
        assert agg["openTime"] == c1["openTime"]

    def test_oi_returns_none_if_data_missing(self):
        """Тест: OI возвращает None, если в c2 нет openInterest"""
        c1 = make_candle(0)
        c2_bad = make_candle(FOUR_HOURS, openInterest=None)
        
        assert _aggregate_oi_4h_to_8h(c1, c2_bad) is None

class TestHelperFR:
    def test_fr_priority_c2(self):
        """Тест: FR берется из c2 (приоритет)"""
        c1 = make_candle(0, fundingRate=0.1)
        c2 = make_candle(FOUR_HOURS, fundingRate=0.5)
        agg = _aggregate_funding_rates(c1, c2)
        assert agg["fundingRate"] == 0.5

    def test_fr_fallback_c1(self):
        """Тест: FR берется из c1 (фоллбэк)"""
        c1 = make_candle(0, fundingRate=0.1)
        c2 = make_candle(FOUR_HOURS, fundingRate=None)
        agg = _aggregate_funding_rates(c1, c2)
        assert agg["fundingRate"] == 0.1

    def test_fr_handles_zero_from_c2(self):
        """Тест: (Критический) FR корректно берет 0.0 из c2 (не 'or')"""
        c1 = make_candle(0, fundingRate=0.1)
        c2 = make_candle(FOUR_HOURS, fundingRate=0.0)
        agg = _aggregate_funding_rates(c1, c2)
        assert agg["fundingRate"] == 0.0

    def test_fr_handles_zero_from_c1(self):
        """Тест: (Критический) FR корректно берет 0.0 из c1 (фоллбэк)"""
        c1 = make_candle(0, fundingRate=0.0)
        c2 = make_candle(FOUR_HOURS, fundingRate=None)
        agg = _aggregate_funding_rates(c1, c2)
        assert agg["fundingRate"] == 0.0

    def test_fr_returns_none_if_all_none(self):
        """Тест: FR возвращает None, если FR нет нигде"""
        c1 = make_candle(0, fundingRate=None)
        c2 = make_candle(FOUR_HOURS, fundingRate=None)
        assert _aggregate_funding_rates(c1, c2) is None

# ============================================================================
# === ТЕСТЫ ГЛАВНОЙ ФУНКЦИИ (_build_8h_candles_from_end)
# ============================================================================

class TestBuildFromEnd:

    def test_happy_path_multiple(self):
        """Тест: [00, 04, 08, 12] -> [agg(00,04), agg(08,12)]"""
        candles = [
            make_candle(0, openPrice=100), # c1
            make_candle(FOUR_HOURS, closePrice=150), # c2
            make_candle(EIGHT_HOURS, openPrice=200), # c3
            make_candle(EIGHT_HOURS + FOUR_HOURS, closePrice=250), # c4
        ]
        
        result = _build_8h_candles_from_end(candles, 'klines')
        
        assert len(result) == 2
        # Проверяем первую 8h свечу
        assert result[0]["openTime"] == 0
        assert result[0]["openPrice"] == 100
        assert result[0]["closePrice"] == 150
        # Проверяем вторую 8h свечу
        assert result[1]["openTime"] == EIGHT_HOURS
        assert result[1]["openPrice"] == 200
        assert result[1]["closePrice"] == 250

    def test_skips_misaligned_grid(self):
        """Тест: [04, 08, 12, 16] -> [agg(08,12)]"""
        candles = [
            make_candle(FOUR_HOURS),     # c1 (04:00)
            make_candle(EIGHT_HOURS),    # c2 (08:00)
            make_candle(EIGHT_HOURS + FOUR_HOURS), # c3 (12:00)
            make_candle(EIGHT_HOURS * 2) # c4 (16:00)
        ]
        
        result = _build_8h_candles_from_end(candles, 'klines')
        
        # i=3 (16:00). c2=16:00. c1=12:00. Grid check (c2.closeTime 19:59): FAILED. i=2.
        # i=2 (12:00). c2=12:00. c1=08:00. Grid check (c2.closeTime 15:59): OK. -> agg(08,12). i=0.
        # i=0. while i >= 1: FAILED.
        
        assert len(result) == 1
        assert result[0]["openTime"] == EIGHT_HOURS # Только agg(08,12)

    def test_skips_gaps(self):
        """Тест: [00, (gap), 08, 12] -> [agg(08,12)]"""
        c1 = make_candle(0)
        c2_gap = make_candle(EIGHT_HOURS) # 08:00
        c3 = make_candle(EIGHT_HOURS + FOUR_HOURS) # 12:00
        candles = [c1, c2_gap, c3]
        
        result = _build_8h_candles_from_end(candles, 'klines')

        # i=2 (12:00). c2=12:00. c1=08:00. Gap check: OK. Grid check: OK. -> agg(08,12). i=0.
        # i=0. while i >= 1: FAILED.
        # (Пара [00, 08] не проверяется, т.к. c2.openTime != c1.closeTime + 1)
        
        assert len(result) == 1
        assert result[0]["openTime"] == EIGHT_HOURS

    def test_handles_extra_candle_at_end(self):
        """Тест: [00, 04, 08] -> [agg(00,04)]"""
        candles = [
            make_candle(0), # 00:00
            make_candle(FOUR_HOURS), # 04:00
            make_candle(EIGHT_HOURS) # 08:00 (Лишняя)
        ]
        
        result = _build_8h_candles_from_end(candles, 'klines')
        
        # i=2 (08:00). c2=08:00. c1=04:00. Gap: OK. Grid check (c2.closeTime 11:59): FAILED. i=1.
        # i=1 (04:00). c2=04:00. c1=00:00. Gap: OK. Grid check (c2.closeTime 07:59): OK. -> agg(00,04). i=-1.
        
        assert len(result) == 1
        assert result[0]["openTime"] == 0

    def test_handles_key_error_protection(self):
        """Тест: Защита от KeyError, если в свече нет 'openTime' или 'closeTime'"""
        c1 = make_candle(0)
        c2 = make_candle(FOUR_HOURS)
        c3_bad = make_candle(EIGHT_HOURS)
        del c3_bad["openTime"]
        
        candles = [c1, c2, c3_bad]
        
        # i=2 (c3_bad). 'openTime' not in c2 -> continue. i=1.
        # i=1 (c2). c2=04:00. c1=00:00. Gap: OK. Grid: OK. -> agg(00,04). i=-1.
        result = _build_8h_candles_from_end(candles, 'klines')
        
        assert len(result) == 1
        assert result[0]["openTime"] == 0

# ============================================================================
# === ТЕСТЫ generate_and_save_8h_cache (Моки)
# ============================================================================

# (Используем pytest.mark.asyncio, т.к. generate_and_save_8h_cache - async)
@pytest.mark.asyncio
@patch("data_collector.aggregation_8h.save_to_cache")
@patch("data_collector.aggregation_8h.format_final_structure")
@patch("data_collector.aggregation_8h.merge_data")
@patch("data_collector.aggregation_8h._build_8h_candles_from_end") # Мокаем нашу новую функцию
async def test_generate_calls_build_from_end(
    mock_build_from_end, mock_merge, mock_format, mock_save
):
    """
    Тест: Убеждаемся, что generate_and_save_8h_cache 
    1. Вызывает _build_8h_candles_from_end (а не старую _aggregate_4h_to_8h)
    2. Вызывает его 3 раза (klines, oi, fr)
    3. Корректно передает 'frs_8h' (а не frs_4h) в merge_data
    """
    # 1. Настройка
    mock_klines = [{"openTime": 0, "closePrice": 1}]
    mock_ois = [{"openTime": 0, "openInterest": 1}]
    mock_frs = [{"openTime": 0, "fundingRate": 0.01}] # (Новые 8h FR)
    
    # Настраиваем мок _build_8h_candles_from_end
    def build_side_effect(candles, data_type):
        if data_type == 'klines': return mock_klines
        if data_type == 'oi': return mock_ois
        if data_type == 'fr': return mock_frs
        return []
    
    mock_build_from_end.side_effect = build_side_effect
    mock_merge.return_value = {"BTCUSDT": "merged"}
    mock_format.return_value = {"data": "formatted"}

    # --- ИЗМЕНЕНИЕ: Входные данные (4h) теперь в "сыром" (merged) формате ---
    candles_4h = [make_candle(0), make_candle(FOUR_HOURS)]
    # (Старый формат: {"data": [{"symbol": "BTCUSDT", "data": candles_4h}]})
    data_4h = {
        "BTCUSDT": candles_4h
    }
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---
    
    coins_list = [{"symbol": "BTCUSDT"}]

    # 2. Вызов
    await generate_and_save_8h_cache(data_4h, coins_list)

    # 3. Проверки
    assert mock_build_from_end.call_count == 3
    mock_build_from_end.assert_any_call(candles_4h, 'klines')
    mock_build_from_end.assert_any_call(candles_4h, 'oi')
    mock_build_from_end.assert_any_call(candles_4h, 'fr')
    
    # Проверяем, что klines_8h/ois_8h/frs_8h были переданы в merge_data
    expected_processed_data = {
        "BTCUSDT": {
            'klines': mock_klines,
            'oi': mock_ois,
            'fr': mock_frs # (Проверяем, что используются frs_8h, а не frs_4h)
        }
    }
    mock_merge.assert_called_once_with(expected_processed_data)
    
    mock_format.assert_called_once_with({"BTCUSDT": "merged"}, coins_list, '8h')
    mock_save.assert_called_once_with('8h', {"data": "formatted"})

@pytest.mark.asyncio
@patch("data_collector.aggregation_8h.save_to_cache")
@patch("data_collector.aggregation_8h.format_final_structure")
@patch("data_collector.aggregation_8h.merge_data")
@patch("data_collector.aggregation_8h._build_8h_candles_from_end")
async def test_generate_skips_if_no_klines(
    mock_build_from_end, mock_merge, mock_format, mock_save
):
    """
    Тест: Проверяем 'if not klines_8h: continue'
    """
    # _build_8h_candles_from_end вернет [] для klines
    mock_build_from_end.return_value = [] 
    
    # --- ИЗМЕНЕНИЕ: Входные данные (4h) теперь в "сыром" (merged) формате ---
    candles_4h = [make_candle(0), make_candle(FOUR_HOURS)]
    # (Старый формат: {"data": [{"symbol": "BTCUSDT", "data": candles_4h}]})
    data_4h = {
        "BTCUSDT": candles_4h
    }
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---
    
    coins_list = [{"symbol": "BTCUSDT"}]

    await generate_and_save_8h_cache(data_4h, coins_list)
    
    # (klines_8h был [], поэтому монета пропущена)
    mock_merge.assert_not_called()
    mock_format.assert_not_called()
    mock_save.assert_not_called()