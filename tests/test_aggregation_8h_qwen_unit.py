import pytest
from data_collector.aggregation_8h import _aggregate_4h_to_8h, generate_and_save_8h_cache

# Константы для удобства
HOUR_MS = 60 * 60 * 1000
FOUR_HOURS = 4 * HOUR_MS
EIGHT_HOURS = 8 * HOUR_MS

# Вспомогательная функция для генерации свечей
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


class TestAggregate4hTo8h:
    """Тесты для _aggregate_4h_to_8h."""

    # ------------------------------------------------------------------------
    # 1. Базовые случаи (Klines и OI)
    # ------------------------------------------------------------------------
    def test_klines_basic_aggregation(self):
        """Базовая агрегация Klines: 2 свечи -> 1 свеча 8h."""
        c1 = make_candle(open_time=0)  # 00:00 UTC
        c2 = make_candle(open_time=FOUR_HOURS)  # 04:00 UTC

        result = _aggregate_4h_to_8h([c1, c2], "klines")
        
        assert len(result) == 1
        agg = result[0]
        assert agg["openTime"] == 0
        assert agg["closeTime"] == c2["closeTime"]  # Используется closeTime из c2
        assert agg["openPrice"] == c1["openPrice"]
        assert agg["closePrice"] == c2["closePrice"]
        assert agg["highPrice"] == max(c1["highPrice"], c2["highPrice"])
        assert agg["lowPrice"] == min(c1["lowPrice"], c2["lowPrice"])
        assert agg["volume"] == round(c1["volume"] + c2["volume"], 2)
        assert agg["volumeDelta"] == round(c1["volumeDelta"] + c2["volumeDelta"], 2)

    def test_oi_basic_aggregation(self):
        """Базовая агрегация OI: берётся OI из второй свечи."""
        c1 = make_candle(open_time=0, openInterest=1000)
        c2 = make_candle(open_time=FOUR_HOURS, openInterest=2000)

        result = _aggregate_4h_to_8h([c1, c2], "oi")
        
        assert len(result) == 1
        assert result[0]["openInterest"] == 2000  # Из второй свечи

    # ------------------------------------------------------------------------
    # 2. Привязка к 8-часовой UTC сетке
    # ------------------------------------------------------------------------
    def test_only_8h_grid_candles_are_processed(self):
        """Обрабатываются ТОЛЬКО свечи, начинающиеся на 8h-сетке (00:00, 08:00, ...)."""
        # Свеча на 8h-сетке (00:00)
        c1 = make_candle(open_time=0)
        c2 = make_candle(open_time=FOUR_HOURS)
        # Свеча НЕ на сетке (04:00) — не должна использоваться как начало 8h
        c3 = make_candle(open_time=4 * HOUR_MS)
        c4 = make_candle(open_time=8 * HOUR_MS)

        result = _aggregate_4h_to_8h([c1, c2, c3, c4], "klines")
        
        # Должна быть только одна 8h-свеча (из c1 и c2)
        assert len(result) == 1
        assert result[0]["openTime"] == 0

    def test_candles_on_8h_grid_but_no_pair(self):
        """Свеча на 8h-сетке, но нет пары -> пропуск."""
        c1 = make_candle(open_time=0)  # Есть на сетке
        # c2 (для 04:00) отсутствует

        result = _aggregate_4h_to_8h([c1], "klines")
        assert len(result) == 0

    # ------------------------------------------------------------------------
    # 3. Устойчивость к пропускам и неупорядоченным данным
    # ------------------------------------------------------------------------
    def test_handles_missing_candle2(self):
        """Пропущена вторая свеча -> пропуск пары."""
        c1 = make_candle(open_time=0)
        c3 = make_candle(open_time=8 * HOUR_MS)  # Пропущена свеча на 04:00

        result = _aggregate_4h_to_8h([c1, c3], "klines")
        assert len(result) == 0

    def test_works_with_unsorted_input(self):
        """Работает с неотсортированными свечами."""
        c2 = make_candle(open_time=FOUR_HOURS)  # 04:00
        c1 = make_candle(open_time=0)           # 00:00

        result = _aggregate_4h_to_8h([c2, c1], "klines")
        assert len(result) == 1
        assert result[0]["openTime"] == 0

    def test_skips_duplicate_candles(self):
        """Избегает повторного использования свечей."""
        c1 = make_candle(open_time=0)
        c2 = make_candle(open_time=FOUR_HOURS)
        c3 = make_candle(open_time=8 * HOUR_MS)
        c4 = make_candle(open_time=12 * HOUR_MS)

        # Передаём свечи дважды
        result = _aggregate_4h_to_8h([c1, c2, c3, c4, c1, c2], "klines")
        assert len(result) == 2  # Две 8h-свечи: [0-8h], [8-16h]

    # ------------------------------------------------------------------------
    # 4. Обработка некорректных/неполных данных
    # ------------------------------------------------------------------------
    def test_klines_skips_if_ohlc_missing(self):
        """Klines: пропуск, если в одной из свечей нет OHLC."""
        c1 = make_candle(open_time=0)
        c2 = make_candle(open_time=FOUR_HOURS, closePrice=None)  # Нет closePrice

        result = _aggregate_4h_to_8h([c1, c2], "klines")
        assert len(result) == 0

    def test_oi_skips_if_oi_missing_in_candle2(self):
        """OI: пропуск, если во второй свече нет openInterest."""
        c1 = make_candle(open_time=0, openInterest=1000)
        c2 = make_candle(open_time=FOUR_HOURS)  # openInterest отсутствует
        del c2["openInterest"]

        result = _aggregate_4h_to_8h([c1, c2], "oi")
        assert len(result) == 0

    def test_handles_closeTime_missing_in_candle2(self):
        """Пропуск, если во второй свече нет closeTime."""
        c1 = make_candle(open_time=0)
        c2 = make_candle(open_time=FOUR_HOURS)
        del c2["closeTime"]

        result = _aggregate_4h_to_8h([c1, c2], "klines")
        assert len(result) == 0

    # ------------------------------------------------------------------------
    # 5. Граничные случаи
    # ------------------------------------------------------------------------
    def test_empty_input(self):
        """Пустой список -> пустой результат."""
        assert _aggregate_4h_to_8h([], "klines") == []

    def test_single_candle(self):
        """Одна свеча -> пустой результат."""
        c1 = make_candle(open_time=0)
        assert _aggregate_4h_to_8h([c1], "klines") == []

    def test_unknown_data_type(self):
        """Неизвестный тип данных -> пустой результат."""
        c1 = make_candle(open_time=0)
        c2 = make_candle(open_time=FOUR_HOURS)
        assert _aggregate_4h_to_8h([c1, c2], "unknown") == []

    def test_volume_delta_as_none(self):
        """Обработка volumeDelta = None."""
        c1 = make_candle(open_time=0, volumeDelta=None)
        c2 = make_candle(open_time=FOUR_HOURS, volumeDelta=100.0)

        result = _aggregate_4h_to_8h([c1, c2], "klines")
        assert result[0]["volumeDelta"] == 100.0  # None -> 0

    # ------------------------------------------------------------------------
    # 6. Несколько 8h-свечей
    # ------------------------------------------------------------------------
    def test_multiple_8h_candles(self):
        """Генерация нескольких 8h-свечей."""
        candles = []
        for i in range(6):  # 0h, 4h, 8h, 12h, 16h, 20h
            candles.append(make_candle(open_time=i * FOUR_HOURS))

        result = _aggregate_4h_to_8h(candles, "klines")
        assert len(result) == 3  # [0-8h], [8-16h], [16-24h]
        assert result[0]["openTime"] == 0
        assert result[1]["openTime"] == 8 * HOUR_MS
        assert result[2]["openTime"] == 16 * HOUR_MS

    # ------------------------------------------------------------------------
    # 7. Свечи с нестандартным closeTime
    # ------------------------------------------------------------------------
    def test_nonstandard_closeTime(self):
        """Использование реального closeTime из candle2 (даже если он не идеален)."""
        c1 = make_candle(open_time=0, close_time=4 * HOUR_MS - 1)  # Стандарт
        c2 = make_candle(open_time=FOUR_HOURS, close_time=9 * HOUR_MS)  # Нестандарт (на 1 час больше)

        result = _aggregate_4h_to_8h([c1, c2], "klines")
        assert result[0]["closeTime"] == 9 * HOUR_MS  # Берётся из c2