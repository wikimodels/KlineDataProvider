import pytest
from data_collector.data_processing import merge_data, format_final_structure


class TestMergeData:
    def test_merge_data_normal_case(self):
        """Тест: нормальное слияние klines, OI, FR и volumeDelta."""
        processed_data = {
            "BTCUSDT": {
                "klines": [
                    {"openTime": 1000, "openPrice": 30000, "highPrice": 31000, "lowPrice": 29000, "closePrice": 30500, "volume": 100, "closeTime": 1060, "volumeDelta": 50.5}, # <-- ДОБАВЛЕНО
                ],
                "oi": [
                    {"openTime": 1000, "openInterest": 500},
                ],
                "fr": [
                    {"openTime": 1000, "fundingRate": 0.001},
                ],
            }
        }

        result = merge_data(processed_data)
        assert "BTCUSDT" in result
        candle = result["BTCUSDT"][0]
        assert candle["openTime"] == 1000
        assert candle["openInterest"] == 500
        assert candle["fundingRate"] == 0.001
        assert candle["volumeDelta"] == 50.5 # <-- ПРОВЕРКА

    def test_merge_data_missing_oi_fr_and_delta(self):
        """Тест: отсутствие OI, FR, volumeDelta — мержится только то, что есть."""
        processed_data = {
            "ETHUSDT": {
                "klines": [{"openTime": 2000, "openPrice": 2000, "closePrice": 2100, "volume": 50, "closeTime": 2060}], # <-- volumeDelta отсутствует (как у Bybit)
                "oi": [],  # Нет OI
                "fr": [{"openTime": 2000, "fundingRate": -0.0005}],
            }
        }

        result = merge_data(processed_data)
        candle = result["ETHUSDT"][0]
        assert "openInterest" not in candle
        assert candle["fundingRate"] == -0.0005
        assert candle["volumeDelta"] is None # <-- ПРОВЕРКА (kline.get() вернет None)

    def test_merge_data_no_klines(self):
        """Тест: монета без klines — игнорируется."""
        processed_data = {
            "DOGEUSDT": {
                "klines": [],
                "oi": [{"openTime": 3000, "openInterest": 10000}],
                "fr": [{"openTime": 3000, "fundingRate": 0.0}],
            }
        }
        result = merge_data(processed_data)
        assert "DOGEUSDT" not in result

    def test_merge_data_multiple_candles(self):
        """Тест: корректное сопоставление OI/FR/volumeDelta по времени."""
        klines = [
            {"openTime": 1000, "closeTime": 1060, "openPrice": 1, "closePrice": 2, "volume": 10, "volumeDelta": 1},
            {"openTime": 2000, "closeTime": 2060, "openPrice": 2, "closePrice": 3, "volume": 20, "volumeDelta": 2},
        ]
        ois = [
            {"openTime": 1000, "openInterest": 100},
            {"openTime": 2000, "openInterest": 200},
        ]
        frs = [
            {"openTime": 1000, "fundingRate": 0.001},
            {"openTime": 2000, "fundingRate": 0.002},
        ]

        processed_data = {"SOLUSDT": {"klines": klines, "oi": ois, "fr": frs}}
        result = merge_data(processed_data)
        candles = result["SOLUSDT"]
        assert len(candles) == 2
        assert candles[0]["openInterest"] == 100
        assert candles[1]["fundingRate"] == 0.002
        assert candles[0]["volumeDelta"] == 1 # <-- ПРОВЕРКА
        assert candles[1]["volumeDelta"] == 2 # <-- ПРОВЕРКА


class TestFormatFinalStructure:
    # (Тесты для format_final_structure не требуют изменений,
    # так как эта функция просто передает 'data', а не анализирует
    # 'volumeDelta'. Логика аудита и обрезки остается прежней.)

    def test_format_normal_case(self):
        """Тест: нормальный финальный формат с обрезкой и аудитом."""
        market_data = {
            "BTCUSDT": [
                {"openTime": i * 1000, "closeTime": i * 1000 + 60, "openPrice": i, "closePrice": i + 1, "volume": 100,
                 "openInterest": 500 + i, "fundingRate": 0.001, "volumeDelta": i} # (Добавили volumeDelta для полноты)
                for i in range(401)  # 401 свеча → последняя удалится, останется 400 → возьмём последние 399
            ]
        }
        coins = [{"symbol": "BTCUSDT", "exchanges": ["binance", "okx"]}]
        result = format_final_structure(market_data, coins, "1h")

        assert len(result["data"][0]["data"]) == 399
        last_candle = result["data"][0]["data"][-1]
        assert last_candle["openTime"] == 399 * 1000
        assert last_candle["volumeDelta"] == 399 # (Проверяем, что данные на месте)

        assert result["data"][0]["exchanges"] == ["binance", "okx"]
        assert result["timeframe"] == "1h"
        assert result["audit_report"] == {"missing_klines": [], "missing_oi": [], "missing_fr": []}

    def test_format_missing_oi_fr_in_last_kept_candle(self):
        """
        Тест: отсутствие OI/FR в ПОСЛЕДНЕЙ ИЗ ОСТАВШИХСЯ свечей.
        """
        market_data = {
            "ETHUSDT": [
                {"openTime": 1000, "closeTime": 1060, "openPrice": 2000, "closePrice": 2100, "volume": 50,
                 # Нет OI и FR → будет в аудите
                },
                {"openTime": 2000, "closeTime": 2060, "openPrice": 2100, "closePrice": 2200, "volume": 60,
                 "openInterest": 1000, "fundingRate": 0.001},
                # ↑ эта свеча — последняя → будет УДАЛЕНА
            ]
        }
        coins = [{"symbol": "ETHUSDT", "exchanges": ["binance"]}]

        result = format_final_structure(market_data, coins, "1h")
        audit = result["audit_report"]
        assert "ETHUSDT" in audit["missing_oi"]
        assert "ETHUSDT" in audit["missing_fr"]

    def test_format_missing_klines_symbols(self):
        """Тест: монета в coins, но отсутствует в market_data — попадает в missing_klines."""
        market_data = {"BTCUSDT": [{"openTime": 1000, "closeTime": 1060, "openPrice": 30000, "closePrice": 30500, "volume": 100}]}
        coins = [
            {"symbol": "BTCUSDT", "exchanges": ["binance"]},
            {"symbol": "XRPUSDT", "exchanges": ["kraken"]},  # отсутствует в market_data
        ]
        result = format_final_structure(market_data, coins, "1h")
        audit = result["audit_report"]
        assert "XRPUSDT" in audit["missing_klines"]

    def test_format_no_valid_candles_after_trimming(self):
        """Тест: после удаления последней свечи — данных нет."""
        market_data = {
            "ADAUSDT": [
                {"openTime": 1000, "closeTime": 1060, "openPrice": 0.5, "closePrice": 0.51, "volume": 1000}
                # только 1 свеча → после удаления → пусто
            ]
        }
        coins = [{"symbol": "ADAUSDT", "exchanges": ["binance"]}]
        result = format_final_structure(market_data, coins, "1h")
        assert len(result["data"]) == 0  # монета не попадает в итог

    def test_format_timeframe_and_times_with_realistic_data(self):
        """
        Тест: корректные openTime/closeTime после обрезки.
        """
        market_data = {
            "BTCUSDT": [
                {"openTime": 1000, "closeTime": 2000, "openPrice": 30000, "closePrice": 30500, "volume": 100},
                {"openTime": 2000, "closeTime": 3000, "openPrice": 30500, "closePrice": 31000, "volume": 110},
                {"openTime": 3000, "closeTime": 4000, "openPrice": 31000, "closePrice": 31500, "volume": 120},
            ],
            "ETHUSDT": [
                {"openTime": 1500, "closeTime": 2500, "openPrice": 2000, "closePrice": 2100, "volume": 50},
                {"openTime": 2500, "closeTime": 3500, "openPrice": 2100, "closePrice": 2200, "volume": 60},
            ],
        }
        coins = [
            {"symbol": "BTCUSDT", "exchanges": ["binance"]},
            {"symbol": "ETHUSDT", "exchanges": ["binance"]},
        ]

        result = format_final_structure(market_data, coins, "1h")
        assert result["openTime"] == 1000
        assert result["closeTime"] == 3000
        assert result["timeframe"] == "1h"