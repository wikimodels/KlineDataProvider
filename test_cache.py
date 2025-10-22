import requests
import logging
import time
import asyncio
from typing import Dict, Any, List

# --- НАСТРОЙКА ---
BASE_URL = "http://127.0.0.1:8000"
TIMEFRAME_TO_TEST = '4h'
CACHE_WAIT_TIMEOUT = 600  # 10 минут (увеличено, т.к. 200 монет × 500 свечей — много)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_indicators(data: Dict[str, Any]) -> bool:
    """
    Проверяет, что все ожидаемые индикаторы и поля присутствуют в финальной структуре данных.
    """
    try:
        # --- БАЗОВАЯ ПРОВЕРКА СТРУКТУРЫ ---
        if not all(k in data for k in ["openTime", "closeTime", "timeframe", "data"]):
            logging.error("  -> ПРОВАЛ: Отсутствуют ключи верхнего уровня (openTime, closeTime, timeframe, data).")
            return False

        data_list = data.get('data', [])
        if not data_list:
            logging.warning("  -> ВАЛИДАЦИЯ: Список 'data' пуст, но структура верна.")
            return True

        first_coin_data = data_list[0]
        symbol = first_coin_data.get("symbol", "Неизвестный символ")

        # --- ПРОВЕРКА МЕТАДАННЫХ ---
        expected_common_meta_keys = {
            "symbol", "exchanges", "logoUrl", "category", "volatility_index",
            "efficiency_index", "trend_harmony_index", "btc_correlation",
            "returns_skewness", "avg_wick_ratio", "relative_strength_vs_btc",
            "max_drawdown_percent", "data"
        }
        expected_tf_meta_keys = {"hurst", "entropy"}
        all_expected_keys = expected_common_meta_keys.union(expected_tf_meta_keys)

        missing_keys = [key for key in all_expected_keys if key not in first_coin_data]
        if missing_keys:
            logging.error(f"  -> ПРОВАЛ: У монеты {symbol} отсутствуют мета-поля: {missing_keys}")
            return False
        logging.info(f"  -> УСПЕХ: Все {len(all_expected_keys)} мета-полей для {symbol} на месте.")

        # --- ПРОВЕРКА СВЕЧЕЙ И ИНДИКАТОРОВ ---
        candle_list = first_coin_data.get('data', [])
        if not candle_list:
            logging.warning(f"  -> ВАЛИДАЦИЯ: Список свечей для {symbol} пуст.")
            return True

        last_candle = candle_list[-1]

        # --- СПИСОК ВСЕХ ОЖИДАЕМЫХ ПОЛЕЙ (индикаторов) ---
        expected_indicator_keys = {
            # --- ADX ---
            "adx", "di_plus", "di_minus",
            # --- VWAP ---
            "w_avwap", "w_avwap_upper_band", "w_avwap_lower_band",
            "m_avwap", "m_avwap_upper_band", "m_avwap_lower_band",
            # --- ATR ---
            "atr",
            # --- Bollinger Bands ---
            "bb_basis", "bb_upper", "bb_lower", "bb_width",
            # --- CMF ---
            "cmf", "cmf_ema",
            # --- EMA ---
            "ema_50", "ema_100", "ema_150",
            # --- Highest / Lowest ---
            "highest_50", "lowest_50",
            # --- KAMA ---
            "kama", "kama_sc",
            # --- Keltner Channel ---
            "kc_upper", "kc_middle", "kc_lower", "kc_width",
            # --- MACD ---
            "macd", "macd_signal", "macd_hist",
            # --- OBV ---
            "obv", "obv_ema",
            # --- RSI ---
            "rsi",
            # --- Patterns ---
            "is_doji", "is_bullish_engulfing", "is_bearish_engulfing", "is_hammer", "is_pinbar",
            # --- RVWAP ---
            "rvwap",
            "rvwap_upper_band_1_0", "rvwap_lower_band_1_0", "rvwap_width_1_0",
            "rvwap_upper_band_2_0", "rvwap_lower_band_2_0", "rvwap_width_2_0",
            # --- Slope (EMA) ---
            "ema_50_slope", "ema_100_slope", "ema_150_slope",
            # --- VZO ---
            "vzo",
            # --- Z-Score ---
            "closePrice_z_score",
            "bb_width_z_score",
            "kc_width_z_score",
            "rvwap_width_1_0_z_score",
            "ema_proximity_z_score",
            # Если 'openInterest' и 'fundingRate' есть — проверяем их z_score тоже
            # "openInterest_z_score",  # Условно
            # "fundingRate_z_score",   # Условно
        }

        # --- Проверяем, есть ли 'openInterest' и 'fundingRate' в свече ---
        dynamic_keys = set()
        if 'openInterest' in last_candle:
            dynamic_keys.add('openInterest_z_score')
        if 'fundingRate' in last_candle:
            dynamic_keys.add('fundingRate_z_score')

        expected_indicator_keys.update(dynamic_keys)

        missing_candle_keys = [key for key in expected_indicator_keys if key not in last_candle]
        if missing_candle_keys:
            logging.error(f"  -> ПРОВАЛ: В последней свече для {symbol} отсутствуют индикаторы: {missing_candle_keys}")
            return False

        logging.info(f"  -> УСПЕХ: Все {len(expected_indicator_keys)} индикаторов для {symbol} на месте.")

        logging.info(f"  -> УСПЕХ ВАЛИДАЦИИ: Все индикаторы и поля для {symbol} корректны.")
        return True

    except Exception as e:
        logging.error(f"  -> КРИТИЧЕСКАЯ ОШИБКА ВАЛИДАЦИИ: {e}", exc_info=True)
        return False


async def main_test():
    """
    Основная асинхронная функция для проведения теста индикаторов.
    """
    start_time = time.time()
    
    logging.info(f"--- Шаг 1: Запуск задачи для таймфрейма '{TIMEFRAME_TO_TEST}' ---")
    post_url = f"{BASE_URL}/get-market-data"
    try:
        response = requests.post(post_url, json={"timeframe": TIMEFRAME_TO_TEST})
        if response.status_code != 202:
            logging.critical(f"Не удалось запустить задачу. Сервер ответил {response.status_code}. Тест остановлен.")
            return
        logging.info(f"  -> УСПЕХ: Задача для '{TIMEFRAME_TO_TEST}' принята в обработку.")
    except requests.exceptions.RequestException as e:
        logging.critical(f"Не удалось отправить запрос. Сервер запущен? Ошибка: {e}. Тест остановлен.")
        return

    logging.info(f"\n--- Шаг 2: Ожидание результата в кэше (таймаут {CACHE_WAIT_TIMEOUT} сек) ---")
    cache_url = f"{BASE_URL}/cache/{TIMEFRAME_TO_TEST}"
    
    wait_start_time = time.time()
    while time.time() - wait_start_time < CACHE_WAIT_TIMEOUT:
        try:
            response = requests.get(cache_url)
            if response.status_code == 200:
                logging.info(f"  -> УСПЕХ: Данные появились в кэше!")
                
                logging.info(f"\n--- Шаг 3: Проверка всех индикаторов ---")
                is_valid = validate_indicators(response.json())
                
                if is_valid:
                    total_time = time.time() - start_time
                    logging.info(f"\n--- ТЕСТ УСПЕШЕН: Все индикаторы присутствуют! Общее время: {total_time:.2f} сек. ---")
                else:
                    logging.critical(f"\n--- ТЕСТ ПРОВАЛЕН: Не все индикаторы присутствуют. ---")
                
                return
                
            logging.info(f"  -> ОЖИДАНИЕ: Кэш пока пуст (404). Ждем 10 секунд...")
            await asyncio.sleep(10)
            
        except requests.exceptions.RequestException as e:
            logging.error(f"  -> ОШИБКА: Не удалось подключиться к серверу для проверки кэша. {e}")
            await asyncio.sleep(10)

    logging.critical(f"--- ПРОВАЛ: Данные не появились в кэше за {CACHE_WAIT_TIMEOUT} секунд. ---")


if __name__ == "__main__":
    asyncio.run(main_test())