import asyncio
import aiohttp
import logging
import url_builder

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Список тестовых пар (символов)
# Выберем несколько популярных, чтобы повысить шансы на получение OI/FR
TEST_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"
    # "YGGUSDT", "ZECUSDT" # Эти были в логах, но добавим популярные
]
TIMEFRAME = "4h"
LIMIT = 100  # Меньше лимит, чтобы быстрее проверить

async def fetch_single_url(session: aiohttp.ClientSession, url: str, label: str) -> tuple[str, dict | list | None]:
    """Асинхронно запрашивает один URL и возвращает результат."""
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            json_data = await response.json()
            logging.info(f"УСПЕШНО: {label} -> Статус: {response.status}")
            return label, json_data
    except Exception as e:
        logging.error(f"ОШИБКА: {label} -> {e}")
        return label, None

async def test_api_data():
    """Тестирует API напрямую."""
    async with aiohttp.ClientSession() as session:
        for symbol in TEST_SYMBOLS:
            logging.info(f"\n--- Тестирование для символа: {symbol} ---")
            
            # Собираем все URL для текущего символа
            tasks_to_run = []
            
            # Klines
            binance_kline_url = url_builder.get_binance_klines_url(symbol, TIMEFRAME, LIMIT)
            bybit_kline_url = url_builder.get_bybit_klines_url(symbol, TIMEFRAME, LIMIT)
            tasks_to_run.append((binance_kline_url, f"Binance_Klines_{symbol}"))
            tasks_to_run.append((bybit_kline_url, f"Bybit_Klines_{symbol}"))
            
            # Funding Rate
            binance_fr_url = url_builder.get_binance_funding_rate_url(symbol, LIMIT)
            bybit_fr_url = url_builder.get_bybit_funding_rate_url(symbol, LIMIT)
            tasks_to_run.append((binance_fr_url, f"Binance_FR_{symbol}"))
            tasks_to_run.append((bybit_fr_url, f"Bybit_FR_{symbol}"))
            
            # Open Interest
            binance_oi_url = url_builder.get_binance_open_interest_url(symbol, TIMEFRAME, LIMIT)
            bybit_oi_url = url_builder.get_bybit_open_interest_url(symbol, TIMEFRAME, LIMIT)
            tasks_to_run.append((binance_oi_url, f"Binance_OI_{symbol}"))
            tasks_to_run.append((bybit_oi_url, f"Bybit_OI_{symbol}"))

            # Выполняем все запросы для символа параллельно
            results = await asyncio.gather(*(fetch_single_url(session, url, label) for url, label in tasks_to_run))

            # Анализируем результаты
            results_dict = dict(results)
            for label, data in results_dict.items():
                # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
                # Проверяем, что данные вообще получены (не было ошибки)
                if data is not None: 
                    if "Klines" in label:
                        # Проверяем структуру klines
                        if isinstance(data, list) and len(data) > 0:
                            logging.info(f"  {label}: Получено {len(data)} свечей.")
                        elif isinstance(data, dict) and 'result' in data and isinstance(data.get('result'), dict) and 'list' in data.get('result', {}):
                            list_data = data['result']['list']
                            logging.info(f"  {label}: Получено {len(list_data)} свечей (Bybit).")
                        else:
                            logging.warning(f"  {label}: Непредвиденная структура данных: {type(data)}")
                    elif "FR" in label:
                        # Проверяем структуру FR
                        if isinstance(data, list) and len(data) > 0:
                            # Binance FR
                            has_fr_data = any('fundingRate' in item for item in data)
                            logging.info(f"  {label}: Получено {len(data)} записей. Содержит FR: {has_fr_data}.")
                        elif isinstance(data, dict) and 'result' in data and isinstance(data.get('result'), dict) and 'list' in data.get('result', {}):
                            # Bybit FR
                            list_data = data['result']['list']
                            has_fr_data = any('fundingRate' in item for item in list_data)
                            logging.info(f"  {label}: Получено {len(list_data)} записей (Bybit). Содержит FR: {has_fr_data}.")
                        else:
                            logging.warning(f"  {label}: Непредвиденная структура данных: {type(data)}")
                    elif "OI" in label:
                        # Проверяем структуру OI
                        if isinstance(data, list) and len(data) > 0:
                            # Binance OI
                            has_oi_data = any('sumOpenInterestValue' in item for item in data)
                            logging.info(f"  {label}: Получено {len(data)} записей. Содержит OI: {has_oi_data}.")
                        elif isinstance(data, dict) and 'result' in data and isinstance(data.get('result'), dict) and 'list' in data.get('result', {}):
                            # Bybit OI
                            list_data = data['result']['list']
                            has_oi_data = any('openInterest' in item for item in list_data)
                            logging.info(f"  {label}: Получено {len(list_data)} записей (Bybit). Содержит OI: {has_oi_data}.")
                        else:
                            logging.warning(f"  {label}: Непредвиденная структура данных: {type(data)}")
                else:
                    # Это сработает, если fetch_single_url вернул None (была ошибка)
                    logging.warning(f"  {label}: Данные отсутствуют (ошибка запроса или пустый ответ).")

if __name__ == "__main__":
    # Предполагаем, что url_builder существует. 
    # Если его нет, нужно будет добавить заглушки или реальный код.
    # Для целей исправления синтаксиса, он не нужен.
    # --- Начало Заглушки для url_builder (если файла нет) ---
    try:
        import url_builder
    except ImportError:
        logging.warning("Файл url_builder.py не найден. Используются временные заглушки URL.")
        class MockUrlBuilder:
            def get_binance_klines_url(self, symbol, timeframe, limit):
                return f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={timeframe}&limit={limit}"
            def get_bybit_klines_url(self, symbol, timeframe, limit):
                # Bybit использует минуты для интервалов
                interval_map = {"4h": "240"}
                return f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval={interval_map.get(timeframe, '240')}&limit={limit}"
            def get_binance_funding_rate_url(self, symbol, limit):
                return f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&limit={limit}"
            def get_bybit_funding_rate_url(self, symbol, limit):
                return f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&limit={limit}"
            def get_binance_open_interest_url(self, symbol, timeframe, limit):
                # У Binance 'period' - это timeframe
                return f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period={timeframe}&limit={limit}"
            def get_bybit_open_interest_url(self, symbol, timeframe, limit):
                # У Bybit 'intervalTime' - это timeframe
                return f"https://api.bybit.com/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={timeframe}&limit={limit}"

        url_builder = MockUrlBuilder()
    # --- Конец Заглушки ---

    asyncio.run(test_api_data())
