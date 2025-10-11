"""
Этот модуль централизованно создает URL-адреса для всех необходимых
API-запросов к Binance и Bybit, основываясь на примерах из JS-файлов.
"""

# --- Klines (Свечи) ---
def get_binance_klines_url(symbol: str, interval: str, limit: int = 500) -> str:
    return f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"

def get_bybit_klines_url(symbol: str, interval: str, limit: int = 500) -> str:
    return f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit}"

# --- Funding Rate (Ставка финансирования) ---
def get_binance_funding_rate_url(symbol: str, limit: int = 500) -> str:
    return f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&limit={limit}"

def get_bybit_funding_rate_url(symbol: str, limit: int = 500) -> str:
    return f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&limit={limit}"

# --- Open Interest (Открытый интерес) ---
def get_binance_open_interest_url(symbol: str, interval: str, limit: int = 500) -> str:
    return f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period={interval}&limit={limit}"

def get_bybit_open_interest_url(symbol: str, interval: str, limit: int = 500) -> str:
    # Bybit использует 'intervalTime' для OI
    return f"https://api.bybit.com/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval}&limit={limit}"
