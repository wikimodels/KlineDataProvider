from typing import List, Dict, Any
from api_helpers import get_interval_duration_ms

# --- KLINE PARSERS ---

def parse_binance_klines(raw_data: List[list], timeframe: str) -> List[Dict[str, Any]]:
    """Парсит klines от Binance и вычисляет доп. поля."""
    parsed_list = []
    for entry in raw_data:
        try:
            open_time = int(entry[0])
            total_quote_volume = float(entry[7])
            taker_buy_quote = float(entry[10])
            base_volume = float(entry[5])
            taker_buy_base = float(entry[9])

            buyer_ratio = round((taker_buy_base / base_volume) * 100, 2) if base_volume > 0 else 0
            seller_quote_volume = total_quote_volume - taker_buy_quote
            volume_delta = taker_buy_quote - seller_quote_volume

            parsed_list.append({
                "openTime": open_time,
                "closeTime": int(entry[6]),
                "highPrice": float(entry[2]),
                "lowPrice": float(entry[3]),
                "closePrice": float(entry[4]),
                "quoteVolume": round(total_quote_volume, 2),
                "buyerRatio": buyer_ratio,
                "volumeDelta": round(volume_delta, 2),
            })
        except (ValueError, TypeError, IndexError):
            continue
    return parsed_list

def parse_bybit_klines(raw_data: List[list], timeframe: str) -> List[Dict[str, Any]]:
    """Парсит klines от Bybit."""
    parsed_list = []
    interval_ms = get_interval_duration_ms(timeframe)
    for entry in raw_data:
        try:
            open_time = int(entry[0])
            parsed_list.append({
                "openTime": open_time,
                "closeTime": open_time + interval_ms - 1,
                "highPrice": float(entry[2]),
                "lowPrice": float(entry[3]),
                "closePrice": float(entry[4]),
                "quoteVolume": float(entry[6]),
            })
        except (ValueError, TypeError, IndexError):
            continue
    return parsed_list

# --- FUNDING RATE PARSERS ---

def parse_binance_fr(raw_data: List[Dict], timeframe: str) -> List[Dict[str, Any]]:
    """Парсит funding rate от Binance."""
    parsed_list = []
    interval_ms = get_interval_duration_ms(timeframe)
    for entry in raw_data:
        try:
            open_time = int(entry['fundingTime'])
            parsed_list.append({
                "openTime": open_time,
                "closeTime": open_time + interval_ms - 1,
                "fundingRate": float(entry['fundingRate']),
            })
        except (ValueError, TypeError, KeyError):
            continue
    return parsed_list

def parse_bybit_fr(raw_data: List[Dict], timeframe: str) -> List[Dict[str, Any]]:
    """Парсит funding rate от Bybit."""
    parsed_list = []
    interval_ms = get_interval_duration_ms(timeframe)
    for entry in raw_data:
        try:
            open_time = int(entry['fundingRateTimestamp'])
            parsed_list.append({
                "openTime": open_time,
                "closeTime": open_time + interval_ms - 1,
                "fundingRate": float(entry['fundingRate']),
            })
        except (ValueError, TypeError, KeyError):
            continue
    return parsed_list

# --- OPEN INTEREST PARSERS ---

def parse_binance_oi(raw_data: List[Dict], timeframe: str) -> List[Dict[str, Any]]:
    """Парсит open interest от Binance."""
    parsed_list = []
    interval_ms = get_interval_duration_ms(timeframe)
    for entry in raw_data:
        try:
            open_time = int(entry['timestamp'])
            parsed_list.append({
                "openTime": open_time,
                "closeTime": open_time + interval_ms - 1,
                "openInterest": round(float(entry['sumOpenInterestValue']), 2),
            })
        except (ValueError, TypeError, KeyError):
            continue
    return parsed_list

def parse_bybit_oi(raw_data: List[Dict], timeframe: str) -> List[Dict[str, Any]]:
    """Парсит open interest от Bybit."""
    parsed_list = []
    interval_ms = get_interval_duration_ms(timeframe)
    for entry in raw_data:
        try:
            open_time = int(entry['timestamp'])
            parsed_list.append({
                "openTime": open_time,
                "closeTime": open_time + interval_ms - 1,
                "openInterest": round(float(entry['openInterest']), 2),
            })
        except (ValueError, TypeError, KeyError):
            continue
    return parsed_list

