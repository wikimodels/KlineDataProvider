from typing import List, Dict, Any
from api_helpers import get_interval_duration_ms

# --- KLINE PARSERS ---

def parse_binance_klines(raw_data: List[list], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит klines от Binance, извлекает БАЗОВЫЙ ОБЪЕМ (volume),
    сортирует и удаляет последнюю свечу.
    """
    parsed_list = []
    for entry in raw_data:
        try:
            open_time = int(entry[0])
            
            # --- ИЗМЕНЕНИЕ: Берем base_volume (entry[5]) ---
            base_volume = float(entry[5])
            # --- УБРАНЫ: quoteVolume, taker_buy_quote, buyer_ratio, volumeDelta ---

            parsed_list.append({
                "openTime": open_time,
                "closeTime": int(entry[6]),
                "highPrice": float(entry[2]),
                "lowPrice": float(entry[3]),
                "closePrice": float(entry[4]),
                # --- ИЗМЕНЕНИЕ: Используем 'volume' ---
                "volume": round(base_volume, 2), 
            })
        except (ValueError, TypeError, IndexError):
            continue
    
    # 1. Сортируем данные по времени открытия (от старых к новым)
    sorted_list = sorted(parsed_list, key=lambda x: x['openTime'])
    # 2. Возвращаем все, кроме последней (потенциально незакрытой) свечи
    return sorted_list[:-1]

def parse_bybit_klines(raw_data: List[list], timeframe: str) -> List[Dict[str, Any]]:
    """
    Парсит klines от Bybit, извлекает БАЗОВЫЙ ОБЪЕМ (volume),
    сортирует и удаляет последнюю свечу.
    """
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
                # --- ИЗМЕНЕНИЕ: entry[5] (base volume) вместо entry[6] (quote volume) ---
                "volume": float(entry[5]),
            })
        except (ValueError, TypeError, IndexError):
            continue
            
    sorted_list = sorted(parsed_list, key=lambda x: x['openTime'])
    return sorted_list[:-1]

# --- FUNDING RATE PARSERS ---

def parse_binance_fr(raw_data: List[Dict], timeframe: str) -> List[Dict[str, Any]]:
    """Парсит funding rate от Binance и сортирует."""
    parsed_list = []
    interval_ms = get_interval_duration_ms(timeframe)
    for entry in raw_data:
        try:
            current_rate = float(entry['fundingRate'])
            open_time = int(entry['fundingTime'])
            parsed_list.append({
                "openTime": open_time,
                "closeTime": open_time + interval_ms - 1,
                "fundingRate": current_rate,
            })
        except (ValueError, TypeError, KeyError):
            continue
    return sorted(parsed_list, key=lambda x: x['openTime'])

def parse_bybit_fr(raw_data: List[Dict], timeframe: str) -> List[Dict[str, Any]]:
    """Парсит funding rate от Bybit и сортирует."""
    parsed_list = []
    interval_ms = get_interval_duration_ms(timeframe)
    for entry in raw_data:
        try:
            current_rate = float(entry['fundingRate'])
            open_time = int(entry['fundingRateTimestamp'])
            parsed_list.append({
                "openTime": open_time,
                "closeTime": open_time + interval_ms - 1,
                "fundingRate": current_rate,
            })
        except (ValueError, TypeError, KeyError):
            continue
    return sorted(parsed_list, key=lambda x: x['openTime'])

# --- OPEN INTEREST PARSERS ---

def parse_binance_oi(raw_data: List[Dict], timeframe: str) -> List[Dict[str, Any]]:
    """Парсит open interest от Binance, сортирует и удаляет последнюю точку."""
    parsed_list = []
    interval_ms = get_interval_duration_ms(timeframe)
    for entry in raw_data:
        try:
            current_value = float(entry['sumOpenInterestValue'])
            open_time = int(entry['timestamp'])
            parsed_list.append({
                "openTime": open_time,
                "closeTime": open_time + interval_ms - 1,
                "openInterest": round(current_value, 2),
            })
        except (ValueError, TypeError, KeyError):
            continue
    sorted_list = sorted(parsed_list, key=lambda x: x['openTime'])
    return sorted_list[:-1]

def parse_bybit_oi(raw_data: List[Dict], timeframe: str) -> List[Dict[str, Any]]:
    """Парсит open interest от Bybit, сортирует и удаляет последнюю точку."""
    parsed_list = []
    interval_ms = get_interval_duration_ms(timeframe)
    for entry in raw_data:
        try:
            current_value = float(entry['openInterest'])
            open_time = int(entry['timestamp'])
            parsed_list.append({
                "openTime": open_time,
                "closeTime": open_time + interval_ms - 1,
                "openInterest": round(current_value, 2),
            })
        except (ValueError, TypeError, KeyError):
            continue
    sorted_list = sorted(parsed_list, key=lambda x: x['openTime'])
    return sorted_list[:-1]
