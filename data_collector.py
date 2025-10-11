import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Tuple
from collections import defaultdict

# Импортируем наши новые модули
import url_builder
import api_parser
import api_helpers

CONCURRENCY_LIMIT = 20 # Ограничение одновременных запросов

async def _fetch_url(session: aiohttp.ClientSession, task_info: Dict) -> Tuple[str, str, Any | None]:
    """Асинхронно запрашивает один URL и возвращает результат."""
    url = task_info['url']
    symbol = task_info['symbol']
    data_type = task_info['data_type']
    exchange = task_info['exchange']
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if exchange == 'binance':
        headers.update({
            "Origin": "https://www.binance.com",
            "Referer": "https://www.binance.com/"
        })

    try:
        await asyncio.sleep(0.1) # Задержка как в JS примерах
        async with session.get(url, headers=headers, timeout=10) as response:
            response.raise_for_status()
            json_data = await response.json()
            return (symbol, data_type, exchange, json_data)
    except Exception as e:
        logging.error(f"Ошибка при запросе {url} для {symbol}: {e}")
        return (symbol, data_type, exchange, None)

def _merge_data(processed_data: Dict[str, Dict[str, list]]) -> Dict[str, list]:
    """Объединяет klines, oi и fr данные для каждой монеты."""
    final_data = {}
    for symbol, data_types in processed_data.items():
        klines = sorted(data_types.get('klines', []), key=lambda x: x['openTime'])
        ois = sorted(data_types.get('oi', []), key=lambda x: x['openTime'])
        frs = sorted(data_types.get('fr', []), key=lambda x: x['openTime'])

        if not klines:
            continue

        merged_klines = []
        oi_idx, fr_idx = 0, 0

        for kline in klines:
            # Находим последнее актуальное значение OI
            while oi_idx < len(ois) - 1 and ois[oi_idx + 1]['openTime'] <= kline['openTime']:
                oi_idx += 1
            if ois and ois[oi_idx]['openTime'] <= kline['openTime']:
                kline.update({k:v for k,v in ois[oi_idx].items() if k not in ['openTime', 'closeTime']})

            # Находим последнее актуальное значение FR
            while fr_idx < len(frs) - 1 and frs[fr_idx + 1]['openTime'] <= kline['openTime']:
                fr_idx += 1
            if frs and frs[fr_idx]['openTime'] <= kline['openTime']:
                 kline.update({k:v for k,v in frs[fr_idx].items() if k not in ['openTime', 'closeTime']})
            
            merged_klines.append(kline)
        
        final_data[symbol] = merged_klines
    return final_data

def _format_final_structure(market_data: Dict[str, list], coins: List[Dict], timeframe: str) -> Dict[str, Any]:
    """
    Форматирует собранные данные в финальную структуру с метаданными.
    """
    all_candles = [candle for coin_candles in market_data.values() for candle in coin_candles if coin_candles]
    if not all_candles:
        return {
            "openTime": None,
            "closeTime": None,
            "timeframe": timeframe,
            "data": []
        }

    min_open_time = min(c['openTime'] for c in all_candles)
    max_close_time = max(c['closeTime'] for c in all_candles)
    
    # Создаем карту для быстрого поиска бирж по символу
    exchanges_map = {c['symbol'].split(':')[0]: c['exchanges'] for c in coins}

    data_list = []
    for symbol, candles in market_data.items():
        if candles: # Добавляем монету только если для нее есть данные
            data_list.append({
                "symbol": symbol,
                "exchanges": exchanges_map.get(symbol, []),
                "data": candles
            })

    # Сортируем итоговый список по символу для консистентности
    data_list.sort(key=lambda x: x['symbol'])

    return {
        "openTime": min_open_time,
        "closeTime": max_close_time,
        "timeframe": timeframe,
        "data": data_list
    }

async def fetch_market_data(coins: List[Dict], timeframe: str) -> Dict[str, Any]:
    """
    Основная функция для быстрого параллельного сбора и обработки данных.
    """
    tasks_to_run = []
    # 1. Формируем список всех URL-запросов
    for coin in coins:
        symbol_path = coin['symbol'].split(':')[0]
        symbol_api = symbol_path.replace('/', '')
        exchange = 'binance' if 'binance' in coin['exchanges'] else 'bybit'

        for data_type in ['klines', 'oi', 'fr']:
            url_func = getattr(url_builder, f"get_{exchange}_{data_type}_url", None)
            if url_func:
                url = url_func(symbol_api, timeframe, 500)
                tasks_to_run.append({
                    "url": url, "symbol": symbol_path, "data_type": data_type, "exchange": exchange
                })

    # 2. Выполняем все запросы параллельно с ограничением
    processed_data = defaultdict(dict)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async with aiohttp.ClientSession() as session:
        async def run_task(task_info):
            async with semaphore:
                return await _fetch_url(session, task_info)

        results = await asyncio.gather(*(run_task(t) for t in tasks_to_run))

    # 3. Парсим результаты
    for symbol, data_type, exchange, json_data in results:
        if json_data:
            parser_func = getattr(api_parser, f"parse_{exchange}_{data_type}", None)
            if parser_func:
                data_list = json_data.get('result', {}).get('list', []) if exchange == 'bybit' else json_data
                if data_list:
                    processed_data[symbol][data_type] = parser_func(data_list, timeframe)

    # 4. Объединяем klines, oi, fr
    merged_data = _merge_data(processed_data)
    
    # 5. Форматируем в итоговую структуру
    final_structured_data = _format_final_structure(merged_data, coins, timeframe)
    
    return final_structured_data

