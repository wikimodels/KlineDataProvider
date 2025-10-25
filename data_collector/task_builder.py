from typing import List, Dict, Any

# Импортируем из родительской директории
try:
    from .. import url_builder
    from .. import api_parser
    from .logging_setup import logger, oi_fr_error_logger
except ImportError:
    # Фоллбэк для standalone запуска
    import url_builder
    import api_parser
    import logging
    logger = logging.getLogger(__name__)
    oi_fr_error_logger = logging.getLogger('oi_fr_errors')

# Импортируем из текущей папки
from . import fetch_strategies


def prepare_tasks(coins: List[Dict], timeframe: str) -> List[Dict[str, Any]]:
    """
    Создает список словарей задач, каждый из которых содержит
    информацию для запроса, функцию-стратегию и функцию-парсер.
    """
    tasks_to_run = []
    
    for coin in coins:
        symbol_path = coin['symbol'].split(':')[0]
        symbol_api = symbol_path.replace('/', '')
        # Определяем основную биржу для этой монеты
        exchange = 'binance' if 'binance' in coin['exchanges'] else 'bybit'

        # 1. Klines (всегда простая стратегия)
        klines_url_func = getattr(url_builder, f"get_{exchange}_klines_url", None)
        klines_parser_func = getattr(api_parser, f"parse_{exchange}_klines", None)
        
        if klines_url_func and klines_parser_func:
            url = klines_url_func(symbol_api, timeframe, 400)
            tasks_to_run.append({
                "task_info": {"url": url, "symbol": symbol_path, "data_type": "klines", "exchange": exchange},
                "fetch_strategy": fetch_strategies.fetch_simple,
                "parser": klines_parser_func,
                "timeframe": timeframe # Парсеру нужен timeframe
            })
        else:
            logger.error(f"TASK_BUILDER: Не найден url/parser klines для {exchange} ({symbol_path})")

        # 2. OI и FR
        # (Используем суффиксы из старого data_collector.py)
        for data_type, suffix in [('oi', 'open_interest'), ('fr', 'funding_rate')]:
            url_func_name = f"get_{exchange}_{suffix}_url"
            parser_func_name = f"parse_{exchange}_{data_type}"
            
            url_func = getattr(url_builder, url_func_name, None)
            parser_func = getattr(api_parser, parser_func_name, None)

            if not url_func or not parser_func:
                msg = f"TASK_BUILDER: Не найден {url_func_name} или {parser_func_name} для {symbol_path}"
                logger.error(msg)
                oi_fr_error_logger.error(msg)
                continue

            # Определяем URL и СТРАТЕГИЮ
            strategy_func = None
            url = None
            
            if exchange == 'binance':
                # Binance: простая стратегия
                url = url_func(symbol_api, timeframe, 400) if data_type == 'oi' else url_func(symbol_api, 400)
                strategy_func = fetch_strategies.fetch_simple
            
            elif exchange == 'bybit':
                # Bybit: стратегия с пагинацией
                # URL-билдеру нужен лимит, но он будет перезаписан в стратегии
                # Передаем timeframe для OI
                url = url_func(symbol_api, timeframe, 400) if data_type == 'oi' else url_func(symbol_api, 400)
                strategy_func = fetch_strategies.fetch_bybit_paginated
            
            tasks_to_run.append({
                "task_info": {"url": url, "symbol": symbol_path, "data_type": data_type, "exchange": exchange},
                "fetch_strategy": strategy_func,
                "parser": parser_func,
                "timeframe": timeframe # Парсеру нужен timeframe
            })

    logger.info(f"TASK_BUILDER: Создано {len(tasks_to_run)} задач для {len(coins)} монет.")
    return tasks_to_run
