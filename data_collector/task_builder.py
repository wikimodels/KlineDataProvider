from typing import List, Dict, Any, Optional
import math # <-- НОВЫЙ ИМПОРТ

# Импортируем из родительской директории
try:
    from .. import url_builder
    from .. import api_parser
    # --- ИЗМЕНЕНИЕ: Убираем oi_fr_error_logger ---
    from .logging_setup import logger
except ImportError:
    # Фоллбэк для standalone запуска
    import url_builder
    import api_parser
    import logging
    logger = logging.getLogger(__name__)

# Импортируем из текущей папки
from . import fetch_strategies


# --- НОВЫЙ ХЕЛПЕР ---
def _calculate_oi_limit(timeframe: str) -> int:
    """
    Рассчитывает лимит для OI, чтобы уложиться в 30 дней (720 часов).
    """
    if timeframe == '1h':
        return 400 # (1h * 400 = 400h. Меньше 720h, оставляем)
    if timeframe == '4h':
        return math.ceil(720 / 4) # 180
    if timeframe == '8h':
        return math.ceil(720 / 8) # 90
    if timeframe == '12h':
        return math.ceil(720 / 12) # 60
    if timeframe == '1d':
        return 30 # (30 дней)
    
    # Фоллбэк (если '1m' или др.)
    return 30 
# --- КОНЕЦ НОВОГО ХЕЛПЕРА ---


def prepare_fr_tasks(coins: List[Dict]) -> List[Dict[str, Any]]:
    """
    Создает задачи для сбора Global Funding Rate.
    """
    tasks_to_run = []
    logger.info(f"[FR_TASK_BUILDER] Создание задач для сбора FR для {len(coins)} монет...")

    for coin in coins:
        symbol_path = coin['symbol'].split(':')[0]
        symbol_api = symbol_path.replace('/', '')
        exchange = 'binance' if 'binance' in coin['exchanges'] else 'bybit'

        fr_limit = 400
        base_task_info = { "symbol": symbol_path, "exchange": exchange, "task_specific_timeframe": "1h" }
        data_type = 'fr'
        suffix = 'funding_rate'
        
        # --- ИЗМЕНЕНИЕ 1.1: Добавляем дебаггинг ---
        url_func_name = f"get_{exchange}_{suffix}_url"
        parser_func_name = f"parse_{exchange}_{data_type}"

        url_func = getattr(url_builder, url_func_name, None)
        parser_func = getattr(api_parser, parser_func_name, None)

        logger.debug(f"[FR_TASK_BUILDER: {symbol_path}] URL func: {url_func_name}, Parser: {parser_func_name}")
        # --- Конец Изменения 1.1 ---

        if not url_func or not parser_func:
            msg = f"[FR_TASK_BUILDER] Не найден {url_func_name} или {parser_func_name} для {symbol_path}"
            logger.error(msg)
            continue
        
        # ... (остальная логика) ...
        strategy_func = None
        # --- ИЗМЕНЕНИЕ 1.2: Корректный вызов URL-билдера FR (без timeframe) ---
        url = url_func(symbol_api, fr_limit)
        # --- Конец Изменения 1.2 ---
        
        if exchange == 'binance':
            strategy_func = fetch_strategies.fetch_simple
        elif exchange == 'bybit':
            strategy_func = fetch_strategies.fetch_bybit_paginated
        # ...
        task_info_fr = base_task_info.copy()
        task_info_fr.update({"url": url, "data_type": data_type})
        tasks_to_run.append({
            "task_info": task_info_fr,
            "fetch_strategy": strategy_func,
            "parser": parser_func,
            "timeframe": base_task_info["task_specific_timeframe"]
        })

    logger.info(f"[FR_TASK_BUILDER] Создано {len(tasks_to_run)} задач для FR.")
    return tasks_to_run


def prepare_tasks(coins: List[Dict], timeframe: str, prefetched_fr_data: Optional[Dict] = None) -> List[Dict[str, Any]]:
    """
    Готовит задачи для сбора Klines, Open Interest и Funding Rate.
    """
    tasks_to_run = []
    log_prefix = f"[{timeframe.upper()}_TASK_BUILDER]"

    for coin in coins:
        symbol_path = coin['symbol'].split(':')[0]
        symbol_api = symbol_path.replace('/', '')
        exchange = 'binance' if 'binance' in coin['exchanges'] else 'bybit'
        api_timeframe = '4h' if timeframe == '8h' else timeframe
        
        # --- ИЗМЕНЕНИЕ: Klines limit ВОССТАНОВЛЕН ---
        klines_limit = 800 if timeframe in ['4h', '8h'] else 400
        # --- ИЗМЕНЕНИЕ: OI limit ДИНАМИЧЕСКИЙ ---
        oi_limit = _calculate_oi_limit(api_timeframe)
        
        base_task_info = { "symbol": symbol_path, "exchange": exchange }

        # 1. Klines
        klines_url_func_name = f"get_{exchange}_klines_url"
        klines_parser_func_name = f"parse_{exchange}_klines"
        
        klines_url_func = getattr(url_builder, klines_url_func_name, None)
        klines_parser_func = getattr(api_parser, klines_parser_func_name, None)
        
        # --- ИЗМЕНЕНИЕ 1.3: Добавляем дебаггинг Klines ---
        logger.debug(f"{log_prefix}: {symbol_path} (Klines): URL func: {klines_url_func_name}, Parser: {klines_parser_func_name}")
        # --- Конец Изменения 1.3 ---
        
        if klines_url_func and klines_parser_func:
            # --- ИЗМЕНЕНИЕ: Klines URL-билдер ожидает (symbol_api, interval, limit) ---
            url = klines_url_func(symbol_api, api_timeframe, klines_limit)
            task_info_klines = base_task_info.copy()
            task_info_klines.update({"url": url, "data_type": "klines"})
            tasks_to_run.append({
                "task_info": task_info_klines,
                "fetch_strategy": fetch_strategies.fetch_simple if exchange == 'binance' else fetch_strategies.fetch_bybit_paginated,
                "parser": klines_parser_func,
                "timeframe": api_timeframe
            })
        else:
            logger.error(f"{log_prefix} Не найден url/parser klines для {exchange} ({symbol_path})")

        # 2. OI и FR
        for data_type, suffix in [('oi', 'open_interest'), ('fr', 'funding_rate')]:
            if data_type == 'fr' and prefetched_fr_data is not None:
                continue

            url_func_name = f"get_{exchange}_{suffix}_url"
            parser_func_name = f"parse_{exchange}_{data_type}"
            url_func = getattr(url_builder, url_func_name, None)
            parser_func = getattr(api_parser, parser_func_name, None)

            # --- ИЗМЕНЕНИЕ 1.4: Добавляем дебаггинг OI/FR ---
            logger.debug(f"{log_prefix}: {symbol_path} ({data_type.upper()}): URL func: {url_func_name}, Parser: {parser_func_name}")
            # --- Конец Изменения 1.4 ---
            
            if not url_func or not parser_func:
                if not (data_type == 'fr' and prefetched_fr_data is not None):
                    msg = f"{log_prefix} Не найден {url_func_name} или {parser_func_name} для {symbol_path}"
                    logger.error(msg)
                continue
            
            # ... (остальная логика) ...
            strategy_func = None
            url = None
            
            if data_type == 'oi':
                 # --- ИЗМЕНЕНИЕ: OI URL-билдер ожидает (symbol_api, period, limit) ---
                 url = url_func(symbol_api, api_timeframe, oi_limit)
            elif data_type == 'fr':
                 fr_limit_url = 400
                 # --- ИЗМЕНЕНИЕ: FR URL-билдер ожидает (symbol_api, limit) ---
                 url = url_func(symbol_api, fr_limit_url)
                 
            if exchange == 'binance':
                strategy_func = fetch_strategies.fetch_simple
            elif exchange == 'bybit':
                strategy_func = fetch_strategies.fetch_bybit_paginated
            # ...
            task_info_oi_fr = base_task_info.copy()
            task_info_oi_fr.update({"url": url, "data_type": data_type})
            tasks_to_run.append({
                "task_info": task_info_oi_fr,
                "fetch_strategy": strategy_func,
                "parser": parser_func,
                "timeframe": api_timeframe
            })

    return tasks_to_run