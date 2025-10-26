from typing import List, Dict, Any, Optional

# Импортируем из родительской директории
try:
    from .. import url_builder
    from .. import api_parser
    # --- Используем правильный импорт логгера ---
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


def prepare_fr_tasks(coins: List[Dict]) -> List[Dict[str, Any]]:
    """
    (Код этой функции не изменен)
    Создает список задач ТОЛЬКО для сбора Funding Rate (FR).
    """
    tasks_to_run = []
    logger.info(f"[FR_TASK_BUILDER] Создание задач для сбора FR для {len(coins)} монет...")

    for coin in coins:
        symbol_path = coin['symbol'].split(':')[0]
        symbol_api = symbol_path.replace('/', '')
        exchange = 'binance' if 'binance' in coin['exchanges'] else 'bybit'

        fr_limit = 400

        base_task_info = {
            "symbol": symbol_path,
            "exchange": exchange,
            "task_specific_timeframe": "1h"
        }

        data_type = 'fr'
        suffix = 'funding_rate'
        url_func_name = f"get_{exchange}_{suffix}_url"
        parser_func_name = f"parse_{exchange}_{data_type}"

        url_func = getattr(url_builder, url_func_name, None)
        parser_func = getattr(api_parser, parser_func_name, None)

        if not url_func or not parser_func:
            msg = f"[FR_TASK_BUILDER] Не найден {url_func_name} или {parser_func_name} для {symbol_path}"
            logger.error(msg)
            oi_fr_error_logger.error(msg)
            continue

        strategy_func = None
        url = url_func(symbol_api, fr_limit)

        if exchange == 'binance':
            strategy_func = fetch_strategies.fetch_simple
        elif exchange == 'bybit':
            strategy_func = fetch_strategies.fetch_bybit_paginated

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


# --- Изменение №1: ИСПРАВЛЕНА СИГНАТУРА (добавлен prefetched_fr_data) ---
def prepare_tasks(coins: List[Dict], timeframe: str, prefetched_fr_data: Optional[Dict] = None) -> List[Dict[str, Any]]:
# --- Конец Изменения №1 ---
    """
    Создает список словарей задач для Klines и OI.
    Пропускает FR, если prefetched_fr_data предоставлен.
    Для timeframe='8h' запрашивает Klines/OI как '4h'.
    (Код этой функции взят из твоего data_collector/task_builder.py (Remove Final Log):data_collector/task_builder.py)
    """
    tasks_to_run = []
    log_prefix = f"[{timeframe.upper()}_TASK_BUILDER]"

    for coin in coins:
        symbol_path = coin['symbol'].split(':')[0]
        symbol_api = symbol_path.replace('/', '')
        exchange = 'binance' if 'binance' in coin['exchanges'] else 'bybit'

        api_timeframe = timeframe
        if timeframe == '8h':
            api_timeframe = '4h'

        klines_limit = 800 if timeframe in ['4h', '8h'] else 400
        oi_limit = 400

        base_task_info = {
            "symbol": symbol_path,
            "exchange": exchange,
        }

        # 1. Klines
        klines_url_func = getattr(url_builder, f"get_{exchange}_klines_url", None)
        klines_parser_func = getattr(api_parser, f"parse_{exchange}_klines", None)

        if klines_url_func and klines_parser_func:
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

            # --- Изменение №1: Используем prefetched_fr_data ---
            if data_type == 'fr' and prefetched_fr_data is not None:
                # logger.debug(f"{log_prefix} Пропускаю задачу FR для {symbol_path} (данные получены из global_fr_cache)")
                continue
            # -----------------------------------------------

            url_func_name = f"get_{exchange}_{suffix}_url"
            parser_func_name = f"parse_{exchange}_{data_type}"

            url_func = getattr(url_builder, url_func_name, None)
            parser_func = getattr(api_parser, parser_func_name, None)

            if not url_func or not parser_func:
                # Не логируем ошибку для FR, если мы его все равно пропустили
                if not (data_type == 'fr' and prefetched_fr_data is not None):
                    msg = f"{log_prefix} Не найден {url_func_name} или {parser_func_name} для {symbol_path}"
                    logger.error(msg)
                    if data_type == 'oi':
                        oi_fr_error_logger.error(msg)
                continue

            strategy_func = None
            url = None
            current_limit = oi_limit

            if data_type == 'oi':
                 url = url_func(symbol_api, api_timeframe, current_limit)
            elif data_type == 'fr':
                 # Этого не должно случиться, если prefetched_fr_data есть,
                 # но если его нет, мы соберем FR (старая логика)
                 fr_limit_url = 400
                 url = url_func(symbol_api, fr_limit_url)

            if exchange == 'binance':
                strategy_func = fetch_strategies.fetch_simple
            elif exchange == 'bybit':
                strategy_func = fetch_strategies.fetch_bybit_paginated

            task_info_oi_fr = base_task_info.copy()
            task_info_oi_fr.update({"url": url, "data_type": data_type})

            tasks_to_run.append({
                "task_info": task_info_oi_fr,
                "fetch_strategy": strategy_func,
                "parser": parser_func,
                "timeframe": api_timeframe
            })

    # (Финальный лог убран, как в твоем файле)
    return tasks_to_run

