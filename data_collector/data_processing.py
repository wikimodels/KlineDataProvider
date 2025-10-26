"""
Этот модуль отвечает за обработку (слияние, форматирование)
данных после их сбора и парсинга.
Включает функцию для генерации данных 8h из 4h.
"""
import logging
from typing import List, Dict, Any, Optional # Добавляем Optional
from collections import defaultdict
import math
import copy # Для глубокого копирования данных
import time # Для замера времени генерации 8h

# --- Импорты ---
try:
    from api_helpers import get_interval_duration_ms
    # Импортируем save_to_cache из cache_manager
    from cache_manager import save_to_cache
except ImportError:
    # Фоллбэки
    logging.error("Не удалось импортировать зависимости для data_processing (get_interval_duration_ms, save_to_cache).")
    def get_interval_duration_ms(tf: str) -> int: return 0
    def save_to_cache(tf, data): pass
# --------------------------------------------------------------------

# Используем try-except для импорта логгера, если он уже настроен
try:
    from .logging_setup import logger
except ImportError:
    logger = logging.getLogger(__name__)


def _enrich_coin_metadata(market_data: Dict[str, Any], coins_from_db: List[Dict]) -> Dict[str, Any]:
    """
    Обогащает метаданные каждой монеты, добавляя общие и специфичные для
    таймфрейма аналитические данные из БД. Корректно обрабатывает суффиксы.
    (Код этой функции не изменен)
    """
    if not market_data.get('data') or not coins_from_db:
        if not market_data.get('data'):
             logger.warning("ENRICH: Нет 'data' для обогащения.")
        if not coins_from_db:
             logger.warning("ENRICH: Нет данных из БД для обогащения.")
        return market_data

    coins_map = {c['symbol'].split(':')[0]: c for c in coins_from_db}
    data_timeframe = market_data.get('timeframe')
    enriched_count = 0
    known_suffixes = ['_1h', '_4h', '_8h', '_12h', '_1d']

    for coin_data in market_data['data']:
        symbol = coin_data.get('symbol')
        if not symbol:
            logger.warning("ENRICH: Обнаружена запись без 'symbol' в market_data['data'].")
            continue

        full_coin_details = coins_map.get(symbol)

        if full_coin_details:
            enriched_count += 1
            for key, value in full_coin_details.items():
                if key in ['symbol', 'exchanges', 'created_at']:
                    continue

                clean_key = key
                key_has_known_suffix = False
                for suffix in known_suffixes:
                    if key.endswith(suffix):
                        clean_key = key[:-len(suffix)]
                        key_has_known_suffix = True
                        break

                target_key = clean_key if key_has_known_suffix else key
                if target_key not in coin_data:
                     coin_data[target_key] = value

    return market_data


def _aggregate_4h_to_8h(candles_4h: List[Dict], data_type: str) -> List[Dict]:
    """
    Агрегирует список 4-часовых свечей (Klines или OI) в 8-часовые.
    """
    if not candles_4h or len(candles_4h) < 2:
        return []

    candles_8h = []
    four_hours_ms = get_interval_duration_ms('4h')
    eight_hours_ms = get_interval_duration_ms('8h')

    for i in range(0, len(candles_4h) - 1, 2):
        candle1 = candles_4h[i]
        candle2 = candles_4h[i+1]

        if 'openTime' not in candle1 or 'openTime' not in candle2:
            logger.warning(f"AGGREGATE_8h [{data_type}]: Пропуск пары из-за отсутствия openTime.")
            continue
        if abs(candle2['openTime'] - candle1['openTime'] - four_hours_ms) > 60 * 1000:
            continue

        open_time_8h = candle1['openTime']
        close_time_8h = open_time_8h + eight_hours_ms - 1

        agg_candle = {
            "openTime": open_time_8h,
            "closeTime": close_time_8h
        }

        if data_type == 'klines':
            c1_h = candle1.get('highPrice')
            c2_h = candle2.get('highPrice')
            c1_l = candle1.get('lowPrice')
            c2_l = candle2.get('lowPrice')
            c1_v = candle1.get('volume', 0)
            c2_v = candle2.get('volume', 0)

            if None in (candle1.get('openPrice'), candle2.get('closePrice'), c1_h, c2_h, c1_l, c2_l):
                logger.debug(f"AGGREGATE_8h [Klines]: Пропуск свечи {open_time_8h} из-за None в OHLCV.")
                continue

            agg_candle.update({
                "openPrice": candle1['openPrice'],
                "highPrice": max(c1_h, c2_h),
                "lowPrice": min(c1_l, c2_l),
                "closePrice": candle2['closePrice'],
                "volume": round(c1_v + c2_v, 2)
            })

        elif data_type == 'oi':
            oi_value = candle2.get('openInterest')
            if oi_value is not None:
                agg_candle["openInterest"] = oi_value
            else:
                # --- Изменение №1: Комментируем лог ---
                # logger.debug(f"AGGREGATE_8h [OI]: Пропуск свечи {open_time_8h} из-за None в OI.")
                # ------------------------------------
                continue

        else:
            continue

        candles_8h.append(agg_candle)

    return candles_8h


def merge_data(processed_data: Dict[str, Dict[str, list]]) -> Dict[str, list]:
    """
    Объединяет klines, oi и fr данные для каждой монеты.
    (Код этой функции не изменен)
    """
    final_data = {}

    for symbol, data_types in processed_data.items():
        klines = sorted(data_types.get('klines', []), key=lambda x: x.get('openTime', 0))
        ois = sorted(data_types.get('oi', []), key=lambda x: x.get('openTime', 0))
        frs = sorted(data_types.get('fr', []), key=lambda x: x.get('openTime', 0))

        if not klines:
            continue

        merged_klines = []
        oi_idx, fr_idx = 0, 0

        for kline in klines:
            if 'openTime' not in kline: continue
            merged_kline = kline.copy()

            while oi_idx < len(ois) - 1 and ois[oi_idx + 1].get('openTime', 0) <= merged_kline['openTime']:
                oi_idx += 1
            if ois and oi_idx < len(ois) and ois[oi_idx].get('openTime', 0) <= merged_kline['openTime']:
                current_oi = {k:v for k,v in ois[oi_idx].items() if k not in ['openTime', 'closeTime']}
                merged_kline.update(current_oi)

            while fr_idx < len(frs) - 1 and frs[fr_idx + 1].get('openTime', 0) <= merged_kline['openTime']:
                fr_idx += 1
            if frs and fr_idx < len(frs) and frs[fr_idx].get('openTime', 0) <= merged_kline['openTime']:
                 current_fr = {k:v for k,v in frs[fr_idx].items() if k not in ['openTime', 'closeTime']}
                 merged_kline.update(current_fr)

            merged_klines.append(merged_kline)

        if merged_klines:
            final_data[symbol] = merged_klines

    return final_data


def format_final_structure(market_data: Dict[str, list], coins: List[Dict], timeframe: str) -> Dict[str, Any]:
    """
    Форматирует собранные данные в финальную структуру с метаданными,
    включает 'audit_report' и ОБРЕЗАЕТ данные до 399 свечей.
    (Код этой функции не изменен)
    """
    missing_klines_symbols = []
    missing_oi_symbols = []
    missing_fr_symbols = []
    log_prefix = f"[{timeframe.upper()}]"

    expected_symbols = {c['symbol'].split(':')[0] for c in coins}
    actual_symbols = set(market_data.keys())
    missing_klines_symbols = sorted(list(expected_symbols - actual_symbols))

    data_list_formatted = []
    processed_symbols = set()

    for symbol, candles in market_data.items():
        processed_symbols.add(symbol)
        if not candles:
            continue

        candles_completed = candles[:-1]
        final_candles = candles_completed[-399:]

        if not final_candles:
            continue

        last_candle = final_candles[-1]
        if 'openInterest' not in last_candle or last_candle['openInterest'] is None:
            missing_oi_symbols.append(symbol)
        if 'fundingRate' not in last_candle or last_candle['fundingRate'] is None:
            missing_fr_symbols.append(symbol)

        data_list_formatted.append({
            "symbol": symbol,
            "data": final_candles
        })

    missing_klines_symbols.extend(sorted(list(expected_symbols - processed_symbols)))
    missing_klines_symbols = sorted(list(set(missing_klines_symbols)))

    audit_report = {
        "missing_klines": missing_klines_symbols,
        "missing_oi": sorted(missing_oi_symbols),
        "missing_fr": sorted(missing_fr_symbols)
    }

    if missing_klines_symbols:
        logging.warning(f"{log_prefix} AUDIT [KLINES]: Отсутствуют Klines для {len(missing_klines_symbols)}/{len(expected_symbols)} монет: {missing_klines_symbols[:5]}...")
    num_checked = len(actual_symbols)
    if missing_oi_symbols:
        logging.warning(f"{log_prefix} AUDIT [OI]: Отсутствует OI (в посл. свече) для {len(missing_oi_symbols)}/{num_checked} монет: {sorted(missing_oi_symbols)[:5]}...")
    if missing_fr_symbols:
        logging.warning(f"{log_prefix} AUDIT [FR]: Отсутствует FR (в посл. свече) для {len(missing_fr_symbols)}/{num_checked} монет: {sorted(missing_fr_symbols)[:5]}...")

    all_final_candles = [candle for coin_data in data_list_formatted for candle in coin_data['data']]
    min_open_time = min(c['openTime'] for c in all_final_candles if 'openTime' in c) if all_final_candles else None
    max_close_time = max(c['closeTime'] for c in all_final_candles if 'closeTime' in c) if all_final_candles else None

    exchanges_map = {c['symbol'].split(':')[0]: c['exchanges'] for c in coins}
    for item in data_list_formatted:
        item['exchanges'] = exchanges_map.get(item['symbol'], [])

    data_list_formatted.sort(key=lambda x: x['symbol'])

    return {
        "openTime": min_open_time, "closeTime": max_close_time,
        "timeframe": timeframe, "audit_report": audit_report, "data": data_list_formatted
    }


async def generate_and_save_8h_cache(data_4h_enriched: Dict, coins_from_db_4h: List[Dict]):
    """
    Берет готовые, ОБОГАЩЕННЫЕ данные 4h, агрегирует их в 8h,
    форматирует, обогащает метаданными 4h и сохраняет в кэш '8h'.
    (Код этой функции не изменен)
    """
    logger.info("[8H_GEN] Начинаю генерацию данных 8h из обогащенных 4h...")
    start_time = time.time()

    processed_data_8h = defaultdict(dict)
    processed_count = 0
    symbols_with_data = 0

    original_data_4h_list = copy.deepcopy(data_4h_enriched.get('data', []))

    if not original_data_4h_list:
        logger.warning("[8H_GEN] Нет данных 'data' в исходных 4h. Генерация 8h невозможна.")
        return

    for coin_data_4h in original_data_4h_list:
        symbol = coin_data_4h.get('symbol')
        candles_4h = coin_data_4h.get('data', [])
        if not symbol or not candles_4h:
            continue
        symbols_with_data += 1

        frs_4h = [{ "openTime": c['openTime'],
                    "fundingRate": c.get('fundingRate')}
                   for c in candles_4h if 'fundingRate' in c and c.get('fundingRate') is not None]

        klines_8h = _aggregate_4h_to_8h(candles_4h, 'klines')
        ois_8h = _aggregate_4h_to_8h(candles_4h, 'oi')

        if not klines_8h:
             continue

        processed_data_8h[symbol]['klines'] = klines_8h
        processed_data_8h[symbol]['oi'] = ois_8h
        processed_data_8h[symbol]['fr'] = frs_4h
        processed_count += 1

    logger.info(f"[8H_GEN] Агрегация 4h->8h завершена для {processed_count} из {symbols_with_data} монет с данными.")

    if not processed_data_8h:
        logger.error("[8H_GEN] Нет данных для обработки после агрегации. Кэш 8h не будет создан.")
        return

    logger.info(f"[8H_GEN] Начинаю слияние данных 8h...")
    merged_8h_data = merge_data(processed_data_8h)
    logger.info(f"[8H_GEN] Слияние данных 8h завершено для {len(merged_8h_data)} монет.")

    if not merged_8h_data:
        logger.error("[8H_GEN] Нет данных после слияния 8h. Кэш 8h не будет создан.")
        return

    logger.info("[8H_GEN] Начинаю форматирование структуры 8h...")
    formatted_8h_data = format_final_structure(merged_8h_data, coins_from_db_4h, '8h')
    logger.info(f"[8H_GEN] Форматирование структуры 8h завершено ({len(formatted_8h_data.get('data',[]))} монет).")

    logger.info("[8H_GEN] Начинаю обогащение метаданных 8h...")
    final_enriched_8h_data = _enrich_coin_metadata(formatted_8h_data, coins_from_db_4h)
    logger.info("[8H_GEN] Обогащение метаданных 8h завершено.")

    save_to_cache('8h', final_enriched_8h_data)

    end_time = time.time()
    logger.info(f"[8H_GEN] Весь процесс генерации и сохранения кэша 8h занял {end_time - start_time:.2f} сек.")
