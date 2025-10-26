"""
Этот модуль отвечает за обработку (слияние, форматирование)
данных после их сбора и парсинга.
(Логика 8h ВЫНЕСЕНА в aggregation_8h.py)
"""
import logging
from typing import List, Dict, Any, Optional
from collections import defaultdict
# (Удалены импорты math, copy, time)

# --- (Удален блок импортов api_helpers и cache_manager) ---
# --------------------------------------------------------------------

# Используем try-except для импорта логгера, если он уже настроен
try:
    from .logging_setup import logger
except ImportError:
    logger = logging.getLogger(__name__)


# --- Функция _enrich_coin_metadata (без изменений) ---
def _enrich_coin_metadata(market_data: Dict[str, Any], coins_from_db: List[Dict]) -> Dict[str, Any]:
    """
    Обогащает метаданные каждой монеты, добавляя общие и специфичные для
    таймфрейма аналитические данные из БД. Корректно обрабатывает суффиксы.
    """
    if not market_data.get('data') or not coins_from_db:
        if not market_data.get('data'):
             logger.warning("ENRICH: Нет 'data' для обогащения.")
        if not coins_from_db:
             logger.warning("ENRICH: Нет данных из БД для обогащения.")
        return market_data

    # Создаем карту для быстрого доступа к деталям монеты по символу
    # Убираем ':USDT' если он есть в symbol из БД
    coins_map = {c['symbol'].split(':')[0]: c for c in coins_from_db}
    data_timeframe = market_data.get('timeframe') # Таймфрейм обогащаемых данных
    enriched_count = 0
    known_suffixes = ['_1h', '_4h', '_8h', '_12h', '_1d']

    # Проходим по списку монет в market_data['data']
    for coin_data in market_data['data']:
        symbol = coin_data.get('symbol')
        if not symbol:
            logger.warning("ENRICH: Обнаружена запись без 'symbol' в market_data['data'].")
            continue

        # Находим соответствующие детали из БД
        full_coin_details = coins_map.get(symbol)

        if full_coin_details:
            enriched_count += 1
            # Проходим по всем полям из БД
            for key, value in full_coin_details.items():
                # Пропускаем ключи, которые не нужно добавлять или которые уже есть
                if key in ['symbol', 'exchanges', 'created_at']:
                    continue

                clean_key = key
                key_has_known_suffix = False
                # Проверяем, заканчивается ли ключ на известный суффикс
                for suffix in known_suffixes:
                    if key.endswith(suffix):
                        clean_key = key[:-len(suffix)] # Убираем суффикс
                        key_has_known_suffix = True
                        break

                # Добавляем ключ (с суффиксом или без) в coin_data,
                # если его там еще нет (чтобы не перезаписывать данные Klines/OI/FR)
                target_key = clean_key if key_has_known_suffix else key
                if target_key not in coin_data:
                     coin_data[target_key] = value

    return market_data
# --- Конец _enrich_coin_metadata ---


# --- (Функция _aggregate_4h_to_8h УДАЛЕНА и перенесена) ---


def merge_data(processed_data: Dict[str, Dict[str, list]]) -> Dict[str, list]:
    """
    Объединяет klines, oi и fr данные для каждой монеты.
    """
    final_data = {}

    for symbol, data_types in processed_data.items():
        # Сортировка данных перед слиянием
        klines = sorted(data_types.get('klines', []), key=lambda x: x.get('openTime', 0))
        ois = sorted(data_types.get('oi', []), key=lambda x: x.get('openTime', 0))
        frs = sorted(data_types.get('fr', []), key=lambda x: x.get('openTime', 0))

        if not klines:
            continue

        merged_klines = []
        oi_idx, fr_idx = 0, 0

        # --- Основной цикл слияния ---
        for kline in klines:
            # Пропускаем, если нет openTime
            if 'openTime' not in kline: continue

            # merged_kline = kline.copy() # Копируем, чтобы не изменять исходные
            # --- ИЗМЕНЕНИЕ: Явно создаем merged_kline с полями из "Единого формата" ---
            merged_kline = {
                "openTime": kline.get("openTime"),
                "openPrice": kline.get("openPrice"),
                "highPrice": kline.get("highPrice"),
                "lowPrice": kline.get("lowPrice"),
                "closePrice": kline.get("closePrice"),
                "volume": kline.get("volume"),
                "closeTime": kline.get("closeTime"),
            }
            # -----------------------------------------------------------------------

            # Находим OI
            while oi_idx < len(ois) - 1 and ois[oi_idx + 1].get('openTime', 0) <= merged_kline['openTime']:
                oi_idx += 1
            if ois and oi_idx < len(ois) and ois[oi_idx].get('openTime', 0) <= merged_kline['openTime']:
                # Добавляем только OI, исключая время
                current_oi = {k:v for k,v in ois[oi_idx].items() if k not in ['openTime', 'closeTime']}
                merged_kline.update(current_oi)

            # Находим FR
            while fr_idx < len(frs) - 1 and frs[fr_idx + 1].get('openTime', 0) <= merged_kline['openTime']:
                fr_idx += 1
            if frs and fr_idx < len(frs) and frs[fr_idx].get('openTime', 0) <= merged_kline['openTime']:
                 # Добавляем только FR, исключая время
                 current_fr = {k:v for k,v in frs[fr_idx].items() if k not in ['openTime', 'closeTime']}
                 merged_kline.update(current_fr)

            merged_klines.append(merged_kline)
        # --- Конец основного цикла слияния ---

        if merged_klines: # Добавляем только если что-то смержилось
            final_data[symbol] = merged_klines

    return final_data


def format_final_structure(market_data: Dict[str, list], coins: List[Dict], timeframe: str) -> Dict[str, Any]:
    """
    Форматирует собранные данные в финальную структуру с метаданными,
    включает 'audit_report' и ОБРЕЗАЕТ данные до 399 свечей.
    """
    missing_klines_symbols = []
    missing_oi_symbols = []
    missing_fr_symbols = []
    log_prefix = f"[{timeframe.upper()}]"

    expected_symbols = {c['symbol'].split(':')[0] for c in coins}
    # Используем ключи из market_data как актуальные символы
    actual_symbols = set(market_data.keys())

    missing_klines_symbols = sorted(list(expected_symbols - actual_symbols))

    data_list_formatted = [] # Новый список для отформатированных данных
    processed_symbols = set()

    for symbol, candles in market_data.items():
        processed_symbols.add(symbol)
        if not candles:
            continue

        # --- Обрезка свечей (399) ---
        # 1. Удаляем последнюю (потенциально неполную) свечу
        candles_completed = candles[:-1]
        # 2. Берем последние 399 из завершенных
        final_candles = candles_completed[-399:]
        # ------------------------------------

        if not final_candles:
            continue

        # Проверка наличия OI/FR в ПОСЛЕДНЕЙ из обрезанных свечей
        last_candle = final_candles[-1]
        if 'openInterest' not in last_candle or last_candle['openInterest'] is None:
            missing_oi_symbols.append(symbol)
        if 'fundingRate' not in last_candle or last_candle['fundingRate'] is None:
            missing_fr_symbols.append(symbol)

        # Добавляем в итоговый список
        data_list_formatted.append({
            "symbol": symbol,
            # exchanges добавляются позже при обогащении
            "data": final_candles # Добавляем обрезанные свечи
        })

    # Добавляем символы, которые были в БД, но отсутствуют в market_data
    missing_klines_symbols.extend(sorted(list(expected_symbols - processed_symbols)))
    missing_klines_symbols = sorted(list(set(missing_klines_symbols))) # Убираем дубликаты

    audit_report = {
        "missing_klines": missing_klines_symbols,
        "missing_oi": sorted(missing_oi_symbols),
        "missing_fr": sorted(missing_fr_symbols)
    }

    # Логируем аудит (только ошибки/предупреждения)
    if missing_klines_symbols:
        logging.warning(f"{log_prefix} AUDIT [KLINES]: Отсутствуют Klines для {len(missing_klines_symbols)}/{len(expected_symbols)} монет: {missing_klines_symbols[:5]}...")

    num_checked = len(actual_symbols) # Количество реально проверенных символов
    if missing_oi_symbols:
        logging.warning(f"{log_prefix} AUDIT [OI]: Отсутствует OI (в посл. свече) для {len(missing_oi_symbols)}/{num_checked} монет: {sorted(missing_oi_symbols)[:5]}...")

    if missing_fr_symbols:
        logging.warning(f"{log_prefix} AUDIT [FR]: Отсутствует FR (в посл. свече) для {len(missing_fr_symbols)}/{num_checked} монет: {sorted(missing_fr_symbols)[:5]}...")

    # --- Расчет общего openTime/closeTime ---
    all_final_candles = [candle for coin_data in data_list_formatted for candle in coin_data['data']]
    min_open_time = min(c['openTime'] for c in all_final_candles if 'openTime' in c) if all_final_candles else None
    max_close_time = max(c['closeTime'] for c in all_final_candles if 'closeTime' in c) if all_final_candles else None

    # Добавляем exchanges к data_list_formatted перед возвратом
    exchanges_map = {c['symbol'].split(':')[0]: c['exchanges'] for c in coins}
    for item in data_list_formatted:
        item['exchanges'] = exchanges_map.get(item['symbol'], [])

    data_list_formatted.sort(key=lambda x: x['symbol']) # Сортируем финальный список

    return {
        "openTime": min_open_time, "closeTime": max_close_time,
        "timeframe": timeframe, "audit_report": audit_report, "data": data_list_formatted
    }


# --- (Функция generate_and_save_8h_cache УДАЛЕНА и перенесена) ---
