import logging
from typing import List, Dict, Any, Optional
from collections import defaultdict
import time 

# --- Импорты из других модулей проекта ---
try:
    from api_helpers import get_interval_duration_ms
    from cache_manager import save_to_cache
    from .logging_setup import logger
    from .data_processing import merge_data, format_final_structure

except ImportError:
    # Фоллбэки
    logger = logging.getLogger(__name__)
    logging.error("Не удалось импортировать зависимости для aggregation_8h.")
    def get_interval_duration_ms(tf: str) -> int: 
        if tf == '4h': return 4 * 3600 * 1000
        if tf == '8h': return 8 * 3600 * 1000
        return 0
    def save_to_cache(tf, data): pass
    def merge_data(data): return {}
    def format_final_structure(data, coins, tf): return {}


def _aggregate_klines_4h_to_8h(candle1: Dict, candle2: Dict) -> Optional[Dict]:
    """
    Агрегирует две 4h Klines-свечи в одну 8h.
    Возвращает агрегированную свечу или None, если данные некорректны.
    """
    c1_h = candle1.get('highPrice')
    c2_h = candle2.get('highPrice')
    c1_l = candle1.get('lowPrice')
    c2_l = candle2.get('lowPrice')
    c1_v = candle1.get('volume', 0)
    c2_v = candle2.get('volume', 0)
    c1_vd = candle1.get('volumeDelta') or 0
    c2_vd = candle2.get('volumeDelta') or 0

    if None in (candle1.get('openPrice'), candle2.get('closePrice'),
                c1_h, c2_h, c1_l, c2_l, candle2.get('closeTime')):
        return None

    return {
        "openPrice": candle1['openPrice'],
        "highPrice": max(c1_h, c2_h),
        "lowPrice": min(c1_l, c2_l),
        "closePrice": candle2['closePrice'],
        "volume": round(c1_v + c2_v, 2),
        "volumeDelta": round(c1_vd + c2_vd, 2),
        "closeTime": candle2['closeTime']
    }


def _aggregate_oi_4h_to_8h(candle1: Dict, candle2: Dict) -> Optional[Dict]:
    """
    Агрегирует две 4h OI-свечи в одну 8h.
    Возвращает агрегированную свечу или None, если данные некорректны.
    """
    oi_value = candle2.get('openInterest')
    close_time = candle2.get('closeTime')

    if oi_value is not None and close_time is not None:
        return {
            "openInterest": oi_value,
            "closeTime": close_time
        }
    return None


def _aggregate_4h_to_8h(candles_4h: List[Dict], data_type: str) -> List[Dict]:
    """
    Агрегирует 4-часовые свечи в 8-часовые и оставляет только попадающие на UTC-сетку.
    """
    if not candles_4h or len(candles_4h) < 2:
        return []

    four_hours_ms = get_interval_duration_ms('4h')
    eight_hours_ms = get_interval_duration_ms('8h')

    if four_hours_ms == 0 or eight_hours_ms == 0:
        logger.error(f"AGGREGATE_8h [{data_type}]: Не удалось получить длительность интервалов (4h/8h). Агрегация невозможна.")
        return []

    aggregate_func = _aggregate_klines_4h_to_8h if data_type == 'klines' else _aggregate_oi_4h_to_8h

    candles_8h = []
    i = 0
    while i < len(candles_4h) - 1:
        candle1 = candles_4h[i]
        candle2 = candles_4h[i + 1]

        # Проверяем, что они идут подряд (без разрыва)
        if candle2['openTime'] != candle1['openTime'] + four_hours_ms:
            i += 1
            continue

        open_time_8h = candle1['openTime']

        # Проверяем, попадает ли на UTC сетку (например, 00:00, 08:00)
        if open_time_8h % eight_hours_ms != 0:
            i += 1
            continue

        # Агрегируем в зависимости от типа
        agg_candle = {"openTime": open_time_8h}
        result = aggregate_func(candle1, candle2)

        if result:
            agg_candle.update(result)
            candles_8h.append(agg_candle)
        else:
            logger.debug(f"AGGREGATE_8h [{data_type}]: Пропуск свечи {open_time_8h} из-за некорректных данных.")

        i += 2  # Переходим к следующей паре

    return candles_8h


async def generate_and_save_8h_cache(data_4h: Dict, coins_from_api: List[Dict]):
    """
    (Код этой функции не изменен)
    """
    logger.info("[8H_GEN] Начинаю генерацию данных 8h из данных 4h...")
    start_time = time.time()

    processed_data_8h = defaultdict(dict)
    processed_count = 0
    symbols_with_data = 0

    original_data_4h_list = data_4h.get('data', [])

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
    formatted_8h_data = format_final_structure(merged_8h_data, coins_from_api, '8h')
    logger.info(f"[8H_GEN] Форматирование структуры 8h завершено ({len(formatted_8h_data.get('data',[]))} монет).")

    save_to_cache('8h', formatted_8h_data)

    end_time = time.time()
    logger.info(f"[8H_GEN] Весь процесс генерации и сохранения кэша 8h занял {end_time - start_time:.2f} сек.")