import logging
from typing import List, Dict, Any, Optional
from collections import defaultdict
import time 

# --- Импорты из других модулей проекта ---
try:
    # --- ИЗМЕНЕНИЕ №1: (Импорты из корня, а не относительные) ---
    from api_helpers import get_interval_duration_ms
    from cache_manager import save_to_cache, get_redis_connection
    from data_collector.logging_setup import logger
    from data_collector.data_processing import merge_data, format_final_structure

except ImportError:
    # Фоллбэки
    logger = logging.getLogger(__name__)
    logging.error("Не удалось импортировать зависимости для aggregation_8h.")
    def get_interval_duration_ms(tf: str) -> int: 
        if tf == '4h': return 4 * 3600 * 1000
        if tf == '8h': return 8 * 3600 * 1000
        return 0
    async def save_to_cache(redis_conn, tf, data): pass # --- Исправлена заглушка ---
    async def get_redis_connection(): return None # --- Исправлена заглушка ---
    def merge_data(data): return {}
    def format_final_structure(data, coins, tf): return {}


def _is_8h_close_time_ms(close_time_ms: int) -> bool:
    """(Код не изменен)"""
    eight_hours_ms = get_interval_duration_ms('8h')
    return (close_time_ms + 1) % eight_hours_ms == 0


def _aggregate_klines_4h_to_8h(candle1: Dict, candle2: Dict) -> Optional[Dict]:
    """(Код не изменен)"""
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
        "openTime": candle1['openTime'],
        "closeTime": candle2['closeTime']
    }


def _aggregate_oi_4h_to_8h(candle1: Dict, candle2: Dict) -> Optional[Dict]:
    """(Код не изменен)"""
    oi_value = candle2.get('openInterest')
    close_time = candle2.get('closeTime')
    open_time = candle1.get('openTime')

    if oi_value is not None and close_time is not None and open_time is not None:
        return {
            "openInterest": oi_value,
            "openTime": open_time,
            "closeTime": close_time
        }
    return None


def _aggregate_funding_rates(candle1: Dict, candle2: Dict) -> Optional[Dict]:
    """(Код не изменен)"""
    fr1 = candle1.get('fundingRate')
    fr2 = candle2.get('fundingRate')
    
    if fr2 is not None:
        fr = fr2
    elif fr1 is not None:
        fr = fr1
    else:
        fr = None
    
    close_time = candle2.get('closeTime')
    open_time = candle1.get('openTime')

    if fr is not None and close_time is not None and open_time is not None:
        return {
            "fundingRate": fr,
            "openTime": open_time,
            "closeTime": close_time
        }
    return None


def _build_8h_candles_from_end(candles_4h: List[Dict], data_type: str, symbol: str) -> List[Dict]:
    """(Код не изменен)"""
    if not candles_4h:
        return []

    four_hours_ms = get_interval_duration_ms('4h')
    
    if not four_hours_ms:
        return []

    aggregate_func = {
        'klines': _aggregate_klines_4h_to_8h,
        'oi': _aggregate_oi_4h_to_8h,
        'fr': _aggregate_funding_rates
    }.get(data_type)

    if not aggregate_func:
        return []

    result = []
    
    i = len(candles_4h) - 1
    while i >= 1:
        candle2 = candles_4h[i]
        close_time = candle2.get('closeTime')
        
        if close_time and _is_8h_close_time_ms(close_time):
            break
        
        i -= 1

    while i >= 1:
        candle2 = candles_4h[i]
        candle1 = candles_4h[i - 1]
        
        if candle2.get('openTime') != (candle1.get('closeTime', 0) + 1):
             i -= 1
             continue

        agg_candle = aggregate_func(candle1, candle2)
        if agg_candle:
            result.append(agg_candle)
        
        i -= 2
    
    result.reverse()
    
    logger.debug(f"[8H_GEN_CORE] {symbol} ({data_type}): Построено {len(result)} 8h-свечей из {len(candles_4h)} 4h-свечей.")
    
    return result


# --- ИЗМЕНЕНИЕ №2: Адаптация к формату final_structured_data (из worker.py) ---
async def generate_and_save_8h_cache(
    data_4h_list: List[Dict], # 1. Принимаем список (из cache:4h -> data)
    coins_from_api: List[Dict]
):
    """
    Генерирует и сохраняет 8h кэш из 4h данных.
    
    Args:
        data_4h_list: Список данных 4h из кэша (ОБРЕЗАННЫЙ, 399 свечей).
                 Ожидаемый формат: [{"symbol": "BTCUSDT", "data": [...]}, ...]
        coins_from_api: Список монет с биржи
    """
    logger.info(f"[8H_GEN] Начинаю генерацию данных 8h из (обрезанных [:-1]) данных 4h...")
    start_time = time.time()

    processed_data_8h = defaultdict(dict)
    
    symbols_with_data_count = 0
    symbols_processed_count = 0
    symbols_skipped_no_klines_count = 0

    # --- ИЗМЕНЕНИЕ №2: Проверяем, что data_4h_list это список ---
    if not data_4h_list or not isinstance(data_4h_list, list):
        logger.warning(f"[8H_GEN] Нет данных 'data_4h_list' (None или не список). Генерация 8h невозможна.")
        return

    # --- ИЗМЕНЕНИЕ №2: Итерация по списку (формат final_structured_data) ---
    for coin_data in data_4h_list:
        
        symbol = coin_data.get('symbol')
        # 'data' в этом формате УЖЕ содержит СМЕРЖЕННЫЕ (klines+oi+fr) свечи 4h
        candles_4h = coin_data.get('data', []) 
        
        if not symbol or not candles_4h or len(candles_4h) < 2:
            continue
        
        symbols_with_data_count += 1
        
        # 3. Так как данные уже смержены,
        # мы передаем один и тот же список свечей (candles_4h)
        # во все функции агрегации.
        oi_4h = candles_4h
        fr_4h = candles_4h
        # --------------------------------------------------

        # Агрегируем разные типы данных отдельно
        klines_8h = _build_8h_candles_from_end(candles_4h, 'klines', symbol)
        ois_8h = _build_8h_candles_from_end(oi_4h, 'oi', symbol)
        frs_8h = _build_8h_candles_from_end(fr_4h, 'fr', symbol)

        if not klines_8h:
             logger.debug(f"[8H_GEN] _build_8h_candles_from_end(klines) вернул [] для {symbol}. Пропускаю монету.")
             symbols_skipped_no_klines_count += 1
             continue 

        processed_data_8h[symbol]['klines'] = klines_8h
        processed_data_8h[symbol]['oi'] = ois_8h
        processed_data_8h[symbol]['fr'] = frs_8h 
        symbols_processed_count += 1
    # --- КОНЕЦ ИЗМЕНЕНИЯ №2 ---

    logger.info(f"[8H_GEN] Агрегация 4h->8h завершена. "
                f"Всего монет с 4h-данными: {symbols_with_data_count}. "
                f"Успешно обработано (созданы 8h klines): {symbols_processed_count}. "
                f"Пропущено (не удалось создать 8h klines): {symbols_skipped_no_klines_count}.")

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

    # --- ИЗМЕНЕНИЕ №3: Получаем redis_conn для сохранения ---
    redis_conn = await get_redis_connection()
    if not redis_conn:
        logger.error("[8H_GEN] Не удалось получить Redis соединение. Кэш 8h не сохранен.")
        return
        
    await save_to_cache(redis_conn, '8h', formatted_8h_data)
    # --- КОНЕЦ ИЗМЕНЕНИЯ №3 ---

    end_time = time.time()
    logger.info(f"[8H_GEN] Весь процесс генерации и сохранения кэша 8h занял {end_time - start_time:.2f} сек.")