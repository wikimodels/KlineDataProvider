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
# --------------------------------------------------------------------


def _aggregate_4h_to_8h(candles_4h: List[Dict], data_type: str) -> List[Dict]:
    """
    (Перенесено из data_processing.py)
    Агрегирует список 4-часовых свечей (Klines или OI) в 8-часовые,
    СТРОГО ПРИДЕРЖИВАЯСЬ 8-часовой UTC сетки (00:00, 08:00, 16:00).
    
    (ИЗМЕНЕНО: Логика стала устойчивой к пропускам свечей в 4h)
    (ИЗМЕНЕНО: Исправлен KeyError 'closeTime')
    """
    if not candles_4h or len(candles_4h) < 2:
        return []

    candles_8h = []
    four_hours_ms = get_interval_duration_ms('4h')
    eight_hours_ms = get_interval_duration_ms('8h')

    if four_hours_ms == 0 or eight_hours_ms == 0:
        logger.error(f"AGGREGATE_8h [{data_type}]: Не удалось получить длительность интервалов (4h/8h). Агрегация невозможна.")
        return []

    candles_map = {candle['openTime']: candle for candle in candles_4h if 'openTime' in candle}
    sorted_open_times = sorted(candles_map.keys())

    processed_c1_times = set()

    for c1_open_time in sorted_open_times:
        
        if c1_open_time in processed_c1_times:
            continue
            
        candle1 = candles_map[c1_open_time]

        is_on_8h_grid = (candle1['openTime'] % eight_hours_ms == 0)

        if not is_on_8h_grid:
            continue

        expected_c2_open_time = candle1['openTime'] + four_hours_ms
        candle2 = candles_map.get(expected_c2_open_time)
        
        if candle2 is None:
            logger.debug(f"AGGREGATE_8h [{data_type}]: Пропуск свечи {candle1['openTime']}. Найдена свеча на UTC сетке, но отсутствует смежная 4h свеча (ожидалось: {expected_c2_open_time}).")
            continue

        # --- Свечи 1 и 2 валидны и образуют 8h свечу ---
        open_time_8h = candle1['openTime']
        
        # (Помечаем обе свечи как обработанные)
        processed_c1_times.add(c1_open_time)
        processed_c1_times.add(expected_c2_open_time)

        # --- (ИЗМЕНЕНИЕ) 'closeTime' удален отсюда ---
        agg_candle = {
            "openTime": open_time_8h
        }

        if data_type == 'klines':
            c1_h = candle1.get('highPrice')
            c2_h = candle2.get('highPrice')
            c1_l = candle1.get('lowPrice')
            c2_l = candle2.get('lowPrice')
            c1_v = candle1.get('volume', 0)
            c2_v = candle2.get('volume', 0)

            c1_vd = candle1.get('volumeDelta') or 0
            c2_vd = candle2.get('volumeDelta') or 0

            # --- (ИЗМЕНЕНИЕ) Добавлена проверка .get('closeTime') ---
            if None in (candle1.get('openPrice'), candle2.get('closePrice'), 
                        c1_h, c2_h, c1_l, c2_l, candle2.get('closeTime')):
                logger.debug(f"AGGREGATE_8h [Klines]: Пропуск свечи {open_time_8h} из-за None в OHLCV или closeTime.")
                continue

            agg_candle.update({
                "openPrice": candle1['openPrice'],
                "highPrice": max(c1_h, c2_h),
                "lowPrice": min(c1_l, c2_l),
                "closePrice": candle2['closePrice'],
                "volume": round(c1_v + c2_v, 2), 
                "volumeDelta": round(c1_vd + c2_vd, 2),
                "closeTime": candle2['closeTime'] # (Теперь безопасно)
            })

        elif data_type == 'oi':
            oi_value = candle2.get('openInterest')
            close_time = candle2.get('closeTime')
            
            # --- (ИЗМЕНЕНИЕ) Добавлена проверка 'close_time' ---
            if oi_value is not None and close_time is not None:
                agg_candle["openInterest"] = oi_value
                agg_candle["closeTime"] = close_time
            else:
                logger.debug(f"AGGREGATE_8h [OI]: Пропуск свечи {open_time_8h} из-за None в OI или closeTime.")
                continue

        else: 
            continue

        candles_8h.append(agg_candle)

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