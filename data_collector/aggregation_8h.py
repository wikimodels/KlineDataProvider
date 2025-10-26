import logging
from typing import List, Dict, Any, Optional
from collections import defaultdict
import copy # Для глубокого копирования данных
import time # Для замера времени генерации 8h

# --- Импорты из других модулей проекта ---
try:
    # Импорты из корневого уровня
    # (api_helpers.py должен быть в корне, рядом с cache_manager.py)
    from api_helpers import get_interval_duration_ms
    from cache_manager import save_to_cache
    
    # Импорты из пакета data_collector
    from .logging_setup import logger
    # Импортируем функции, от которых зависим, из data_processing
    from .data_processing import merge_data, format_final_structure, _enrich_coin_metadata

except ImportError:
    # Фоллбэки
    logger = logging.getLogger(__name__)
    logging.error("Не удалось импортировать зависимости для aggregation_8h.")
    def get_interval_duration_ms(tf: str) -> int: 
        # Фоллбэк, который возвращает реальные значения
        if tf == '4h': return 4 * 3600 * 1000
        if tf == '8h': return 8 * 3600 * 1000
        return 0
    def save_to_cache(tf, data): pass
    def merge_data(data): return {}
    def format_final_structure(data, coins, tf): return {}
    def _enrich_coin_metadata(data, coins): return {}
# --------------------------------------------------------------------


def _aggregate_4h_to_8h(candles_4h: List[Dict], data_type: str) -> List[Dict]:
    """
    (Перенесено из data_processing.py)
    Агрегирует список 4-часовых свечей (Klines или OI) в 8-часовые,
    СТРОГО ПРИДЕРЖИВАЯСЬ 8-часовой UTC сетки (00:00, 08:00, 16:00).
    
    ПРАВИЛО АГРЕГАЦИИ OI:
    OI для 8h-свечи берется из *второй* 4h-свечи (candle2.openInterest),
    что соответствует снапшоту на конец 8h-периода.
    """
    if not candles_4h or len(candles_4h) < 2:
        return []

    candles_8h = []
    four_hours_ms = get_interval_duration_ms('4h')
    eight_hours_ms = get_interval_duration_ms('8h')

    if four_hours_ms == 0 or eight_hours_ms == 0:
        logger.error(f"AGGREGATE_8h [{data_type}]: Не удалось получить длительность интервалов (4h/8h). Агрегация невозможна.")
        return []

    i = 0
    while i < len(candles_4h) - 1:
        candle1 = candles_4h[i]

        if 'openTime' not in candle1:
            i += 1
            continue

        # --- ПРОВЕРКА UTC СЕТКИ ---
        # Нас интересуют только 4h свечи, которые начинаются в 00:00, 08:00, или 16:00 UTC.
        # (openTime 8h свечи) % (8 * 3600 * 1000) == 0
        is_on_8h_grid = (candle1['openTime'] % eight_hours_ms == 0)

        if not is_on_8h_grid:
            i += 1 # Эта 4h свеча (напр., 04:00 или 12:00) не может быть началом 8h свечи
            continue
        # --------------------------

        # Мы нашли свечу, начинающуюся в 00:00, 08:00 или 16:00.
        # Теперь ищем ее пару (которая должна быть в 04:00, 12:00 или 20:00)
        candle2 = candles_4h[i+1]
        if 'openTime' not in candle2:
            i += 1 # У candle1 нет валидной пары, пропускаем ее
            continue

        # Проверяем, что свечи идут подряд (допуск 1 минута, как в старой функции)
        if abs(candle2['openTime'] - candle1['openTime'] - four_hours_ms) > 60 * 1000:
            # Напр., у нас есть 00:00, но 04:00 отсутствует, а следующая свеча 08:00.
            # Мы не можем создать 8h свечу 00:00-08:00.
            logger.warning(f"AGGREGATE_8h [{data_type}]: Пропуск свечи {candle1['openTime']}. Найдена свеча на UTC сетке, но отсутствует смежная 4h свеча.")
            i += 1 # Пропускаем candle1, и следующая итерация начнется с candle2 (которая м.б. 08:00)
            continue

        # --- Свечи 1 и 2 валидны и образуют 8h свечу ---
        open_time_8h = candle1['openTime']
        close_time_8h = open_time_8h + eight_hours_ms - 1

        agg_candle = {
            "openTime": open_time_8h,
            "closeTime": close_time_8h
        }

        if data_type == 'klines':
            # (Логика агрегации Klines не изменилась)
            c1_h = candle1.get('highPrice')
            c2_h = candle2.get('highPrice')
            c1_l = candle1.get('lowPrice')
            c2_l = candle2.get('lowPrice')
            c1_v = candle1.get('volume', 0)
            c2_v = candle2.get('volume', 0)

            # Проверяем, что все нужные значения есть
            if None in (candle1.get('openPrice'), candle2.get('closePrice'), c1_h, c2_h, c1_l, c2_l):
                logger.debug(f"AGGREGATE_8h [Klines]: Пропуск свечи {open_time_8h} из-за None в OHLCV.")
                i += 2 # Пропускаем обе свечи
                continue

            agg_candle.update({
                "openPrice": candle1['openPrice'],
                "highPrice": max(c1_h, c2_h),
                "lowPrice": min(c1_l, c2_l),
                "closePrice": candle2['closePrice'],
                "volume": round(c1_v + c2_v, 2) # Суммируем объемы
            })

        elif data_type == 'oi':
            # --- ВАЖНО: Логика агрегации OI (взять candle2) ---
            oi_value = candle2.get('openInterest')
            # ------------------------------------------------
            if oi_value is not None:
                agg_candle["openInterest"] = oi_value
            else:
                pass # Убрали лог
                i += 2 # Пропускаем обе свечи
                continue

        else: # Неизвестный тип данных
            i += 2
            continue

        candles_8h.append(agg_candle)
        i += 2 # Мы успешно обработали 2 свечи
        # --- Конец цикла while ---

    return candles_8h


async def generate_and_save_8h_cache(data_4h_enriched: Dict, coins_from_db_8h: List[Dict]):
    """
    (Перенесено из data_processing.py)
    Берет готовые, ОБОГАЩЕННЫЕ данные 4h,
    агрегирует их в 8h, форматирует,
    обогащает метаданными 8h (из coins_from_db_8h)
    и сохраняет в кэш '8h'.
    """
    logger.info("[8H_GEN] Начинаю генерацию данных 8h из обогащенных 4h...")
    start_time = time.time()

    processed_data_8h = defaultdict(dict)
    processed_count = 0
    symbols_with_data = 0

    # Используем глубокое копирование, чтобы не изменять data_4h_enriched
    original_data_4h_list = copy.deepcopy(data_4h_enriched.get('data', []))

    if not original_data_4h_list:
        logger.warning("[8H_GEN] Нет данных 'data' в исходных 4h (для 8h-монет). Генерация 8h невозможна.")
        return

    for coin_data_4h in original_data_4h_list:
        symbol = coin_data_4h.get('symbol')
        # Данные 4h уже должны быть обогащены (метаданными 8h), но могут быть неполными
        candles_4h = coin_data_4h.get('data', []) # Это уже обрезанные 399 свечей 4h
        if not symbol or not candles_4h:
            continue
        symbols_with_data += 1

        # 1. Извлекаем FR из 4h (они не агрегируются)
        frs_4h = [{ "openTime": c['openTime'],
                    "fundingRate": c.get('fundingRate')}
                   for c in candles_4h if 'fundingRate' in c and c.get('fundingRate') is not None]

        # 2. Агрегируем Klines и OI из 4h в 8h
        #    (Эта функция использует правильную UTC сетку и логику OI)
        klines_8h = _aggregate_4h_to_8h(candles_4h, 'klines')
        ois_8h = _aggregate_4h_to_8h(candles_4h, 'oi')

        if not klines_8h:
             continue # FR и OI без Klines бесполезны

        processed_data_8h[symbol]['klines'] = klines_8h
        processed_data_8h[symbol]['oi'] = ois_8h
        processed_data_8h[symbol]['fr'] = frs_4h # Используем исходные FR
        processed_count += 1

    logger.info(f"[8H_GEN] Агрегация 4h->8h завершена для {processed_count} из {symbols_with_data} монет с данными.")

    if not processed_data_8h:
        logger.error("[8H_GEN] Нет данных для обработки после агрегации. Кэш 8h не будет создан.")
        return

    # 3. Слияние агрегированных Klines/OI (8h) с исходными FR (4h)
    logger.info(f"[8H_GEN] Начинаю слияние данных 8h...")
    # (Используем data_processing.merge_data)
    merged_8h_data = merge_data(processed_data_8h)
    logger.info(f"[8H_GEN] Слияние данных 8h завершено для {len(merged_8h_data)} монет.")

    if not merged_8h_data:
        logger.error("[8H_GEN] Нет данных после слияния 8h. Кэш 8h не будет создан.")
        return

    # 4. Финальное форматирование для 8h (включая обрезку до 399 свечей)
    logger.info("[8H_GEN] Начинаю форматирование структуры 8h...")
    # (Используем data_processing.format_final_structure)
    formatted_8h_data = format_final_structure(merged_8h_data, coins_from_db_8h, '8h')
    logger.info(f"[8H_GEN] Форматирование структуры 8h завершено ({len(formatted_8h_data.get('data',[]))} монет).")

    # 5. Обогащение метаданными (используем список 8h)
    logger.info("[8H_GEN] Начинаю обогащение метаданных 8h...")
    # (Используем data_processing._enrich_coin_metadata)
    final_enriched_8h_data = _enrich_coin_metadata(formatted_8h_data, coins_from_db_8h)
    logger.info("[8H_GEN] Обогащение метаданных 8h завершено.")

    # 6. Сохранение в кэш '8h'
    save_to_cache('8h', final_enriched_8h_data)

    end_time = time.time()
    logger.info(f"[8H_GEN] Весь процесс генерации и сохранения кэша 8h занял {end_time - start_time:.2f} сек.")
