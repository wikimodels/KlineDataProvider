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


def _is_8h_close_time_ms(close_time_ms: int) -> bool:
    """
    Проверяет, является ли close_time_ms моментом окончания 8h интервала.
    
    8h интервалы в UTC:
    - 00:00:00.000 - 07:59:59.999 (closeTime = 28799999)
    - 08:00:00.000 - 15:59:59.999 (closeTime = 57599999)
    - 16:00:00.000 - 23:59:59.999 (closeTime = 86399999)
    
    Логика: (closeTime + 1) должно делиться на 8h без остатка
    """
    eight_hours_ms = get_interval_duration_ms('8h')
    return (close_time_ms + 1) % eight_hours_ms == 0


def _aggregate_klines_4h_to_8h(candle1: Dict, candle2: Dict) -> Optional[Dict]:
    """
    Агрегирует две 4h Klines-свечи в одну 8h.
    
    Логика агрегации OHLCV:
    - Open: из первой свечи (начало 8h периода)
    - High: максимум из обеих свечей
    - Low: минимум из обеих свечей
    - Close: из второй свечи (конец 8h периода)
    - Volume: сумма объемов обеих свечей
    - VolumeDelta: сумма дельт объемов (buy - sell)
    
    Args:
        candle1: Первая 4h свеча (начало 8h периода)
        candle2: Вторая 4h свеча (конец 8h периода)
    
    Returns:
        Агрегированная 8h свеча или None при отсутствии обязательных данных
    """
    c1_h = candle1.get('highPrice')
    c2_h = candle2.get('highPrice')
    c1_l = candle1.get('lowPrice')
    c2_l = candle2.get('lowPrice')
    c1_v = candle1.get('volume', 0)
    c2_v = candle2.get('volume', 0)
    c1_vd = candle1.get('volumeDelta') or 0
    c2_vd = candle2.get('volumeDelta') or 0

    # Проверяем обязательные поля
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
    """
    Агрегирует две 4h OI-свечи в одну 8h.
    
    ЛОГИКА ДЛЯ BYBIT:
    Open Interest - это снимок состояния на момент времени, а не накопительная метрика.
    Берем OI из ВТОРОЙ свечи (candle2), так как это актуальное значение 
    на момент закрытия 8h периода.
    
    Почему вторая свеча:
    - OI показывает текущее количество открытых контрактов
    - Для анализа важно знать состояние на конец периода
    - Это согласуется с логикой closePrice (тоже берется из второй свечи)
    
    Args:
        candle1: Первая 4h свеча (используется только для openTime)
        candle2: Вторая 4h свеча (источник OI значения)
    
    Returns:
        Агрегированный OI для 8h периода или None при отсутствии данных
    """
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
    """
    Агрегирует фандинг рейты из двух 4h свечей в одну 8h.
    
    ЛОГИКА ДЛЯ BYBIT:
    На Bybit некоторые монеты имеют funding rate каждые 4 часа (не 8).
    Используем явную логику выбора FR для максимальной прозрачности.
    
    Почему последнее значение (Вариант 2: АКТУАЛЬНОСТЬ):
    - Показывает ТЕКУЩИЙ FR на момент закрытия 8h свечи
    - Важно для анализа sentiment: дорого ли держать позицию СЕЙЧАС
    - Помогает выявлять тренды изменения FR
    - Согласуется с логикой closePrice и OI (берем актуальное состояние)
    
    Приоритет выбора:
    1. Если FR есть во второй свече (candle2) → берем его (самое актуальное)
    2. Если FR только в первой свече (candle1) → берем его (фоллбэк)
    3. Если FR нет нигде → возвращаем None
    
    Важно: Явная проверка "is not None" критична для корректной обработки 
    нулевых значений FR (0.0 - валидное значение, означает нейтральный рынок).
    
    Args:
        candle1: Первая 4h свеча (фоллбэк источник FR)
        candle2: Вторая 4h свеча (основной источник FR)
    
    Returns:
        Агрегированный FR для 8h периода или None при отсутствии данных
    """
    # Явная логика для максимальной прозрачности
    fr1 = candle1.get('fundingRate')
    fr2 = candle2.get('fundingRate')
    
    # Приоритет: candle2 (актуальное), фоллбэк: candle1
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


def _build_8h_candles_from_end(candles_4h: List[Dict], data_type: str) -> List[Dict]:
    """
    Собирает 8h свечи из массива 4h свечей, начиная с конца списка.
    
    АЛГОРИТМ:
    1. Идем с конца массива назад (от новых свечей к старым)
    2. Берем пары последовательных 4h свечей
    3. Проверяем, что вторая свеча заканчивается на границе 8h интервала
    4. Агрегируем валидные пары в 8h свечи
    5. Разворачиваем результат (возвращаем от старых к новым)
    
    ПРОВЕРКИ (минималистичный подход):
    ✅ Наличие обязательных ключей (openTime, closeTime)
    ✅ Последовательность свечей: candle2.openTime == candle1.closeTime + 1
    ✅ Выравнивание по 8h сетке UTC: _is_8h_close_time_ms(candle2.closeTime)
    
    ОТСУТСТВУЮЩИЕ ПРОВЕРКИ (намеренно):
    ❌ НЕ проверяем длительность 4h свечей (closeTime - openTime == 4h - 1)
    
    Почему не проверяем длительность:
    - Данные от Bybit стабильны 4+ года - биржа надежная
    - Биржи могут давать микросдвиги (3h 59m 59s вместо ровно 4h)
    - Строгие проверки могут отбросить валидные данные
    - Проверка смежности + 8h границ уже гарантирует корректность
    - Меньше проверок = быстрее работает код
    - Риск терпим: даже при некорректных данных потеря минимальна
    
    Args:
        candles_4h: Список 4h свечей (должен быть отсортирован по времени)
        data_type: Тип данных ('klines', 'oi', 'fr')
    
    Returns:
        Список агрегированных 8h свечей (от старых к новым)
    """
    if not candles_4h or len(candles_4h) < 2:
        return []

    four_hours_ms = get_interval_duration_ms('4h')
    if four_hours_ms == 0:
        logger.error(f"AGGREGATE_8h: Не удалось получить длительность интервала 4h. Агрегация невозможна.")
        return []

    # Выбираем функцию агрегации в зависимости от типа данных
    aggregate_func = {
        'klines': _aggregate_klines_4h_to_8h,
        'oi': _aggregate_oi_4h_to_8h,
        'fr': _aggregate_funding_rates
    }.get(data_type)

    if not aggregate_func:
        logger.error(f"AGGREGATE_8h: Неизвестный тип данных '{data_type}'")
        return []

    result = []
    i = len(candles_4h) - 1

    # Идем с конца массива, агрегируя пары свечей
    while i >= 1:
        candle2 = candles_4h[i]     # Вторая свеча пары (конец 8h периода)
        candle1 = candles_4h[i - 1] # Первая свеча пары (начало 8h периода)

        # Защита от поврежденных данных: проверяем наличие ключей
        if 'openTime' not in candle2 or 'closeTime' not in candle1:
            i -= 1
            continue

        # Проверка 1: Свечи должны идти последовательно (без пропусков)
        # candle2 должна начинаться сразу после окончания candle1
        if candle2['openTime'] != candle1['closeTime'] + 1:
            i -= 1
            continue

        # Проверка 2: Вторая свеча должна заканчиваться на границе 8h интервала
        # (07:59:59.999, 15:59:59.999, 23:59:59.999 UTC)
        if not _is_8h_close_time_ms(candle2['closeTime']):
            i -= 1
            continue

        # Все проверки пройдены - агрегируем пару
        agg_candle = aggregate_func(candle1, candle2)
        if agg_candle:
            result.append(agg_candle)
        else:
            # Агрегация не удалась (отсутствуют обязательные поля в данных)
            logger.debug(
                f"AGGREGATE_8h [{data_type}]: Пропуск пары "
                f"{candle1.get('openTime')} - {candle2.get('closeTime')} "
                f"из-за некорректных данных."
            )

        # Переходим к следующей паре (назад на 2 свечи)
        i -= 2

    # Результат приходит в обратном порядке (от новых к старым)
    # Разворачиваем, чтобы вернуть от старых к новым
    result.reverse()
    return result


async def generate_and_save_8h_cache(data_4h: Dict, coins_from_api: List[Dict]):
    """
    Генерирует и сохраняет 8h кэш из 4h данных с правильным выравниванием по UTC сетке.
    
    ПРОЦЕСС:
    1. Берем 4h данные для всех монет
    2. Для каждой монеты агрегируем klines, OI и FR отдельно
    3. Объединяем данные через merge_data
    4. Форматируем в финальную структуру
    5. Сохраняем в кэш
    
    ЛОГИКА АГРЕГАЦИИ (см. функции выше):
    - Klines: OHLC + суммирование объемов
    - OI: берем из второй 4h свечи (актуальное состояние)
    - FR: берем из второй 4h свечи с фоллбэком на первую (актуальность)
    
    Args:
        data_4h: Словарь с 4h данными {'data': [{'symbol': '...', 'data': [...]}]}
        coins_from_api: Список монет с биржи
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

    # Обрабатываем каждую монету
    for coin_data_4h in original_data_4h_list:
        symbol = coin_data_4h.get('symbol')
        candles_4h = coin_data_4h.get('data', []) 
        
        # Пропускаем монеты без символа или данных, или с недостаточным количеством свечей
        if not symbol or not candles_4h or len(candles_4h) < 2:
            continue
        
        symbols_with_data += 1

        # Агрегируем разные типы данных отдельно
        klines_8h = _build_8h_candles_from_end(candles_4h, 'klines')
        ois_8h = _build_8h_candles_from_end(candles_4h, 'oi')
        frs_8h = _build_8h_candles_from_end(candles_4h, 'fr')

        # Если не удалось создать klines - пропускаем монету
        if not klines_8h:
             continue 

        # Сохраняем агрегированные данные
        processed_data_8h[symbol]['klines'] = klines_8h
        processed_data_8h[symbol]['oi'] = ois_8h
        processed_data_8h[symbol]['fr'] = frs_8h 
        processed_count += 1

    logger.info(f"[8H_GEN] Агрегация 4h->8h завершена для {processed_count} из {symbols_with_data} монет с данными.")

    if not processed_data_8h:
        logger.error("[8H_GEN] Нет данных для обработки после агрегации. Кэш 8h не будет создан.")
        return

    # Объединяем данные (klines, oi, fr) в единую структуру
    logger.info(f"[8H_GEN] Начинаю слияние данных 8h...")
    merged_8h_data = merge_data(processed_data_8h)
    logger.info(f"[8H_GEN] Слияние данных 8h завершено для {len(merged_8h_data)} монет.")

    if not merged_8h_data:
        logger.error("[8H_GEN] Нет данных после слияния 8h. Кэш 8h не будет создан.")
        return

    # Форматируем в финальную структуру
    logger.info("[8H_GEN] Начинаю форматирование структуры 8h...")
    formatted_8h_data = format_final_structure(merged_8h_data, coins_from_api, '8h')
    logger.info(f"[8H_GEN] Форматирование структуры 8h завершено ({len(formatted_8h_data.get('data',[]))} монет).")

    # Сохраняем в кэш
    save_to_cache('8h', formatted_8h_data)

    end_time = time.time()
    logger.info(f"[8H_GEN] Весь процесс генерации и сохранения кэша 8h занял {end_time - start_time:.2f} сек.")