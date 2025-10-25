"""
Этот модуль отвечает за обработку (слияние, форматирование)
данных после их сбора и парсинга.
"""
import logging
from typing import List, Dict, Any
from collections import defaultdict

from .logging_setup import logger 


def merge_data(processed_data: Dict[str, Dict[str, list]]) -> Dict[str, list]:
    """
    Объединяет klines, oi и fr данные для каждой монеты.
    (Ранее _merge_data в data_collector.py)
    """
    final_data = {}
    for symbol, data_types in processed_data.items():
        # Получаем и сортируем данные
        klines = sorted(data_types.get('klines', []), key=lambda x: x['openTime'])
        ois = sorted(data_types.get('oi', []), key=lambda x: x['openTime'])
        frs = sorted(data_types.get('fr', []), key=lambda x: x['openTime'])

        if not klines:
            # logger.warning(f"MERGE: Нет klines для {symbol}, пропуск.")
            # ^^^ Этот лог теперь не нужен, т.к. аудит ниже его перехватит
            continue

        merged_klines = []
        oi_idx, fr_idx = 0, 0

        # --- ИЗМЕНЕНИЕ: Логируем только проблемы ---
        has_oi = bool(ois)
        has_fr = bool(frs)
        
        # УДАЛЕНО: logger.info(f"MERGE: Для {symbol} получено OI: {has_oi}...")

        # Логируем, только если чего-то не хватает
        if not has_oi or not has_fr:
            missing_parts = []
            if not has_oi:
                missing_parts.append("OI")
            if not has_fr:
                missing_parts.append("FR")
            
            # Этот лог менее важен, т.к. аудит ниже его перехватит, но оставим
            logger.warning(f"MERGE: Для {symbol} отсутствуют данные: {', '.join(missing_parts)}. (Klines: {len(klines)}, OI: {len(ois)}, FR: {len(frs)})")
        # --- Конец изменения ---

        for kline in klines:
            # Находим последнее актуальное значение OI
            while oi_idx < len(ois) - 1 and ois[oi_idx + 1]['openTime'] <= kline['openTime']:
                oi_idx += 1
            if ois and ois[oi_idx]['openTime'] <= kline['openTime']:
                # Добавляем все ключи из OI, кроме временных меток
                kline.update({k:v for k,v in ois[oi_idx].items() if k not in ['openTime', 'closeTime']})

            # Находим последнее актуальное значение FR
            while fr_idx < len(frs) - 1 and frs[fr_idx + 1]['openTime'] <= kline['openTime']:
                fr_idx += 1
            if frs and frs[fr_idx]['openTime'] <= kline['openTime']:
                 # Добавляем все ключи из FR, кроме временных меток
                 kline.update({k:v for k,v in frs[fr_idx].items() if k not in ['openTime', 'closeTime']})

            merged_klines.append(kline)
        
        final_data[symbol] = merged_klines
    return final_data

def format_final_structure(market_data: Dict[str, list], coins: List[Dict], timeframe: str) -> Dict[str, Any]:
    """
    Форматирует собранные данные в финальную структуру с метаданными
    И ВСТРАИВАЕТ В ОТВЕТ 'audit_report'.
    """
    
    # --- БЛОК АУДИТА (перенесен из worker.py) ---
    missing_klines_symbols = []
    missing_oi_symbols = []
    missing_fr_symbols = []

    # 1. Проверка Klines
    expected_symbols = {c['symbol'].split(':')[0] for c in coins}
    actual_symbols = {symbol for symbol, klines in market_data.items() if klines}
    
    missing_klines_symbols = sorted(list(expected_symbols - actual_symbols))

    # 2. Проверка OI и FR (только для тех, у кого klines есть)
    for symbol, candles in market_data.items():
        if not candles:
            continue # Уже учтено в missing_klines

        # Проверяем только последнюю, самую актуальную свечу
        last_candle = candles[-1]
        
        if 'openInterest' not in last_candle:
            missing_oi_symbols.append(symbol)
            
        if 'fundingRate' not in last_candle:
            missing_fr_symbols.append(symbol)

    # 3. Создаем audit_report (как вы просили)
    audit_report = {
        "missing_klines": missing_klines_symbols,
        "missing_oi": sorted(missing_oi_symbols),
        "missing_fr": sorted(missing_fr_symbols)
    }

    # 4. Логирование результатов аудита (для консоли)
    total_expected = len(expected_symbols)
    total_actual = len(actual_symbols)
    
    if missing_klines_symbols:
        logging.warning(f"AUDIT ({timeframe}) [KLINES]: Отсутствуют Klines для {len(missing_klines_symbols)} из {total_expected} монет: {missing_klines_symbols}")
    
    if missing_oi_symbols:
        logging.warning(f"AUDIT ({timeframe}) [OI]: Отсутствует Open Interest (в последней свече) для {len(missing_oi_symbols)} монет: {sorted(missing_oi_symbols)}")
    
    if missing_fr_symbols:
        logging.warning(f"AUDIT ({timeframe}) [FR]: Отсутствует Funding Rate (в последней свече) для {len(missing_fr_symbols)} монет: {sorted(missing_fr_symbols)}")

    if not missing_klines_symbols and not missing_oi_symbols and not missing_fr_symbols:
        # logging.info(f"AUDIT ({timeframe}): Все {total_actual} монет ({total_expected} ожидалось) успешно прошли полную проверку (Klines, OI, FR).") # <-- УДАЛЕН ШУМ
        pass # Успех больше не логируем

    # --- КОНЕЦ БЛОКА АУДИТА ---


    # --- Логика форматирования (осталась без изменений) ---
    all_candles = [candle for coin_candles in market_data.values() for candle in coin_candles if coin_candles]
    if not all_candles:
        logger.warning(f"FORMAT: Нет свечей для форматирования (таймфрейм {timeframe}).")
        return {
            "openTime": None,
            "closeTime": None,
            "timeframe": timeframe,
            "audit_report": audit_report, # Добавляем отчет даже в пустой ответ
            "data": []
        }

    min_open_time = min(c['openTime'] for c in all_candles)
    max_close_time = max(c['closeTime'] for c in all_candles)
    
    # Создаем карту для быстрого поиска бирж по символу
    exchanges_map = {c['symbol'].split(':')[0]: c['exchanges'] for c in coins}

    data_list = []
    # Используем market_data.items() (т.к. actual_symbols уже посчитан)
    for symbol, candles in market_data.items():
        if candles: # Добавляем монету только если для нее есть данные
            data_list.append({
                "symbol": symbol,
                "exchanges": exchanges_map.get(symbol, []), # Получаем биржи из карты
                "data": candles
            })

    # Сортируем итоговый список по символу для консистентности
    data_list.sort(key=lambda x: x['symbol'])

    # logger.info(f"FORMAT: Данные успешно отформатированы. {len(data_list)} монет.") # <-- УДАЛЕН ШУМ

    return {
        "openTime": min_open_time,
        "closeTime": max_close_time,
        "timeframe": timeframe,
        "audit_report": audit_report, # --- ДОБАВЛЕНО ---
        "data": data_list
    }

