"""
Этот пакет отвечает за полный цикл сбора, обработки и 
форматирования рыночных данных с бирж.

Основная функция - fetch_market_data()
"""
import asyncio
import aiohttp
from typing import List, Dict, Any
from collections import defaultdict
 

# Импортируем из модулей этого пакета
from . import task_builder
from . import data_processing
from .fetch_strategies import CONCURRENCY_LIMIT
from .logging_setup import logger, oi_fr_error_logger
async def fetch_market_data(coins: List[Dict], timeframe: str) -> Dict[str, Any]:
    """
    Основная функция-оркестратор.
    1. Готовит задачи
    2. Выполняет их параллельно
    3. Парсит результаты
    4. Объединяет данные
    5. Форматирует финальный ответ
    """
    # logger.info(f"--- Начинаю новый цикл сбора данных для таймфрейма: {timeframe} ---") # <-- УДАЛЕН ШУМ

    # 1. Подготовка задач
    # task_builder решает, какие URL, парсеры и стратегии использовать
    tasks_to_run = task_builder.prepare_tasks(coins, timeframe)
    if not tasks_to_run:
        logger.warning("Нет задач для выполнения.")
        return data_processing.format_final_structure({}, coins, timeframe)

    processed_data = defaultdict(dict)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    # 2. Выполнение задач
    async with aiohttp.ClientSession() as session:
        # Создаем список асинхронных вызовов
        async_tasks = []
        for task in tasks_to_run:
            strategy_func = task["fetch_strategy"]
            task_info = task["task_info"]
            async_tasks.append(strategy_func(session, task_info, semaphore))
        
        # Выполняем все задачи параллельно
        # results будет списком кортежей: [(task_info, raw_data_list), ...]
        try:
            # --- ИЗМЕНЕНИЕ 1: Добавлено return_exceptions=True ---
            # Это предотвратит падение gather() при первой ошибке
            # и не даст сессии закрыться преждевременно.
            results = await asyncio.gather(*async_tasks, return_exceptions=True)
            
        except Exception as e:
            # Этот блок теперь маловероятен, но сохранен для надежности
            logger.critical(f"Критическая ошибка во время asyncio.gather: {e}", exc_info=True)
            results = []

    # --- ИЗМЕНЕНИЕ 2: Полностью переработанный цикл парсинга ---
    # 3. Парсинг результатов (с обработкой исключений)
    failed_tasks_count = 0
    for task, result in zip(tasks_to_run, results):
        
        # Сначала извлекаем task_info из ОРИГИНАЛЬНОЙ задачи
        # Это нужно, чтобы знать, что упало, даже если result - это Exception
        task_info = task['task_info']
        symbol = task_info['symbol']
        data_type = task_info['data_type']
        exchange = task_info['exchange']

        try:
            # --- ШАГ 3.А: Проверка на корневую ошибку ---
            if isinstance(result, Exception):
                failed_tasks_count += 1
                # Это та самая "корневая" ошибка, которую вы просили отловить
                msg = f"FETCH_GATHER: Запрос провален: {symbol} ({data_type}, {exchange}). Ошибка: {result}. URL: {task_info.get('url', 'N/A')}"
                logger.error(msg)
                if data_type in ['oi', 'fr']:
                    oi_fr_error_logger.error(msg)
                continue # Переходим к следующей задаче

            # --- ШАГ 3.Б: Распаковка успешного результата ---
            # Если мы здесь, result - это кортеж (task_info, raw_data)
            # task_info из результата нам не нужен (он есть в task),
            # поэтому используем _ для первого элемента
            _, raw_data = result
            
            if not raw_data:
                msg = f"PARSER: Нет raw_data для {symbol} ({data_type}, {exchange}). Запрос, вероятно, не удался (но без исключения)."
                logger.warning(msg)
                if data_type in ['oi', 'fr']:
                    oi_fr_error_logger.warning(msg)
                continue
            
            # --- ШАГ 3.В: Безопасный парсинг ---
            parser_func = task['parser']
            timeframe_arg = task['timeframe']
            
            # Парсер получает либо список (для klines/binance)
            # либо уже объединенный список (для пагинации bybit)
            parsed_result = parser_func(raw_data, timeframe_arg)
            
            # --- ИСПРАВЛЕНИЕ: Добавлена логика сохранения данных ---
            if not parsed_result:
                logger.warning(f"PARSER: Парсер вернул пустой список для {symbol} ({data_type}, {exchange}). Raw data size: {len(raw_data)}")             
            else:
                # ЭТА СТРОКА ИСПРАВЛЯЕТ ОШИБКУ "Отсутствуют Klines"
                processed_data[symbol][data_type] = parsed_result

        except Exception as e:
            # Эта ошибка теперь ловит только ошибки ПАРСИНГА или РАСПАКОВКИ
            msg = f"PARSER: Ошибка парсинга {symbol} ({data_type}, {exchange}): {e}"
            logger.error(msg, exc_info=True)
            oi_fr_error_logger.error(msg)
    
    if failed_tasks_count > 0:
        logger.warning(f"FETCH_GATHER: Сбор данных завершен с {failed_tasks_count} (из {len(tasks_to_run)}) проваленными запросами. (Подробности см. в логах ERROR)")

    # 4. Объединение данных (Klines + OI + FR)
    merged_data = data_processing.merge_data(processed_data)
    
    # 5. Финальное форматирование
    final_structured_data = data_processing.format_final_structure(merged_data, coins, timeframe)
    
    # logger.info(f"--- Цикл сбора данных для {timeframe} завершен. ---") # <-- УДАЛЕН ШУМ
    return final_structured_data

