import os
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import AsIs, register_adapter
import logging
from typing import List, Dict, Any, Optional
import numpy as np

# Адаптер для numpy.int64
register_adapter(np.int64, AsIs)

# Инициализация логгера
logger = logging.getLogger(__name__)

# DATABASE_URL = os.environ.get("DATABASE_URL")
# ^^^^
# !!! (ИЗМЕНЕНИЕ) УДАЛЯЕМ ЭТУ СТРОКУ С УРОВНЯ МОДУЛЯ !!!
# !!! ОНА БУДЕТ ПЕРЕМЕЩЕНА ВНУТРЬ get_db_connection() !!!
# ^^^^

def get_db_connection() -> Optional[Any]: # Возвращаем Optional[Connection]
    """Устанавливает и возвращает соединение с базой данных PostgreSQL."""
    
    # --- ИЗМЕНЕНИЕ: Получаем URL здесь, а не на уровне модуля ---
    # Это гарантирует, что load_dotenv() уже отработал,
    # даже если .env загружается после импорта этого модуля.
    DATABASE_URL = os.environ.get("DATABASE_URL")
    # --------------------------------------------------------

    if not DATABASE_URL:
        logger.critical("DATABASE_URL не установлена.")
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        logger.critical(f"Не удалось подключиться к базе данных по DATABASE_URL: {e}")
        return None
    except Exception as e:
        logger.critical(f"Непредвиденная ошибка при подключении к БД: {e}", exc_info=True)
        return None


def get_coins_from_db(timeframe: str) -> Optional[List[Dict]]: # Возвращаем Optional[List]
    """
    (ФИНАЛЬНОЕ ИСПРАВЛЕНИЕ): Получает САМУЮ ПОСЛЕДНЮЮ запись для КАЖДОЙ УНИКАЛЬНОЙ монеты
    со всеми аналитическими полями.
    Аргумент 'timeframe' используется только для логирования.
    """
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error(f"Не удалось получить соединение с БД (get_coins_from_db для {timeframe}).")
            return None 

        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # 1. Общие поля (ВСЕГДА)
        common_fields = [
            "symbol", "exchanges", '"logoUrl"', "category",
            "volatility_index", "efficiency_index", "trend_harmony_index",
            "btc_correlation", "returns_skewness", "avg_wick_ratio",
            "relative_strength_vs_btc", "max_drawdown_percent"
        ]
        
        # 2. Поля, специфичные для таймфрейма
        timeframe_fields = []
        # --- ИЗМЕНЕНИЕ: Запрашиваем ВСЕ поля, независимо от ТФ ---
        # Мы добавляем эти поля, только если они существуют в БД (предполагаем, что 4h/8h/12h/1d существуют)
        for tf_suffix in ['4h', '8h', '12h', '1d']:
            timeframe_fields.extend([
                f"hurst_{tf_suffix}",
                f"entropy_{tf_suffix}"
            ])
        # -----------------------------------------------------
        
        # 3. Объединяем и формируем финальный список полей
        all_fields = ", ".join(common_fields + timeframe_fields)
        
        # --- Используем DISTINCT ON (symbol) и ВСЕ поля ---
        query = f"""
            SELECT DISTINCT ON (symbol) {all_fields}
            FROM monthly_coin_selection
            ORDER BY symbol, created_at DESC;
        """
        
        # Используем 'timeframe' из аргумента для логирования
        logger.info(f"DATABASE: Выполняется запрос для *всех* монет (Klines/OI) (вызван из {timeframe})...")
        cursor.execute(query)
        coins = cursor.fetchall()
        coin_list = [dict(row) for row in coins]
        logger.info(f"DATABASE: Получено {len(coin_list)} монет из БД (вызван из {timeframe}).")
        return coin_list
        
    except psycopg2.Error as e:
        logger.error(f"Ошибка Psycopg2 при получении монет из БД: {e.pgcode} - {e.pgerror}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def get_all_unique_coins() -> Optional[List[Dict]]:
    """
    Получает список *всех* уникальных монет (symbol и exchanges)
    для глобального сборщика FR.
    """
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Не удалось получить соединение с БД (get_all_unique_coins).")
            return None 

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Этот запрос нужен fr_fetcher'у
        query = """
            SELECT DISTINCT ON (symbol) symbol, exchanges
            FROM monthly_coin_selection
            ORDER BY symbol, created_at DESC;
        """
        
        logger.info("DATABASE: Выполняется запрос для *всех* уникальных монет (для FR)...")
        cursor.execute(query)
        coins = cursor.fetchall()
        coin_list = [dict(row) for row in coins]
        logger.info(f"DATABASE: Получено {len(coin_list)} *уникальных* монет из БД.")
        return coin_list
        
    except psycopg2.Error as e:
        logger.error(f"Ошибка Psycopg2 при получении *всех* монет из БД: {e.pgcode} - {e.pgerror}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при получении *всех* монет из БД: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()

