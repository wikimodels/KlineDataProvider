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

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection() -> Optional[Any]: # Возвращаем Optional[Connection]
    """Устанавливает и возвращает соединение с базой данных PostgreSQL."""
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
    Получает общие данные о монетах + данные для КОНКРЕТНОГО таймфрейма.
    (Исправлено: не запрашивает hurst/entropy для '1h')
    """
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Не удалось получить соединение с БД.")
            return None 

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # --- Общие поля (common_fields) ---
        common_fields = [
            "symbol", "exchanges", '"logoUrl"', "category",
            "volatility_index", "efficiency_index", "trend_harmony_index",
            "btc_correlation", "returns_skewness", "avg_wick_ratio",
            "relative_strength_vs_btc", "max_drawdown_percent"
        ]
        
        # --- Изменение (Исправление для 1h) ---
        # Для '1h' у нас нет специальных полей hurst/entropy в БД
        if timeframe == '1h':
            timeframe_fields = []
            logger.info(f"DATABASE: Для '1h' запрашиваются только общие поля (common_fields).")
        else:
            timeframe_fields = [
                f"hurst_{timeframe}",
                f"entropy_{timeframe}"
            ]
        # -----------------------------------------
        
        all_fields = ", ".join(common_fields + timeframe_fields)
        
        # --- Запрос ---
        query = f"""
            SELECT {all_fields}
            FROM monthly_coin_selection
            ORDER BY created_at DESC
        """
        # ----------------
        
        logger.info(f"DATABASE: Выполняется запрос для таймфрейма '{timeframe}'...")
        cursor.execute(query)
        coins = cursor.fetchall()
        # Преобразуем RealDictRow в обычные dict
        coin_list = [dict(row) for row in coins]
        logger.info(f"DATABASE: Получено {len(coin_list)} монет из БД.")
        return coin_list
        
    except psycopg2.Error as e:
        logger.error(f"Ошибка Psycopg2 при получении монет из БД: {e.pgcode} - {e.pgerror}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Неизвестная ошибка при получении списка монет из БД: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def get_all_unique_coins() -> Optional[List[Dict]]:
    """
    (Код этой функции не изменен)
    Получает список *всех* уникальных монет (symbol и exchanges)
    из БД для глобального сборщика FR.
    Берет самую последнюю запись для каждой монеты.
    """
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Не удалось получить соединение с БД (get_all_unique_coins).")
            return None 

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
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
        logger.error(f"Неизвестная ошибка при получении *всех* списка монет из БД: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()

