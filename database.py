import os
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import AsIs, register_adapter
import logging
from typing import List, Dict, Any
import numpy as np

# Адаптер для numpy.int64
register_adapter(np.int64, AsIs)

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection() -> Any:
    """Устанавливает и возвращает соединение с базой данных PostgreSQL."""
    if not DATABASE_URL:
        logging.error("DATABASE_URL не установлена.")
        raise ValueError("DATABASE_URL не установлена.")
    return psycopg2.connect(DATABASE_URL)

def get_coins_from_db(timeframe: str) -> List[Dict]:
    """
    Получает общие данные о монетах + данные для КОНКРЕТНОГО таймфрейма.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # --- ИЗМЕНЕНИЕ: Оборачиваем имена в кавычки для сохранения регистра ---
        common_fields = [
            "symbol", "exchanges", '"logoUrl"', "category",
            "volatility_index", "efficiency_index", "trend_harmony_index",
            "btc_correlation", "returns_skewness", "avg_wick_ratio",
            "relative_strength_vs_btc", "max_drawdown_percent"
        ]
        
        timeframe_fields = [
            f"hurst_{timeframe}",
            f"entropy_{timeframe}"
        ]
        
        all_fields = ", ".join(common_fields + timeframe_fields)
        
        query = f"""
            SELECT {all_fields}
            FROM monthly_coin_selection
            ORDER BY created_at DESC
        """
        
        logging.info(f"DATABASE: Выполняется запрос для таймфрейма '{timeframe}'...")
        cursor.execute(query)
        coins = cursor.fetchall()
        logging.info(f"DATABASE: Получено {len(coins)} монет из БД.")
        return coins
        
    except psycopg2.Error as e:
        logging.error(f"Ошибка Psycopg2 при получении монет из БД: {e.pgcode} - {e.pgerror}")
        return []
    except Exception as e:
        logging.error(f"Неизвестная ошибка при получении списка монет из БД: {e}")
        return []
    finally:
        if conn:
            conn.close()

