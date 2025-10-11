import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import List, Dict, Any

# Получаем URL из переменных окружения
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection() -> Any:
    """
    Устанавливает и возвращает соединение с базой данных PostgreSQL.
    """
    if not DATABASE_URL:
        logging.error("DATABASE_URL не установлена в переменных окружения.")
        raise ValueError("DATABASE_URL не установлена.")
    return psycopg2.connect(DATABASE_URL)

def get_coins_from_db() -> List[Dict]:
    """
    Получает список монет из таблицы monthly_coin_selection.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        # Выбираем только нужные для работы поля
        cursor.execute("SELECT symbol, exchanges FROM monthly_coin_selection ORDER BY created_at DESC")
        coins = cursor.fetchall()
        return coins
    except Exception as e:
        logging.error(f"Ошибка при получении списка монет из БД: {e}")
        return []
    finally:
        if conn:
            conn.close()
