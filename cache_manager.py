import zlib
import json
import logging
import os
import base64  # <-- ДОБАВЛЕН ИМПОРТ
from typing import Dict, Any, Optional

from upstash_redis import Redis 
from dotenv import load_dotenv
load_dotenv() 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- ИСПОЛЬЗУЕМ UPSTASH REDIS ---
try:
    upstash_url = os.environ.get("UPSTASH_REDIS_URL")
    upstash_token = os.environ.get("UPSTASH_REDIS_TOKEN")
    
    if not upstash_url or not upstash_token:
        raise ValueError("Переменные UPSTASH_REDIS_URL или UPSTASH_REDIS_TOKEN не найдены в окружении.")
    
    redis_client = Redis(url=upstash_url, token=upstash_token) 
    
    redis_client.ping()
    logging.info("Успешное подключение к Upstash Redis.")

except ValueError as e:
    logging.critical(f"ОШИБКА КОНФИГУРАЦИИ UPSTASH REDIS: {e}")
    redis_client = None
except Exception as e: 
    logging.critical(f"НЕ УДАЛОСЬ ПОДКЛЮЧИТЬСЯ К UPSTASH REDIS: {e}")
    logging.critical("Проверьте URL/TOKEN в .env или настройки сети.")
    redis_client = None


def save_to_cache(timeframe: str, data: Dict[str, Any]):
    """
    Сериализует, сжимает, кодирует в Base64 и сохраняет данные в Upstash Redis.
    """
    if not redis_client:
        logging.error("Upstash Redis недоступен. Сохранение в кэш пропущено.")
        return
        
    try:
        json_string = json.dumps(data)
        byte_data = json_string.encode('utf-8')
        compressed_data = zlib.compress(byte_data)
        
        # --- ИЗМЕНЕНИЕ: Кодируем байты в Base64 строку ---
        base64_string = base64.b64encode(compressed_data).decode('utf-8')
        
        key = f"cache:{timeframe}"
        redis_client.set(key, base64_string) # Сохраняем строку
        
        logging.info(f"Данные для таймфрейма '{timeframe}' успешно сохранены в Upstash Redis (ключ: {key}). Размер (строка Base64): {len(base64_string)} байт.")
    except Exception as e:
        logging.error(f"Ошибка при сохранении данных в Upstash Redis для '{timeframe}': {e}", exc_info=True) # Добавлен exc_info


def get_from_cache(timeframe: str) -> Optional[Dict[str, Any]]:
    """
    Извлекает, декодирует из Base64, распаковывает данные из Upstash Redis.
    """
    if not redis_client:
        logging.error("Upstash Redis недоступен. Чтение из кэша пропущено.")
        return None
        
    try:
        key = f"cache:{timeframe}"
        base64_string = redis_client.get(key) # Получаем строку

        if base64_string:
            # --- ИЗМЕНЕНИЕ: Декодируем строку Base64 обратно в байты ---
            compressed_data = base64.b64decode(base64_string)
            
            decompressed_data = zlib.decompress(compressed_data) 
            json_string = decompressed_data.decode('utf-8')
            data = json.loads(json_string)
            logging.info(f"Данные для таймфрейма '{timeframe}' успешно извлечены из Upstash Redis (ключ: {key}).")
            return data
        else:
            logging.warning(f"Кэш в Upstash Redis для таймфрейма '{timeframe}' пуст (ключ: {key}).")
            return None
            
    except Exception as e:
        logging.error(f"Ошибка при извлечении данных из Upstash Redis для '{timeframe}': {e}", exc_info=True) # Добавлен exc_info
        return None

