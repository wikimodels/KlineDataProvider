import zlib
import json
import logging
from typing import Dict, Any, Optional

# Глобальный словарь для хранения кэшей в памяти
# Ключ - таймфрейм (e.g., '4h'), значение - сжатые данные
_caches: Dict[str, bytes] = {}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_to_cache(timeframe: str, data: Dict[str, Any]):
    """
    Сериализует, сжимает и сохраняет данные в кэш для указанного таймфрейма.
    """
    try:
        # 1. Конвертируем словарь в JSON-строку, затем в байты
        json_string = json.dumps(data)
        byte_data = json_string.encode('utf-8')

        # 2. Сжимаем байтовые данные с помощью zlib
        compressed_data = zlib.compress(byte_data)

        # 3. Сохраняем в кэш
        _caches[timeframe] = compressed_data
        logging.info(f"Данные для таймфрейма '{timeframe}' успешно сохранены в кэш. Размер: {len(compressed_data)} байт.")

    except Exception as e:
        logging.error(f"Ошибка при сохранении данных в кэш для '{timeframe}': {e}")

def get_from_cache(timeframe: str) -> Optional[Dict[str, Any]]:
    """
    Извлекает и распаковывает данные из кэша для указанного таймфрейма.
    """
    if timeframe in _caches:
        try:
            # 1. Получаем сжатые данные из кэша
            compressed_data = _caches[timeframe]

            # 2. Распаковываем данные
            decompressed_data = zlib.decompress(compressed_data)

            # 3. Конвертируем байты обратно в словарь
            json_string = decompressed_data.decode('utf-8')
            data = json.loads(json_string)
            
            logging.info(f"Данные для таймфрейма '{timeframe}' успешно извлечены из кэша.")
            return data
        except Exception as e:
            logging.error(f"Ошибка при извлечении данных из кэша для '{timeframe}': {e}")
            return None
    else:
        logging.warning(f"Кэш для таймфрейма '{timeframe}' пуст.")
        return None
