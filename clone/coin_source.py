import httpx 
import asyncio
import logging
from typing import List, Dict, Any, Optional

from config import (
    COIN_SIFTER_BASE_URL,
    COIN_SIFTER_ENDPOINT_PATH,
    COIN_SIFTER_API_TOKEN,
    COIN_PROCESSING_LIMIT
)

logger = logging.getLogger(__name__)
REQUEST_TIMEOUT = 15

# --- ИЗМЕНЕНИЕ: Основная функция возвращает оригинальное имя ---
async def get_coins_from_api() -> Optional[List[Dict[str, Any]]]:
    """
    Получает список монет (symbol, exchanges) из API 'coin-sifter',
    применяет лимит и возвращает его.
    (Название функции сохранено для совместимости с fr_fetcher.py)
    """
    
    if not COIN_SIFTER_BASE_URL or not COIN_SIFTER_API_TOKEN:
        logger.critical("[COIN_SOURCE] COIN_SIFTER_URL или SECRET_TOKEN не установлены в .env.")
        return None

    full_url = f"{COIN_SIFTER_BASE_URL.rstrip('/')}/{COIN_SIFTER_ENDPOINT_PATH.lstrip('/')}"
    
    headers = {
        "X-Auth-Token": COIN_SIFTER_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    logger.info(f"[COIN_SOURCE] Запрашиваю список монет из {full_url}...")

    try:
        async with httpx.AsyncClient(headers=headers, timeout=REQUEST_TIMEOUT) as client:
            response = await client.get(full_url)
            
            if response.status_code == 200:
                data = response.json()
                coin_list = data.get("symbols")
                
                if coin_list is None:
                    logger.error(f"[COIN_SOURCE] API вернуло 200 OK, но ключ 'symbols' отсутствует в ответе.")
                    return None
                
                total_found = len(coin_list)
                limited_list = coin_list[:COIN_PROCESSING_LIMIT]
                
                logger.info(f"[COIN_SOURCE] Успешно получено {total_found} монет.")
                logger.info(f"[COIN_SOURCE] Применяю лимит: {COIN_PROCESSING_LIMIT} монет.")
                return limited_list
            
            elif response.status_code in [401, 403]:
                logger.error(f"[COIN_SOURCE] Ошибка {response.status_code} (Unauthorized/Forbidden). Проверьте SECRET_TOKEN (.env).")
                return None
            else:
                response_text = response.text
                logger.error(f"[COIN_SOURCE] Ошибка API: Статус {response.status_code}. Ответ: {response_text[:150]}...")
                return None

    except httpx.TimeoutException:
        logger.error(f"[COIN_SOURCE] Таймаут при запросе к API монет ({full_url}).")
        return None
    except httpx.ConnectError as e:
        logger.error(f"[COIN_SOURCE] Ошибка соединения. Не удалось подключиться к {e.request.url.host}.")
        return None
    except Exception as e:
        logger.error(f"[COIN_SOURCE] Непредвиденная ошибка при получении монет: {e}", exc_info=True)
        return None

# --- НОВОЕ: Алиасы для совместимости с worker.py ---
# worker.py импортирует эти имена
get_coins = get_coins_from_api 
get_coins_fr = get_coins_from_api