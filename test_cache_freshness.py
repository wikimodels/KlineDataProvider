import httpx
import asyncio
import sys
import logging
import time
import os 
from dotenv import load_dotenv

# --- 1. Загрузка конфигурации ---
load_dotenv()  
BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000") 

# --- 2. Настройки теста ---
CACHE_KEYS_TO_TEST = ["1h", "4h", "8h", "12h", "1d"]

# Добавляем 15 минут (в мс) к интервалу, чтобы дать время
# cron-задаче и серверу на запуск и выполнение.
GRACE_PERIOD_MS = 15 * 60 * 1000 
# -----------------

# --- 3. Настройка логгера ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("CACHE_FRESHNESS_TEST")

# --- 4. Вспомогательная функция (из api_helpers.py) ---
def get_interval_duration_ms(interval: str) -> int:
    """
    Возвращает длительность интервала в миллисекундах.
    (Скопировано из api_helpers.py для автономности теста)
    """
    duration_map = {
        '1h': 60 * 60 * 1000,
        '4h': 4 * 60 * 60 * 1000,
        '8h': 8 * 60 * 60 * 1000,
        '12h': 12 * 60 * 60 * 1000,
        '1d': 24 * 60 * 60 * 1000,
    }
    return duration_map.get(interval, 0)

# ============================================================================
# === ГЛАВНЫЙ СКРИПТ ===
# ============================================================================

async def run_freshness_test():
    """
    Проверяет "свежесть" (актуальность) всех Klines-кэшей.
    """
    log.info("--- 🚀 НАЧИНАЮ E2E ТЕСТ СВЕЖЕСТИ КЭША ---")
    log.info(f"Цель: {BASE_URL}")
    
    all_fresh = True
    # --- (ИЗМЕНЕНИЕ) 'current_utc_time_ms' ПЕРЕМЕЩЕНО В ЦИКЛ ---

    # Таймаут увеличен до 120 секунд (для загрузки больших кэшей 8h)
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=120.0) as client:
        
        # --- Шаг 1: Проверка, что сервер жив ---
        try:
            response = await client.get("/health")
            response.raise_for_status()
            log.info("✅ [OK] Сервер доступен.")
        except (httpx.ConnectError, httpx.HTTPStatusError) as e:
            log.error(f"💥 [FAIL] Не удалось подключиться к серверу: {e}")
            return

        # --- Шаг 2: Проверка всех кэшей ---
        for key in CACHE_KEYS_TO_TEST:
            log.info(f"--- 🔬 Проверяю свежесть 'cache:{key}' ---")
            
            try:
                response = await client.get(f"/cache/{key}")
                
                if response.status_code == 404:
                    log.error(f"💥 [FAIL] 'cache:{key}' не найден (404).")
                    all_fresh = False
                    continue
                
                response.raise_for_status()
                data = response.json()

                # 1. Получаем 'closeTime'
                last_close_time_ms = data.get("closeTime")
                if not last_close_time_ms:
                    log.error(f"💥 [FAIL] 'cache:{key}' не содержит 'closeTime' в корне.")
                    all_fresh = False
                    continue

                # 2. Рассчитываем интервалы
                interval_ms = get_interval_duration_ms(key)
                if interval_ms == 0:
                    log.error(f"💥 [FAIL] Неизвестный интервал для ключа '{key}'.")
                    all_fresh = False
                    continue
                
                allowed_staleness_ms = interval_ms + GRACE_PERIOD_MS
                
                # --- (ИЗМЕНЕНИЕ) Время 'сейчас' фиксируется ПЕРЕД СРАВНЕНИЕМ ---
                current_utc_time_ms = int(time.time() * 1000)
                time_diff_ms = current_utc_time_ms - last_close_time_ms
                # ----------------------------------------------------------
                
                # 3. Сравниваем (Логика с 4 состояниями)
                if time_diff_ms < 0:
                     log.error(f"💥 [FAIL] 'cache:{key}' из будущего? (Разница: {time_diff_ms} мс). Проверьте системное время.")
                     all_fresh = False
                
                elif time_diff_ms <= interval_ms:
                    # 1. ИДЕАЛЬНО СВЕЖИЕ
                    log.info(f"       ✅ [OK] 'cache:{key}' актуален (Данные: {time_diff_ms / 1000 / 3600:.1f} ч. назад).")

                elif time_diff_ms <= allowed_staleness_ms:
                    # 2. GRACE PERIOD (Все еще ОК, но с предупреждением)
                    staleness_minutes = (time_diff_ms - interval_ms) / 1000 / 60
                    log.warning(f"       ⚠️  [ПРЕДУПРЕЖДЕНИЕ] 'cache:{key}' находится в 'grace period' ({GRACE_PERIOD_MS / 1000 / 60:.0f} мин).")
                    log.warning(f"       Данные старше интервала на: {staleness_minutes:.1f} мин.")
                
                else:
                    # 3. ПРОТУХШИЕ
                    log.error(f"💥 [FAIL] 'cache:{key}' ПРОТУХ!")
                    log.error(f"       Последние данные: {time_diff_ms / 1000 / 3600:.1f} часов назад.")
                    log.error(f"       Допустимо (интервал + буфер 15 мин): {allowed_staleness_ms / 1000 / 3600:.1f} часов назад.")
                    all_fresh = False

            except Exception as e:
                log.error(f"💥 [FAIL] Ошибка при проверке 'cache:{key}': {e}", exc_info=True)
                all_fresh = False

        # --- Финальный вердикт ---
        if all_fresh:
            log.info("--- 🏆🏆🏆 ТЕСТ СВЕЖЕСТИ УСПЕШНО ЗАВЕРШЕН! Все кэши актуальны. ---")
        else:
            log.error("--- 💥 E2E ТЕСТ СВЕЖЕСТИ ПРОВАЛЕН. Найдены протухшие данные. ---")
            sys.exit(1) # Выходим с кодом ошибки


if __name__ == "__main__":
    try:
        asyncio.run(run_freshness_test())
    except KeyboardInterrupt:
        log.warning("Тест свежести прерван вручную.")