# test_cache_freshness.py
import httpx
import asyncio
import sys
import logging
import time
import os 
import json 
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional 

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
# === ХЕЛПЕРЫ ДЛЯ УГЛУБЛЕННОЙ ПРОВЕРКИ 8H ===
# ============================================================================

def _find_coin_data(symbol: str, cache_data: Dict) -> Optional[Dict]:
    """Находит данные монеты по символу в кэше."""
    for coin in cache_data.get("data", []):
        if coin.get("symbol") == symbol:
            return coin
    return None

def _find_candle_by_opentime(candles_list: List[Dict], open_time: int) -> Optional[Dict]:
    """Находит свечу по openTime в списке свечей."""
    for candle in candles_list:
        if candle.get("openTime") == open_time:
            return candle
    return None

async def _verify_8h_aggregation_logic(client: httpx.AsyncClient) -> bool:
    """
    Проверяет, что логика агрегации OI/FR (8h = 4h-свеча-№2) соблюдена.
    """
    log.info("--- 🔬 (Логика) Начинаю проверку агрегации 4h -> 8h (OI/FR) ---")
    is_valid = True
    
    try:
        # 1. Загружаем оба кэша
        log.info("       Загружаю cache:4h и cache:8h...")
        # --- ИСПРАВЛЕНИЕ: Удаляем префикс /api/v1 ---
        resp_4h = await client.get("/get-cache/4h") 
        resp_4h.raise_for_status()
        cache_4h = resp_4h.json()
        
        # --- ИСПРАВЛЕНИЕ: Удаляем префикс /api/v1 ---
        resp_8h = await client.get("/get-cache/8h") 
        resp_8h.raise_for_status()
        cache_8h = resp_8h.json()

        # 2. Проверяем первые 10 монет из 8h кэша
        coins_to_check = cache_8h.get("data", [])[:10]
        if not coins_to_check:
            log.warning("       (Пропущено) cache:8h не содержит данных ('data' пуст).")
            return True 

        for coin_8h_data in coins_to_check:
            symbol = coin_8h_data.get("symbol")
            candles_8h = coin_8h_data.get("data", [])
            
            # Находим ту же монету в 4h
            coin_4h_data = _find_coin_data(symbol, cache_4h)
            if not coin_4h_data:
                log.error(f"       💥 [СБОЙ] Не найдена монета {symbol} в cache:4h для сравнения.")
                is_valid = False
                continue
            
            candles_4h = coin_4h_data.get("data", [])
            if not candles_4h:
                log.error(f"       💥 [СБОЙ] У монеты {symbol} в cache:4h пустой список свечей.")
                is_valid = False
                continue

            # 3. Проверяем последние 10 свечей
            for candle_8h in candles_8h[-10:]:
                open_time_8h = candle_8h.get("openTime")
                
                open_time_4h_1 = open_time_8h
                open_time_4h_2 = open_time_8h + get_interval_duration_ms('4h')

                candle_4h_1 = _find_candle_by_opentime(candles_4h, open_time_4h_1)
                candle_4h_2 = _find_candle_by_opentime(candles_4h, open_time_4h_2)

                if not candle_4h_1 or not candle_4h_2:
                    log.error(f"       💥 [СБОЙ] {symbol}: Не найдены 4h-свечи ({open_time_4h_1} или {open_time_4h_2}) для 8h-свечи {open_time_8h}.")
                    is_valid = False
                    continue
                
                # 4. СРАВНЕНИЕ ЛОГИКИ (OI)
                oi_8h = candle_8h.get("openInterest")
                oi_4h_2 = candle_4h_2.get("openInterest")
                
                if oi_8h != oi_4h_2:
                    log.error(f"       💥 [СБОЙ OI] {symbol} @ {open_time_8h}: OI 8h ({oi_8h}) != OI 4h-№2 ({oi_4h_2}).")
                    is_valid = False

                # 5. СРАВНЕНИЕ ЛОГИКИ (FR)
                fr_8h = candle_8h.get("fundingRate")
                fr_4h_1 = candle_4h_1.get("fundingRate")
                fr_4h_2 = candle_4h_2.get("fundingRate")
                
                # Логика из aggregation_8h.py: (Приоритет: candle2, Фоллбэк: candle1)
                expected_fr = fr_4h_2 if fr_4h_2 is not None else fr_4h_1
                
                if fr_8h != expected_fr:
                     log.error(f"       💥 [СБОЙ FR] {symbol} @ {open_time_8h}: FR 8h ({fr_8h}) != Ожидаемому FR ({expected_fr}) [из 4h-№2: {fr_4h_2}, 4h-№1: {fr_4h_1}].")
                     is_valid = False

        if is_valid:
             log.info("       ✅ [OK] Логика агрегации OI/FR 4h->8h подтверждена.")
             
    except Exception as e:
        log.error(f"       💥 [СБОЙ] КРИТИЧЕСКАЯ ОШИБКА во время проверки логики 8h: {e}", exc_info=True)
        is_valid = False
        
    return is_valid

# ============================================================================
# === ГЛАВНЫЙ СКРИПТ (Изменен) ===
# ============================================================================

async def run_freshness_test():
    """
    Проверяет "свежесть" (актуальность) всех Klines-кэшей.
    """
    log.info("--- 🚀 НАЧИНАЮ E2E ТЕСТ СВЕЖЕСТИ КЭША ---")
    log.info(f"Цель: {BASE_URL}")
    
    all_fresh_and_valid = True

    # Клиент создается с чистым BASE_URL
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=120.0) as client:
        
        # --- Шаг 1: Проверка, что сервер жив ---
        try:
            # --- ИСПРАВЛЕНИЕ: Удаляем префикс /api/v1 ---
            response = await client.get("/health")
            response.raise_for_status()
            log.info("✅ [OK] Сервер доступен.")
        except (httpx.ConnectError, httpx.HTTPStatusError) as e:
            log.error(f"💥 [FAIL] Не удалось подключиться к серверу: {e}")
            return

        # --- Шаг 2: Проверка всех кэшей ---
        for key in CACHE_KEYS_TO_TEST:
            log.info(f"--- 🔬 (Свежесть) Проверяю 'cache:{key}' ---")
            
            try:
                # --- ИСПРАВЛЕНИЕ: Удаляем префикс /api/v1 ---
                response = await client.get(f"/get-cache/{key}")
                
                if response.status_code == 404:
                    log.error(f"💥 [FAIL] 'cache:{key}' не найден (404).")
                    all_fresh_and_valid = False
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                # --- Проверка data_root ---
                data_root = data.get('data', [])
                if not data_root:
                    log.warning(f"       ⚠️  [ПРЕДУПРЕЖДЕНИЕ] 'cache:{key}' пуст (нет 'data'). (Аудит: {data.get('audit_report')})")
                    continue
                # ----------------------------------------------------

                # 1. Получаем 'closeTime'
                last_close_time_ms = data.get("closeTime")
                if not last_close_time_ms:
                    log.error(f"💥 [FAIL] 'cache:{key}' не содержит 'closeTime' в корне, хотя 'data' НЕ пуст.")
                    all_fresh_and_valid = False
                    continue

                # 2. Рассчитываем интервалы
                interval_ms = get_interval_duration_ms(key)
                if interval_ms == 0:
                    log.error(f"💥 [FAIL] Неизвестный интервал для ключа '{key}'.")
                    all_fresh_and_valid = False
                    continue
                
                allowed_staleness_ms = interval_ms + GRACE_PERIOD_MS
                
                current_utc_time_ms = int(time.time() * 1000)
                time_diff_ms = current_utc_time_ms - last_close_time_ms
                
                # 3. Сравниваем (Логика с 4 состояниями)
                if time_diff_ms < 0:
                     log.error(f"💥 [FAIL] 'cache:{key}' из будущего? (Разница: {time_diff_ms} мс). Проверьте системное время.")
                     all_fresh_and_valid = False
                
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
                    all_fresh_and_valid = False

                # --- Шаг 2Б: Проверка Глубины (Количества свечей) ---
                log.info(f"--- 🔬 (Глубина) Проверка количества свечей 'cache:{key}' ---")
                coins_data_list = data.get("data", [])
                if not coins_data_list:
                    log.warning("       (Пропущено) 'data' в кэше пуст.")
                    continue
                
                log.info(f"       (Проверка {min(len(coins_data_list), 10)} из {len(coins_data_list)} монет...)")
                
                # --- ИЗМЕНЕНИЕ: Определяем ожидаемое количество свечей ---
                if key == '4h':
                    expected_max_candles = 799 # Для 4h может быть до 799 свечей (например, при агрегации 8h)
                else:
                    expected_max_candles = 399 # Для других таймфреймов - до 399
                
                for coin_data in coins_data_list[:10]:
                    symbol = coin_data.get("symbol", "N/A")
                    candle_count = len(coin_data.get("data", []))
                    
                    # --- ИЗМЕНЕНИЕ: Проверяем количество свечей с учетом таймфрейма ---
                    if candle_count > expected_max_candles:
                         log.error(f"       💥 [СБОЙ] {symbol}: Найдено {candle_count} свечей (Ожидалось <= {expected_max_candles} для '{key}').")
                         all_fresh_and_valid = False
                    elif candle_count == 0:
                         log.error(f"       💥 [СБОЙ] {symbol}: Найдено 0 свечей (Ожидалось > 0).")
                         all_fresh_and_valid = False
                    else:
                        log.info(f"       ✅ [OK] {symbol}: {candle_count} свечей (<= {expected_max_candles} для '{key}').")

            except Exception as e:
                log.error(f"💥 [FAIL] Ошибка при проверке 'cache:{key}': {e}", exc_info=True)
                all_fresh_and_valid = False
        
        # --- Шаг 3: Проверка логики 8h ---
        logic_8h_valid = await _verify_8h_aggregation_logic(client)
        if not logic_8h_valid:
            all_fresh_and_valid = False

        # --- Финальный вердикт ---
        if all_fresh_and_valid:
            log.info("--- 🏆🏆🏆 ТЕСТ СВЕЖЕСТИ И ЛОГИКИ УСПЕШНО ЗАВЕРШЕН! Все кэши актуальны и корректны. ---")
        else:
            log.error("--- 💥 E2E ТЕСТ СВЕЖЕСТИ И ЛОГИКИ ПРОВАЛЕН. Найдены ошибки. ---")
            sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(run_freshness_test())
    except KeyboardInterrupt:
        log.warning("Тест свежести прерван вручную.")
