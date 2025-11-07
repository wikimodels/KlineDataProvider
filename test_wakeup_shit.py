import httpx
import asyncio
import sys
import logging
import time
import os 
from dotenv import load_dotenv

# --- 1. Загрузка конфигурации ---
load_dotenv()  
KLINE_PROVIDER_URL = os.environ.get("KLINE_PROVIDER_URL")
COIN_SIFTER_URL = os.environ.get("COIN_SIFTER_URL")
SECRET_TOKEN = os.environ.get("SECRET_TOKEN") 

# --- 2. Настройки ---
POLL_INTERVAL_SEC = 15      # Пауза между попытками "разбудить"
MAX_WAIT_MINUTES = 30       # Общий тайм-аут (как вы и просили)

# --- 3. Настройка "Стильного" логгера ---
# ANSI-коды для цветов
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"
CYAN = "\033[96m"

# Создаем логгер
log = logging.getLogger("WAKE_UP_SCRIPT")
log.setLevel(logging.INFO)

# Создаем обработчик для консоли
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)

# Создаем "стильный" форматтер
class ColoredFormatter(logging.Formatter):
    def format(self, record):
        timestamp = f"{CYAN}[{time.strftime('%Y-%m-%d %H:%M:%S')}] - {record.levelname} - {RESET}"
        message = super().format(record)
        return f"{timestamp} {message}"

# (Убираем стандартный формат времени у Formatter, т.к. добавили свой)
formatter = ColoredFormatter('%(message)s')
handler.setFormatter(formatter)

# (Предотвращаем дублирование, если скрипт импортируют)
if not log.hasHandlers():
    log.addHandler(handler)

# ============================================================================
# === Вспомогательная функция "Пробуждения" ===
# ============================================================================

async def wake_up_server(client: httpx.AsyncClient, name: str, endpoint: str, headers: dict = None):
    """
    Пытается "разбудить" один сервер, повторяя запросы до успеха
    или тайм-аута в 30 минут.
    """
    log.info(f"🚀 {YELLOW}Начинаю пробуждение сервера: {name}...{RESET} (Эндпоинт: {endpoint})")
    
    start_time = time.time()
    max_wait_sec = MAX_WAIT_MINUTES * 60

    while True:
        # 1. Проверка тайм-аута
        if time.time() - start_time > max_wait_sec:
            log.error(f"💥 {RED}ПРОВАЛ!{RESET} Сервер '{name}' не ответил за {MAX_WAIT_MINUTES} минут.")
            raise TimeoutError(f"Server {name} wake-up timeout")

        try:
            # 2. Попытка запроса
            response = await client.get(endpoint, headers=headers, timeout=20.0)
            
            # 3. Анализ ответа
            if response.status_code == 200:
                log.info(f"✅ {GREEN}СЕРВЕР '{name}' ПРОСНУЛСЯ!{RESET} (Получен статус 200)")
                return True # Успех

            # 503 (Service Unavailable) - стандартный ответ Render, пока сервис "крутится"
            elif response.status_code == 503:
                log.warning(f"   ... {YELLOW}Сервер '{name}' еще просыпается (503 Service Unavailable)...{RESET} Жду {POLL_INTERVAL_SEC} сек.")
            
            # 401/403 - ошибка токена (это постоянная ошибка, нет смысла ждать)
            elif response.status_code in [401, 403]:
                log.error(f"💥 {RED}ПРОВАЛ!{RESET} Сервер '{name}' ответил {response.status_code} (Ошибка Авторизации).")
                log.error(f"   ... {RED}Проверьте SECRET_TOKEN для {name}.{RESET}")
                return False # Провал

            # 404 - эндпоинт не найден
            elif response.status_code == 404:
                log.error(f"💥 {RED}ПРОВАЛ!{RESET} Сервер '{name}' ответил 404. Эндпоинт {endpoint} не найден.")
                return False # Провал

            else:
                log.warning(f"   ... {YELLOW}Сервер '{name}' ответил {response.status_code}.{RESET} (Не 200/503). Жду {POLL_INTERVAL_SEC} сек...")

        # (Сервер еще не поднялся на уровне сети)
        except httpx.ConnectError:
            log.warning(f"   ... {YELLOW}Сервер '{name}' еще не доступен (ConnectError)...{RESET} Жду {POLL_INTERVAL_SEC} сек.")
        
        # (Другие ошибки)
        except Exception as e:
            log.error(f"💥 {RED}Неизвестная ошибка при пробуждении '{name}': {e}{RESET}")
            return False # Провал

        # 4. Пауза перед следующей попыткой
        await asyncio.sleep(POLL_INTERVAL_SEC)

# ============================================================================
# === ГЛАВНЫЙ СКРИПТ ===
# ============================================================================

async def run_wake_up_script():
    """
    Запускает параллельное пробуждение всех серверов.
    """
    log.info(f"--- 🚀 {GREEN}ЗАПУСК СКРИПТА 'WAKE-UP'{RESET} (Лимит: {MAX_WAIT_MINUTES} мин) ---")
    
    # 1. Проверка конфигурации
    if not all([KLINE_PROVIDER_URL, COIN_SIFTER_URL, SECRET_TOKEN]):
        log.error(f"💥 {RED}ПРОВАЛ!{RESET} Не все переменные окружения установлены в .env:")
        if not KLINE_PROVIDER_URL: log.error("   - KLINE_PROVIDER_URL отсутствует")
        if not COIN_SIFTER_URL: log.error("   - COIN_SIFTER_URL отсутствует")
        if not SECRET_TOKEN: log.error("   - SECRET_TOKEN отсутствует")
        return False

    # 2. Формируем заголовки
    
    # --- (ИЗМЕНЕНИЕ) ИСПРАВЛЕН ЗАГОЛОВОК ---
    # (Используем 'X-Auth-Token', как в coin_source.py)
    sifter_headers = {"X-Auth-Token": SECRET_TOKEN}
    # -------------------------------------
    
    # 3. Создаем клиентов и список задач
    # (Мы создаем отдельных клиентов, т.к. у них разные base_url)
    tasks_to_run = []
    
    try:
        # Задача 1: CoinSifter
        client_sifter = httpx.AsyncClient(base_url=COIN_SIFTER_URL)
        tasks_to_run.append(
            wake_up_server(client_sifter, "CoinSifter", "/blacklist", sifter_headers)
        )
        
        # Задача 2: KlineProvider
        client_klines = httpx.AsyncClient(base_url=KLINE_PROVIDER_URL)
        tasks_to_run.append(
            wake_up_server(client_klines, "KlineProvider", "/cache/global_fr", headers=None)
        )

        # 4. Запускаем задачи параллельно
        results = await asyncio.gather(*tasks_to_run, return_exceptions=True)
    
    finally:
        # 5. (Важно) Закрываем сессии клиентов
        if 'client_sifter' in locals():
            await client_sifter.aclose()
        if 'client_klines' in locals():
            await client_klines.aclose()

    # 6. Финальный вердикт
    # (Проверяем, что все вернули True и не было исключений)
    final_success = all(res is True for res in results)

    if final_success:
        log.info(f"--- 🏆 {GREEN}УСПЕХ! Оба сервера успешно разбужены.{RESET} ---")
        return True
    else:
        log.error(f"--- 🚫 {RED}ПРОВАЛ! Не удалось разбудить один или несколько серверов.{RESET} ---")
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(run_wake_up_script())
        if not success:
            sys.exit(1) # Выходим с кодом ошибки, если тест провален
            
    except KeyboardInterrupt:
        log.warning(f"\n... {YELLOW}Пробуждение прервано вручную.{RESET}")