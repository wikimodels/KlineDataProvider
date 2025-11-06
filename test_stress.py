import httpx
import asyncio
import sys
import logging
import time

# --- Настройки ---
BASE_URL = "http://127.0.0.1:8000"
POLL_INTERVAL_SEC = 15 
MAX_WAIT_MINUTES = 45  
TIMEFRAMES_TO_TEST = ["1h", "4h", "12h", "1d"]
# -----------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("STRESS_TEST")

async def wait_for_worker_to_be_free(client: httpx.AsyncClient, task_name: str):
    """
    Опрашивает сервер, пока воркер не освободится (перестанет возвращать 409).
    """
    log.info(f"--- Ожидаю завершения задачи '{task_name}' (опрос каждые {POLL_INTERVAL_SEC} сек)...")
    start_time = time.time()
    max_wait_time_sec = MAX_WAIT_MINUTES * 60
    
    # 1. Сначала ждем, пока воркер заберет задачу (если она еще в очереди)
    while True:
        try:
            # (Здесь 'await' был)
            response = await client.get("/queue-status")
            queue_len = response.json()["tasks_in_queue"]
            if queue_len == 0:
                log.info(f"... Воркер забрал задачу '{task_name}' (очередь пуста).")
                break
            log.info(f"... Задача '{task_name}' еще в очереди (длина: {queue_len}). Жду 5 сек...")
            await asyncio.sleep(5)
        except Exception as e:
            log.error(f"Ошибка при проверке очереди: {e}")
            await asyncio.sleep(5)
            
        if time.time() - start_time > max_wait_time_sec:
            raise TimeoutError(f"Таймаут! Воркер не забрал задачу '{task_name}' за {MAX_WAIT_MINUTES} мин.")

    # 2. Теперь ждем, пока воркер освободит блокировку
    while True:
        if time.time() - start_time > max_wait_time_sec:
            raise TimeoutError(f"Таймаут! Задача '{task_name}' не завершилась за {MAX_WAIT_MINUTES} мин.")

        try:
            response = await client.post("/get-market-data", json={"timeframe": "1h"})
            
            if response.status_code == 202:
                # Воркер свободен. Мы зря запустили '1h', надо ее убрать.
                
                # --- ИСПРАВЛЕНИЕ: Добавлен 'await' ---
                queue_check_response = await client.get("/queue-status")
                q_len = queue_check_response.json()["tasks_in_queue"]
                # -----------------------------------
                
                if q_len > 0:
                    log.info(f"... (Воркер свободен, но в очереди осталась тестовая задача '1h' (длина: {q_len}). Игнорирую.)")
                    # (Мы не можем ее безопасно удалить, не используя 'lpop',
                    # поэтому просто продолжаем - воркер заберет ее следующей)
                
                log.info(f"✅ Воркер освободился (получен 202). Задача '{task_name}' выполнена.")
                return
            
            elif response.status_code == 409:
                log.info(f"... Воркер занят (409). Жду {POLL_INTERVAL_SEC} сек...")
                await asyncio.sleep(POLL_INTERVAL_SEC)
            else:
                log.error(f"Неожиданный статус при опросе воркера: {response.status_code} {response.text}")
                await asyncio.sleep(POLL_INTERVAL_SEC)
                
        except Exception as e:
            log.error(f"Ошибка при опросе воркера: {e}", exc_info=False) # Убрал exc_info, чтобы не было трейсбэка
            await asyncio.sleep(POLL_INTERVAL_SEC)

async def run_stress_test():
    """
    Запускает все задачи ПОСЛЕДОВАТЕЛЬНО, дожидаясь выполнения каждой.
    """
    log.info(f"--- 🚀 НАЧИНАЮ СТРЕСС-ТЕСТ (250 монет) ---")
    log.info(f"Цель: {BASE_URL}")
    log.info(f"Задачи: {TIMEFRAMES_TO_TEST}")
    
    total_start_time = time.time()
    
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        
        # Убедимся, что воркер свободен перед началом
        try:
            # (Этот вызов 'init' очистит очередь, если там осталась задача '1h' из прошлого прогона)
            await wait_for_worker_to_be_free(client, "init")
        except TimeoutError as e:
            log.error(f"💥 [FAIL] {e}")
            log.error("Воркер был занят еще до начала теста. Прерываю.")
            return
        
        log.info("--- (Воркер свободен. Начинаем) ---")

        for timeframe in TIMEFRAMES_TO_TEST:
            task_start_time = time.time()
            log.info(f"--- 🏁 Запускаю задачу: {timeframe} ---")
            
            try:
                response = await client.post("/get-market-data", json={"timeframe": timeframe})
                
                if response.status_code == 202:
                    log.info(f"✅ Задача '{timeframe}' принята (202).")
                elif response.status_code == 409:
                     log.error(f"💥 [FAIL] Не удалось запустить задачу '{timeframe}': Воркер все еще занят (409).")
                     log.error("Это не должно было случиться. Прерываю тест.")
                     return
                else:
                    response.raise_for_status() # Вызовет ошибку для 500, 400 и т.д.

                # Ждем завершения
                await wait_for_worker_to_be_free(client, timeframe)
                
                task_end_time = time.time()
                log.info(f"--- ✅ Задача '{timeframe}' УСПЕШНО ЗАВЕРШЕНА за {(task_end_time - task_start_time):.2f} сек. ---")

            except TimeoutError as e:
                log.error(f"💥 [FAIL] {e}")
                log.error("Тест прерван из-за таймаута.")
                return
            except httpx.HTTPStatusError as e:
                log.error(f"💥 [FAIL] Не удалось запустить задачу '{timeframe}': {e}")
                return
            except Exception as e:
                log.error(f"💥 [FAIL] Непредвиденная ошибка: {e}")
                return

    total_end_time = time.time()
    log.info(f"--- 🏆 СТРЕСС-ТЕСТ (4 задачи) УСПЕШНО ЗАВЕРШЕН! ---")
    log.info(f"--- Общее время: {(total_end_time - total_start_time):.2f} сек. ---")


if __name__ == "__main__":
    try:
        asyncio.run(run_stress_test())
    except KeyboardInterrupt:
        log.warning("Стресс-тест прерван вручную.")