import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла, чтобы os.environ их "увидел"
load_dotenv()

# ============================================================================
# === Конфигурация Redis (Upstash) ===
# ============================================================================
UPSTASH_REDIS_URL = os.environ.get("UPSTASH_REDIS_URL")
UPSTASH_REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_TOKEN")

# ============================================================================
# === Ключи Очереди и Блокировки Воркера ===
# ============================================================================
REDIS_TASK_QUEUE_KEY = "data_collector_task_queue"
WORKER_LOCK_KEY = "data_collector_lock"

# (НОВАЯ КОНСТАНТА для проверки блокировки)
WORKER_LOCK_VALUE = "processing" # <-- ИСПРАВЛЕНИЕ

WORKER_LOCK_TIMEOUT_SECONDS = 1800 # 30 минут

# ============================================================================
# === Конфигурация API (Этого Сервера) ===
# ============================================================================
SECRET_TOKEN = os.environ.get("SECRET_TOKEN")

POST_TIMEFRAMES = ['1h', '4h', '12h', '1d']
ALLOWED_CACHE_KEYS = ['1h', '4h', '8h', '12h', '1d', 'global_fr']

# ============================================================================
# === Конфигурация Источника Монет (Coin Sifter API) ===
# ============================================================================
COIN_SIFTER_BASE_URL = os.environ.get("COIN_SIFTER_URL")
COIN_SIFTER_API_TOKEN = os.environ.get("SECRET_TOKEN")
COIN_SIFTER_ENDPOINT_PATH = "/coins/formatted-symbols"

COIN_PROCESSING_LIMIT = 250
# ============================================================================


# ============================================================================
# === Устаревшее (Legacy) ===
# ============================================================================
DATABASE_URL = os.environ.get("DATABASE_URL")
# ============================================================================
TG_BOT_TOKEN_KEY = os.environ.get("TG_BOT_TOKEN")
TG_USER_KEY = os.environ.get("TG_USER")