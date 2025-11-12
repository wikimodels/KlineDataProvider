import logging
import sys

# --- НАСТРОЙКА УРОВНЕЙ ЛОГИРОВАНИЯ ---
# Чтобы включить DEBUG, измените logging.INFO на logging.DEBUG
GLOBAL_LOG_LEVEL = logging.INFO
# ------------------------------------

def setup_logging():
    """
    Настраивает корневой логгер.
    """
    
    # 1. Настройка основного логгера (который виден в консоли)
    logger = logging.getLogger()
    logger.setLevel(GLOBAL_LOG_LEVEL)

    # Проверяем, есть ли уже настроенный обработчик (чтобы избежать дублей)
    found_handler = False
    for h in logger.handlers:
        if isinstance(h, logging.StreamHandler) and h.stream == sys.stderr:
            h.setLevel(GLOBAL_LOG_LEVEL)
            
            # Устанавливаем форматтер, если его нет
            if h.formatter is None:
                formatter = logging.Formatter(
                    '%(asctime)s - %(levelname)s - %(message)s',
                    '%Y-%m-%d %H:%M:%S'
                )
                h.setFormatter(formatter)
            
            found_handler = True
            break
            
    # Если обработчик не найден, добавляем новый
    if not found_handler:
         formatter = logging.Formatter(
             '%(asctime)s - %(levelname)s - %(message)s',
             '%Y-%m-%d %H:%M:%S'
         )
         main_handler = logging.StreamHandler(sys.stderr)
         main_handler.setLevel(GLOBAL_LOG_LEVEL)
         main_handler.setFormatter(formatter)
         logger.addHandler(main_handler)

    logger.info("--- Логгирование настроено. Уровень: %s ---", logging.getLevelName(logger.level))

    # --- Секция 'oi_fr_errors' ПОЛНОСТЬЮ УДАЛЕНА ---

# --- Запускаем настройку при импорте ---
setup_logging()

# --- Экспортируем настроенные логгеры для импорта в других модулях ---
logger = logging.getLogger() # Основной (корневой) логгер
# --- oi_fr_error_logger УДАЛЕН ---