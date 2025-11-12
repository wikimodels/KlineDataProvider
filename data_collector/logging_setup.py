import logging
import sys

# --- НАСТРОЙКА УРОВНЕЙ ЛОГИРОВАНИЯ ---
# Устанавливаем DEBUG, чтобы увидеть, что делает task_builder.py (это устранит "засор", если вы 
# изменили уровень логгирования в вашей консоли, но я должен убедиться, что вы видите DEBUG логи).
GLOBAL_LOG_LEVEL = logging.INFO
# ------------------------------------

def setup_logging():
    """
    Настраивает корневой логгер.
    """
    
    logger = logging.getLogger()
    logger.setLevel(GLOBAL_LOG_LEVEL)

    # ... (Остальная часть функции остается без изменений) ...

    # Если обработчик не найден, добавляем новый
    found_handler = False
    for h in logger.handlers:
        if isinstance(h, logging.StreamHandler) and h.stream == sys.stderr:
            h.setLevel(GLOBAL_LOG_LEVEL)
            if h.formatter is None:
                formatter = logging.Formatter(
                    '%(asctime)s - %(levelname)s - %(message)s',
                    '%Y-%m-%d %H:%M:%S'
                )
                h.setFormatter(formatter)
            found_handler = True
            break
            
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

setup_logging()
logger = logging.getLogger()