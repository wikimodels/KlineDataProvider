import logging
import sys

# --- НАСТРОЙКА УРОВНЕЙ ЛОГИРОВАНИЯ ---
# Чтобы включить DEBUG, измените logging.INFO на logging.DEBUG
GLOBAL_LOG_LEVEL = logging.INFO
# ------------------------------------

def setup_logging():
    """
    Настраивает корневой логгер и специальные логгеры.
    """
    
    # 1. Настройка основного логгера (который виден в консоли)
    # -----------------------------------------------------------
    # (Код этой секции не изменен)
    logger = logging.getLogger()
    
    logger.setLevel(GLOBAL_LOG_LEVEL)

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


    # --- Изменение №1: Убираем файловый логгер ---
    # 2. Настройка логгера для ОШИБОК OI/FR (ТЕПЕРЬ ОТКЛЮЧЕНА)
    # -----------------------------------------------------------
    oi_fr_logger = logging.getLogger('oi_fr_errors')
    oi_fr_logger.setLevel(logging.WARNING) # Логируем только Warning и выше
    # logger.propagate = False # (Закомментировано)
    # (Сообщения теперь будут попадать в основной логгер)

    # --- Создаем файловый обработчик (ЗАКОММЕНТИРОВАНО) ---
    # try:
    #     file_handler = logging.FileHandler('oi_fr_errors.log', mode='a', encoding='utf-8')
    #     file_handler.setLevel(logging.WARNING)
    #     file_formatter = logging.Formatter(
    #         '%(asctime)s - [%(levelname)s] - %(message)s',
    #         '%Y-%m-%d %H:%M:%S'
    #     )
    #     file_handler.setFormatter(file_formatter)
        
    #     if not oi_fr_logger.hasHandlers():
    #         oi_fr_logger.addHandler(file_handler)
    #         logger.info("Настроен логгер 'oi_fr_errors' (пишет в oi_fr_errors.log).") # (Тоже закомментировано)
            
    # except PermissionError:
    #     logger.error("Ошибка прав доступа: не удалось создать/открыть oi_fr_errors.log.")
    # except Exception as e:
    #     logger.error(f"Не удалось настроить файловый логгер 'oi_fr_errors': {e}", exc_info=True)
    # --- Конец Изменения №1 ---

# --- Запускаем настройку при импорте ---
setup_logging()

# --- Экспортируем настроенные логгеры для импорта в других модулях ---
logger = logging.getLogger() # Основной (корневой) логгер
oi_fr_error_logger = logging.getLogger('oi_fr_errors') # Логгер ошибок (теперь без файла)

