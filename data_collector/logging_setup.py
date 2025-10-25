import logging

# 1. Настраиваем основной логгер (для общих сообщений)
logger = logging.getLogger("DataCollector")
if not logger.hasHandlers():
    logger.setLevel(logging.DEBUG)
    main_handler = logging.StreamHandler() # Вывод в консоль
    main_handler.setLevel(logging.INFO)
    main_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    main_handler.setFormatter(main_formatter)
    logger.addHandler(main_handler)

# 2. Настраиваем логгер ошибок OI/FR (для записи в файл)
oi_fr_error_logger = logging.getLogger('oi_fr_errors')
if not oi_fr_error_logger.hasHandlers():
    oi_fr_error_logger.setLevel(logging.DEBUG) # Логируем всё
     
    
    # Предотвращаем дублирование в основном логгере
    oi_fr_error_logger.propagate = False
