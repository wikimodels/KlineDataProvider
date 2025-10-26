Анализ архитектуры проекта по сбору данных

Этот документ описывает высокоуровневую архитектуру и поток данных в вашем проекте, основываясь на предоставленных файлах.

1. Ключевые Модули

main.py (Не предоставлен, но упоминается): Точка входа в приложение, запускает FastAPI и фоновый воркер.

api_routes.py: Определяет эндпоинты FastAPI (/get-market-data, /cache/{timeframe}, /api/v1/internal/update-fr). Отвечает за прием запросов, проверку блокировок и постановку задач в очередь.

worker.py: Сердце приложения. Запускается в фоновом режиме (background_worker), слушает очередь задач (task_queue). Отвечает за получение блокировки (WORKER_LOCK_KEY), вызов сборщиков данных и специальную обработку 4h/8h.

data_collector (пакет): Главный пакет, отвечающий за логику сбора.

__init__.py: Оркестратор (fetch_market_data), который объединяет подготовку задач, их выполнение и последующую обработку.

fr_fetcher.py: Независимый сборщик (run_fr_update_process), отвечающий только за сбор Funding Rate в cache:global_fr.

task_builder.py: Создает списки задач (URL, парсеры) для Klines, OI и FR. Критически важно: он пропускает задачи FR, если видит prefetched_fr_data.

fetch_strategies.py: Содержит логику HTTP-запросов (fetch_simple, fetch_bybit_paginated), включая управление параллелизмом (CONCURRENCY_LIMIT).

data_processing.py: Отвечает за постобработку: merge_data (слияние Klines, OI, FR) и _aggregate_4h_to_8h (генерация 8h свечей из 4h).

url_builder.py: Централизованно формирует URL-адреса для API Binance и Bybit.

api_parser.py: Централизованно парсит JSON-ответы от бирж и приводит их к единому внутреннему формату (Klines, OI, FR).

database.py: Управляет подключением к PostgreSQL (get_db_connection) и предоставляет функции для получения списков монет и их метаданных (get_coins_from_db, get_all_unique_coins).

cache_manager.py: Управляет подключением к Upstash Redis. Предоставляет функции save_to_cache (с сжатием zlib и кодированием base64) и get_from_cache (с обратным декодированием).

logging_setup.py: Настраивает логирование для всего приложения.

2. Общая концепция

Проект представляет собой асинхронный сервис (API на FastAPI), предназначенный для сбора рыночных данных (Klines, Open Interest, Funding Rate) с бирж (Binance, Bybit).

Ключевая особенность архитектуры — разделение процессов сбора данных и активное кэширование:

API (api_routes.py): Принимает HTTP-запросы.

POST /get-market-data: Не выполняет сбор данных немедленно, а ставит задачу в очередь.

GET /cache/{timeframe}: Отдает уже готовые, собранные данные из кэша.

Воркер (worker.py): Фоновый процесс, который выполняет задачи из очереди (сбор Klines/OI).

Сборщик FR (fr_fetcher.py): Отдельный, независимый процесс (запускаемый по Cron), который предварительно загружает данные Funding Rate (FR) в отдельный кэш.

2.1. Хранение и Кэширование

Данные хранятся в двух местах:

PostgreSQL (database.py): "Холодное" хранилище. Используется только для получения списка монет и их метаданных (Hurst, entropy и т.д.).

Redis (cache_manager.py): "Горячее" хранилище (кэш). Здесь хранятся фактические рыночные данные (Klines, OI, FR) для быстрого доступа через API.

Реализация: Используется upstash_redis.

Оптимизация: Данные перед сохранением сериализуются в JSON, сжимаются (zlib) и кодируются в base64. get_from_cache выполняет обратную операцию.

3. Ключевая архитектурная особенность: Разделение FR и Klines/OI

Самая важная часть вашей архитектуры — это то, как обрабатывается Funding Rate (FR).

Проблема: Данные FR (фандинг) обычно требуются для всех таймфреймов (1h, 4h, 8h и т.д.), но собирать их каждый раз вместе с Klines для каждого таймфрейма — избыточно и медленно.

Ваше решение:

Независимый сборщик FR (fr_fetcher.py):

Этот модуль запускается по расписанию (через Cron-Job, который обращается к защищенному POST /api/v1/internal/update-fr) и, возможно, при старте сервера.

Он запрашивает все монеты из БД (get_all_unique_coins из database.py).

Он собирает данные FR для всех этих монет (prepare_fr_tasks, fetch_funding_rates).

Он сохраняет эти данные в специальный, глобальный кэш в Redis: cache:global_fr (используя save_to_cache из cache_manager.py).

Интеграция в Воркере (worker.py):

Когда worker.py берет задачу (например, "собрать 4h"), его первый шаг — загрузить cache:global_fr из Redis (_load_global_fr_cache).

Затем он вызывает data_collector.fetch_market_data(), передавая ему эти данные фандинга (prefetched_fr_data).

task_builder.py (внутри fetch_market_data) видит, что данные FR уже предоставлены, и пропускает создание задач на их сбор.

Собираются только Klines и OI.

Наконец, data_processing.py объединяет свежесобранные Klines/OI с заранее загруженными данными FR (merge_data).

Результат: Сбор FR происходит один раз для всех, а сбор Klines/OI — по мере необходимости для конкретного таймфрейма. Это значительно ускоряет выполнение задач воркера.

4. Поток данных: От запроса до кэша

Рассмотрим полный жизненный цикл на примере запроса данных 4h.

Шаг 1: Запуск задачи (API)

Клиент отправляет POST /get-market-data с телом {"timeframe": "4h"}.

api_routes.py принимает запрос. Таймфрейм "4h" проверяется по списку POST_TIMEFRAMES.

Он проверяет, не занят ли воркер, проверив ключ WORKER_LOCK_KEY в Redis.

Если свободен, он добавляет строку "4h" в очередь task_queue (worker.py).

API немедленно отвечает клиенту 202 Accepted (Задача принята).

Шаг 2: Выполнение задачи (Воркер)

worker.py (в background_worker) видит задачу "4h" в task_queue.

Он немедленно устанавливает блокировку WORKER_LOCK_KEY в Redis.

Вызывается роутер _process_task("4h").

Роутер видит, что timeframe == '4h', и вызывает специальную функцию _process_4h_and_8h_task.

Внутри _process_4h_and_8h_task:

Загружается cache:global_fr (_load_global_fr_cache).

Запрашиваются метаданные для монет 4h и 8h из database.py.

Вызывается data_collector.fetch_market_data(..., '4h', prefetched_fr_data).

Шаг 3: Сбор Klines/OI (Data Collector)

fetch_market_data (в data_collector/__init__.py) оркестрирует процесс:

task_builder.py: Создает задачи только для Klines и OI (пропуская FR).

fetch_strategies.py: Асинхронно выполняет все HTTP-запросы.

api_parser.py: Парсит ответы от Binance/Bybit в единый формат.

data_processing.py (merge_data): Объединяет Klines(4h) + OI(4h) + FR(из cache:global_fr).

Сборщик возвращает один большой набор данных 4h (master_market_data).

Шаг 4: Генерация кэшей 4h и 8h (Воркер)

worker.py получает master_market_data (4h).

Процесс 4h: Данные обогащаются метаданными 4h и сохраняются в cache:4h (save_to_cache).

Процесс 8h:

Вызывается generate_and_save_8h_cache (из data_processing.py).

Эта функция использует _aggregate_4h_to_8h для создания свечей 8h из Klines и OI 4h.

Она повторно использует данные FR (т.к. они одинаковы).

Новые данные 8h обогащаются метаданными 8h.

Результат сохраняется в cache:8h.

Шаг 5: Завершение

worker.py завершает _process_4h_and_8h_task.

Блокировка WORKER_LOCK_KEY снимается в блоке finally.

Воркер готов к следующей задаче.

Шаг 6: Чтение (API)

Клиент (в любое время) вызывает GET /cache/4h или GET /cache/8h.

api_routes.py вызывает get_from_cache('4h').

cache_manager.py извлекает, декодирует (base64) и распаковывает (zlib) данные из Redis.

API отдает клиенту готовый JSON.