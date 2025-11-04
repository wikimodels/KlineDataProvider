import logging
import sys
import io
from importlib import reload
from contextlib import redirect_stderr

import pytest


@pytest.fixture(autouse=True)
def reset_root_logger():
    """Сбрасывает корневой логгер перед каждым тестом."""
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.NOTSET)
    yield


def test_default_log_level_is_info():
    """Тест: по умолчанию логгер настроен на уровень INFO."""
    # Удаляем модуль из кэша, чтобы импортировать заново
    if 'data_collector.logging_setup' in sys.modules:
        del sys.modules['data_collector.logging_setup']

    import data_collector.logging_setup as ls
    reload(ls)

    assert ls.logger.level == logging.INFO


def test_log_message_format():
    """Тест: сообщения логгера имеют правильный формат (дата, уровень, текст)."""
    if 'data_collector.logging_setup' in sys.modules:
        del sys.modules['data_collector.logging_setup']

    import data_collector.logging_setup as ls
    reload(ls)

    # Проверим формат через один из обработчиков
    handler = next(
        (h for h in ls.logger.handlers
         if isinstance(h, logging.StreamHandler) and h.stream == sys.stderr),
        None
    )
    assert handler is not None, "StreamHandler для stderr не найден"
    assert handler.formatter is not None

    record = logging.LogRecord(
        name="test", level=logging.WARNING, pathname="", lineno=0,
        msg="Test warning", args=(), exc_info=None
    )
    formatted = handler.formatter.format(record)
    assert " - WARNING - Test warning" in formatted
    assert formatted.startswith("20")  # Год, например: 2025-...


def test_no_duplicate_handlers_on_reimport():
    """Тест: повторный импорт не создаёт дублирующихся обработчиков."""
    if 'data_collector.logging_setup' in sys.modules:
        del sys.modules['data_collector.logging_setup']

    import data_collector.logging_setup as ls
    reload(ls)
    initial_count = len(ls.logger.handlers)

    # Импортируем ещё раз
    reload(ls)
    final_count = len(ls.logger.handlers)

    assert final_count == initial_count


def test_logs_output_to_stderr():
    """Тест: сообщения действительно пишутся в stderr."""
    if 'data_collector.logging_setup' in sys.modules:
        del sys.modules['data_collector.logging_setup']

    # Перехватываем stderr
    stderr_capture = io.StringIO()
    with redirect_stderr(stderr_capture):
        import data_collector.logging_setup as ls
        reload(ls)

        test_msg = "LOG_TO_STDERR_TEST"
        ls.logger.info(test_msg)

    output = stderr_capture.getvalue()
    assert test_msg in output
    assert " - INFO - " in output
    assert output.startswith("20")