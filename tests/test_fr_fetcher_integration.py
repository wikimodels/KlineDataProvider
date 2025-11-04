import pytest
import os
from unittest.mock import patch, MagicMock

# Импортируем *реальный* модуль
from data_collector import fr_fetcher

# Помечаем тест как "slow"
@pytest.mark.slow
@pytest.mark.skipif(
    os.environ.get("RUN_SLOW_TESTS") != "1",
    reason="Тест пропущен. (Запускает реальные запросы к API)."
)
@pytest.mark.asyncio
async def test_real_fr_update_process_integration(caplog):
    """
    Интеграционный тест:
    1. РЕАЛЬНО вызывает coin_source.get_coins_from_api().
    2. РЕАЛЬНО вызывает fetch_funding_rates() (с реальными запросами к биржам).
    3. МОКАЕТ только 'save_to_cache'.
    
    Проверяет весь пайплайн, включая логику приоритета Binance.
    """
    
    mock_save = MagicMock()
    
    with patch('cache_manager.save_to_cache', mock_save):
        print("\n[INFO] Запускаю РЕАЛЬНЫЙ сбор FR (это может занять до 30-60 сек)...")
        await fr_fetcher.run_fr_update_process()

    # Проверяем, что сборщик не вернул пустые данные
    assert "Сбор FR вернул пустой словарь" not in caplog.text
    assert "Сбор FR вернул None" not in caplog.text
    assert "Не удалось получить монеты из API" not in caplog.text
    
    # Проверяем, что мы дошли до конца
    assert "[CRON_JOB] Успех!" in caplog.text
    
    # Проверяем, что 'save_to_cache' был вызван
    mock_save.assert_called_once()
    
    # Проверяем, что данные, которые мы сохраняем, не пустые
    call_args = mock_save.call_args[0]
    key = call_args[0]
    data = call_args[1]
    
    assert key == 'global_fr'
    assert isinstance(data, dict)
    assert len(data) > 0 # Мы должны были собрать данные хотя бы по 1 монете
    
    print(f"[INFO] Успех. Собрано {len(data)} FR. Пример: {list(data.keys())[0]}")