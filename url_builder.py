# url_builder.py
"""
Этот модуль отвечает за ФОРМИРОВАНИЕ URL-адресов
для запросов к API бирж (Binance, Bybit).
"""

from typing import Optional

# --- Базовые URL ---
BINANCE_BASE_URL = "https://fapi.binance.com"
BYBIT_BASE_URL = "https://api.bybit.com"

# --- BINANCE URL Builders ---

def get_binance_klines_url(symbol_api: str, interval: str, limit: int = 400) -> str:
    """
    Формирует URL для получения Klines (свечей) с Binance Futures.
    """
    return f"{BINANCE_BASE_URL}/fapi/v1/klines?symbol={symbol_api}&interval={interval}&limit={limit}"

def get_binance_open_interest_url(symbol_api: str, period: str, limit: int = 400) -> str:
    """
    Формирует URL для получения Open Interest (OI) с Binance Futures.
    """
    # Убедимся, что лимит не превышает 500 (макс. для Binance OI/FR)
    limit = min(limit, 500)
    return f"{BINANCE_BASE_URL}/futures/data/openInterestHist?symbol={symbol_api}&period={period}&limit={limit}"

def get_binance_funding_rate_url(symbol_api: str, limit: int = 400) -> str:
    """
    Формирует URL для получения Funding Rate (FR) с Binance Futures.
    """
    # Убедимся, что лимит не превышает 500
    limit = min(limit, 500)
    return f"{BINANCE_BASE_URL}/fapi/v1/fundingRate?symbol={symbol_api}&limit={limit}"

# --- BYBIT URL Builders ---

def get_bybit_klines_url(symbol_api: str, interval: str, limit: int = 400) -> str:
    """
    Формирует URL для получения Klines (свечей) с Bybit V5 (Linear).
    Bybit использует минуты (1h=60, 4h=240, 1D=D).
    """
    interval_map = {
        '1h': '60',
        '4h': '240',
        '8h': '480', # Bybit поддерживает 8h (480)
        '12h': '720',
        '1d': 'D'
    }
    bybit_interval = interval_map.get(interval, '60') # По умолчанию 1h

    # Bybit V5 Klines: limit (макс 200) устанавливается в fetch_strategies,
    # так как мы используем пагинацию.
    # Мы запрашиваем 800 свечей 4h (для 8h),
    # fetch_bybit_paginated сделает 4 запроса по 200.
    return f"{BYBIT_BASE_URL}/v5/market/klines?category=linear&symbol={symbol_api}&interval={bybit_interval}"

def get_bybit_open_interest_url(symbol_api: str, period: str, limit: int = 400) -> str:
    """
    Формирует URL для получения Open Interest (OI) с Bybit V5 (Linear).
    Bybit использует intervalType (1h, 4h, ...).
    """
    # Bybit V5 OI: limit (макс 200).
    # --- ИСПРАВЛЕНИЕ 1: Добавляем параметр limit ---
    limit = min(limit, 200)
    return f"{BYBIT_BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol_api}&intervalTime={period}&limit={limit}"

def get_bybit_funding_rate_url(symbol_api: str, limit: int = 400) -> str:
    """
    Формирует URL для получения Funding Rate (FR) с Bybit V5 (Linear).
    """
    # Bybit V5 FR: limit (макс 100).
    # --- ИСПРАВЛЕНИЕ 2: Добавляем параметр limit ---
    limit = min(limit, 100)
    return f"{BYBIT_BASE_URL}/v5/market/funding/history?category=linear&symbol={symbol_api}&limit={limit}"