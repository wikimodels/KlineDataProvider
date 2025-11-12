"""
Этот модуль отвечает за форматирование и отправку
уведомлений о сработавших алертах в Telegram.

(АСИНХРОННАЯ ВЕРСИЯ) - для соответствия cache_manager.py и config.py
"""
import logging
import httpx # --- ИЗМЕНЕНИЕ: Используем AsyncClient ---
from typing import List, Optional
# --- ИЗМЕНЕНИЕ: Убран 'Redis' (больше не нужен) ---
from datetime import datetime
import pytz

# --- ИЗМЕНЕНИЕ: Импортируем конфиг напрямую ---
try:
    import config
except ImportError:
    logging.critical("Не удалось импортировать config.py")
    config = None

from alert_manager.model import Alert, VwapAlert

logger = logging.getLogger(__name__)

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"

# --- Хелперы форматирования (Портировано из Deno) ---

def _get_tradingview_link(symbol: str, exchanges: List[str]) -> str:
    """
    Портировано из get-tv-link.ts
    """
    if not exchanges:
        return f"https://www.tradingview.com/chart/?symbol={symbol}"

    priority = ["BYBIT", "BINANCE", "OKX", "BITGET", "GATEIO", "KUCOIN", "COINBASE", "MEXC"]
    
    best_exchange = "BINANCE" 
    for ex in priority:
        if ex in exchanges:
            best_exchange = ex
            break
            
    tv_symbol = f"{best_exchange}:{symbol}"
    
    return f"https://www.tradingview.com/chart/?symbol={tv_symbol}"

def _format_report_time() -> str:
    """
    Портировано из triggered-alerts-msg.ts
    """
    dt_utc = datetime.now(pytz.utc)
    dt_target_tz = dt_utc.astimezone(pytz.FixedOffset(180)) # UTC+3
    time_str = dt_target_tz.strftime('%Y-%m-%d %H:%M:%S')
    
    # (Ссылка на mobile_link удалена, т.к. ее нет в config.py)
    return f'{time_str} 🈸🈸🈸'

def _format_vwap_report_time() -> str:
    """
    Портировано из triggered-vwap-alerts-msg.ts
    """
    dt_utc = datetime.now(pytz.utc)
    dt_target_tz = dt_utc.astimezone(pytz.FixedOffset(180)) # UTC+3
    time_str = dt_target_tz.strftime('%Y-%m-%d %H:%M:%S')
    
    return f'{time_str} 🈯️🈯️🈯️'


# --- Основные функции ---

# --- ИЗМЕНЕНИЕ: Возвращаем 'async', УБИРАЕМ 'redis_conn' ---
async def _send_tg_message(
    msg: str, 
    parse_mode: str = "HTML"
):
    """
    Отправляет сообщение, используя токен и ID пользователя из config.py
    (АСИНХРОННАЯ ВЕРСИЯ)
    """
    try:
        if not config:
            logger.error("Модуль config не загружен. Отправка TG невозможна.")
            return

        # --- ИЗМЕНЕНИЕ: Используем config.py ---
        bot_token = config.TG_BOT_TOKEN_KEY
        chat_id = config.TG_USER_KEY
        
        if not bot_token:
            logger.error("Не найден 'TG_BOT_TOKEN_KEY' в config.py. Отправка TG невозможна.")
            return
        if not chat_id:
            logger.error("Не найден 'TG_USER_KEY' в config.py. Отправка TG невозможна.")
            return

        url = TELEGRAM_API_URL.format(token=bot_token)
        payload = {
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }
        
        # --- ИЗМЕНЕНИЕ: Используем httpx.AsyncClient ---
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload, timeout=10.0)
            
            if response.status_code != 200:
                logger.error(f"Ошибка отправки в TG: {response.status_code} - {response.text}")
            else:
                logger.info("Уведомление о сработавших алертах успешно отправлено в TG.")

    except Exception as e:
        logger.error(f"Критическая ошибка при отправке в TG: {e}", exc_info=True)


# --- ИЗМЕНЕНИЕ: Возвращаем 'async', УБИРАЕМ 'redis_conn' ---
async def send_triggered_alerts_report(
    alerts: List[Alert]
):
    """
    Форматирует и отправляет отчет о сработавших Line Alerts.
    (АСИНХРОННАЯ ВЕРСИЯ)
    """
    if not alerts:
        msg = "<b>✴️ LINE ALERTS (1h): NO TRIGGERED ALERTS</b>"
    else:
        alert_items = []
        for i, alert in enumerate(alerts):
            tv_link = _get_tradingview_link(alert.get("symbol", "N/A"), alert.get("exchanges", []))
            alert_name = alert.get("alertName", "N/A")
            
            safe_name = httpx.utils.escape_html(alert_name) 
            
            item = f'<a href="{tv_link}"><b>{i + 1}. <i>{safe_name}</i></b></a>'
            alert_items.append(item)
        
        alert_list_str = "\n".join(alert_items)
        report_time_str = _format_report_time()
        
        msg = f"""
<b>✴️ LINE ALERTS (1h)</b>
{alert_list_str}
{report_time_str}
""".strip()

    # --- ИЗМЕНЕНИЕ: Возвращаем 'await' ---
    await _send_tg_message(msg)


# --- ИЗМЕНЕНИЕ: Возвращаем 'async', УБИРАЕМ 'redis_conn' ---
async def send_triggered_vwap_alerts_report(
    alerts: List[VwapAlert]
):
    """
    Форматирует и отправляет отчет о сработавших VWAP Alerts.
    (АСИНХРОННАЯ ВЕРСИЯ)
    """
    if not alerts:
        msg = "<b>💹 VWAP ALERTS (1h): NO TRIGGERED ALERTS</b>"
    else:
        alert_items = []
        for i, alert in enumerate(alerts):
            symbol = alert.get("symbol", "N/A")
            tv_link = _get_tradingview_link(symbol, alert.get("exchanges", []))
            anchor_time_str = alert.get("anchorTimeStr", "N/A")
            
            symbol_short = symbol.replace("USDT", "").replace("PERP", "") 
            
            item = f'<a href="{tv_link}"><b>{i + 1}. {symbol_short}/<i>{anchor_time_str}</i></b></a>'
            alert_items.append(item)
            
        alert_list_str = "\n".join(alert_items)
        report_time_str = _format_vwap_report_time()
        
        msg = f"""
<b>💹 VWAP ALERTS (1h)</b>
{alert_list_str}
{report_time_str}
""".strip()

    # --- ИЗМЕНЕНИЕ: Возвращаем 'await' ---
    await _send_tg_message(msg)