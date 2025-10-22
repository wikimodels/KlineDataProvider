# -*- coding: utf-8 -*-

import requests
import time
import os
from dotenv import load_dotenv  # <-- НОВЫЙ ИМПОРТ для работы с .env

# --- НАСТРОЙКИ ---

# Загружаем переменные из .env файла
load_dotenv()

# 1. АДРЕС СМАРТ-КОНТРАКТА, ЗА КОТОРЫМ СЛЕДИМ
CONTRACT_ADDRESS = "EQDYeCUArx2MQsxTKKI5RWR2nj16xlnuZGwYNVmO0lddfuzH"
CONTRACT_URL ="https://tonviewer.com/EQDYeCUArx2MQsxTKKI5RWR2nj16xlnuZGwYNVmO0lddfuzH"
# 2. ПОЛУЧАЕМ КЛЮЧИ ИЗ .ENV ФАЙЛА
API_KEY = os.getenv("TONCENTER_API_KEY")
TG_USER_ID = os.getenv("TELEGRAM_USER_ID")
TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# --- КОНЕЦ НАСТРОЕК ---


# URL для API сети TON
TONCENTER_API_URL = "https://toncenter.com/api/v2/getTransactions"
# URL для API Telegram Bot
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

# Переменная, чтобы хранить ID последней увиденной транзакции
last_seen_lt = 0

def send_telegram_message(message_text):
    """
    Отправляет сообщение в Telegram указанному пользователю.
    """
    print(f"Отправляю уведомление в Telegram: {message_text}")
    payload = {
        'chat_id': TG_USER_ID,
        'text': message_text,
        'parse_mode': 'Markdown'
    }
    try:
        response = requests.post(TELEGRAM_API_URL, data=payload)
        if response.status_code != 200:
            print(f"❗️ Не удалось отправить сообщение в Telegram. Код ошибки: {response.status_code}")
            print(f"Ответ сервера: {response.text}")
    except Exception as e:
        print(f"❗️ Произошла ошибка при отправке в Telegram: {e}")

def check_for_updates():
    """
    Основная функция, которая проверяет наличие новых транзакций.
    """
    global last_seen_lt

    print(f"[{time.strftime('%H:%M:%S')}] Проверяю наличие новых транзакций...")

    headers = {'X-API-Key': API_KEY}
    params = {'address': CONTRACT_ADDRESS, 'limit': 1}

    try:
        response = requests.get(TONCENTER_API_URL, params=params, headers=headers)
        if response.status_code == 200:
            transactions = response.json().get('result', [])
            if not transactions:
                return

            latest_transaction = transactions[0]
            current_lt = int(latest_transaction['transaction_id']['lt'])

            if current_lt > last_seen_lt:
                print(f"🔥 Обнаружена новая транзакция!")
                if last_seen_lt == 0:
                    last_seen_lt = current_lt
                    print("Бот запущен. Жду первых изменений...")
                    return

                last_seen_lt = current_lt
                op_name = latest_transaction.get('in_msg', {}).get('decoded_op_name')

                if op_name == 'change_dns_record':
                    message = "✅ *МОСТ ОТКРЫТ!* ✅\n\nПора действовать!"
                    print("\n" + "="*40 + f"\n{message}\n" + "="*40 + "\n")
                    send_telegram_message(message)
                    os.system('echo -n "\\a"')
                
                elif op_name == 'delete_dns_record':
                    message = "❌ *Мост закрыт.*"
                    print("\n" + "-"*40 + f"\n{message}\n" + "-"*40 + "\n")
                    send_telegram_message(message)
                
                else:
                    print(f"  ℹ️ Новая транзакция другого типа: {op_name or 'Неизвестно'}")

        elif response.status_code == 401:
            print("❗️ Ошибка 401: Неверный API ключ!")
            print("❗️ Сервер toncenter.com не принял ваш ключ. Проверь ключ в .env файле.")
            time.sleep(60)
        else:
            print(f"❗️ Ошибка ответа от сервера Toncenter: {response.status_code} {response.text}")

    except Exception as e:
        print(f"❗️ Произошла ошибка при запросе: {e}")

if __name__ == "__main__":
    if not all([API_KEY, TG_USER_ID, TG_BOT_TOKEN]):
        print("="*50 + "\n‼️ ВНИМАНИЕ! ‼️\nНе удалось загрузить ключи из .env файла.")
        print("Пожалуйста, убедись, что файл .env существует и в нем заполнены все переменные.\n" + "="*50)
    else:
        print("🚀 Запуск бота-наблюдателя с уведомлениями в Telegram...")
        print("Ключи и настройки успешно загружены из .env файла.")
        print(f"Слежу за контрактом: {CONTRACT_ADDRESS}")
        print(f"Уведомления будут приходить пользователю: {TG_USER_ID}")
        print("Нажмите Ctrl+C для остановки.")
        
        send_telegram_message("🤖 *Бот-наблюдатель запущен и следит за шлюзом!*")

        while True:
            check_for_updates()
            time.sleep(10)

