# notifier.py

import requests
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from logger import log_error

def send_telegram_message(message: str, log_file: str):
    """Отправить уведомление в Telegram о сбое."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log_error(log_file, "❌ Ошибка: Не задан TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }

    try:
        response = requests.post(url, data=payload, timeout=10)
        if response.status_code != 200:
            log_error(log_file, f"❌ Ошибка отправки Telegram уведомления: {response.text}")
    except Exception as e:
        log_error(log_file, f"❌ Исключение при отправке Telegram уведомления: {e}")
