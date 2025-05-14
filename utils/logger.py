# utils/logger.py

from datetime import datetime
from zoneinfo import ZoneInfo
from core.config import TIMEZONE
# Часовой пояс

def log_separator(log_file):
    """Добавить длинную линию для логов"""
    log_to_file(log_file, "-" * 30)

def log_section(title, log_file):
    """Добавить секцию с заголовком в логах"""
    try:
        log_to_file(log_file, "=" * 30)
        log_to_file(log_file, f"🧩 {title}")
        log_to_file(log_file, "=" * 30)
    except Exception as e:
        print(f"⚠️ Ошибка при log_section: {e}")
        raise

def log_to_file(log_file, message):
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            try:
                timezone = ZoneInfo(TIMEZONE)
            except Exception as e:
                raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

            f.write(f"{datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S')} — {message}\n")
    except Exception as e:
        print(f"Ошибка записи в лог {log_file}: {e}")
