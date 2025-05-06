# logger.py

from datetime import datetime
from zoneinfo import ZoneInfo
from config import TIMEZONE
# Часовой пояс

def log_separator(log_file):
    """Добавить длинную линию для логов"""
    log_to_file(log_file, "-" * 100)

def log_section(title, log_file):
    """Добавить секцию с заголовком в логах"""
    log_to_file(log_file, "=" * 100)
    log_to_file(log_file, f"🧩 {title}")
    log_to_file(log_file, "=" * 100)

def log_to_file(path, text):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{text}\n")
        # f.write(f"{datetime.now(ZoneInfo(TIMEZONE)).isoformat()} — {text}\n")
