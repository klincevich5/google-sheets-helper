# utils/logger.py

import json
import traceback
from datetime import datetime
from core.timezone import timezone, now

def _log_structured(log_file, level, phase, task=None, status=None, message=None, error=None):
    # Добавляем эмодзи к level для наглядности
    level_emojis = {
        "INFO": "ℹ️ INFO",
        "SUCCESS": "✅ SUCCESS",
        "WARNING": "⚠️ WARNING",
        "ERROR": "❌ ERROR",
        "SECTION": "🔷 SECTION"
    }
    level_with_emoji = level_emojis.get(level, level)
    log_entry = {
        "timestamp": now().strftime('%Y-%m-%d %H:%M:%S'),
        "level": level_with_emoji,
        "message": message,
        "phase": phase,
        "task": task,
        "status": status,
        "error": error,
    }
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"Ошибка записи в лог {log_file}: {e}")

def log_info(log_file, phase, task=None, status=None, message=None):
    _log_structured(log_file, "INFO", phase, task, status, message)

def log_success(log_file, phase, task=None, status=None, message=None):
    _log_structured(log_file, "SUCCESS", phase, task, status, message)

def log_warning(log_file, phase, task=None, status=None, message=None):
    _log_structured(log_file, "WARNING", phase, task, status, message)

def log_error(log_file, phase, task=None, status=None, message=None, exc=None):
    _log_structured(log_file, "ERROR", phase, task, status, message)

def log_section(log_file, phase, message):
    # Добавляем эмодзи и визуальный разделитель
    decorated = f"✨✨ {message} ✨✨"
    _log_structured(log_file, "SECTION", phase, None, None, decorated)

def log_separator(log_file, phase):
    # Добавляем эмодзи-разделитель
    _log_structured(log_file, "SECTION", phase, None, None, " " * 100 )
    decorated = "🟣🟣🟣 " + "━" * 100 + " 🟣🟣🟣"
    _log_structured(log_file, "INFO", phase, None, None, decorated)
    _log_structured(log_file, "SECTION", phase, None, None, " " * 100 )
