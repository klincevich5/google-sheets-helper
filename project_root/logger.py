# logger.py

from datetime import datetime

def log_to_file(path: str, level: str, text: str):
    """Базовая функция записи лога с уровнем."""
    with open(path, "a", encoding="utf-8") as f:
        timestamp = datetime.now().isoformat()
        f.write(f"{timestamp} [{level}] {text}\n")

def log_info(path: str, text: str):
    log_to_file(path, "INFO", text)

def log_warning(path: str, text: str):
    log_to_file(path, "WARNING", text)

def log_error(path: str, text: str):
    log_to_file(path, "ERROR", text)
