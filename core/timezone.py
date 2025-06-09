from datetime import datetime
from zoneinfo import ZoneInfo
from core.config import TIMEZONE

try:
    timezone = ZoneInfo(TIMEZONE)
except Exception as e:
    raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

def now():
    """Возвращает текущее время с учетом таймзоны."""
    return datetime.now(timezone)
