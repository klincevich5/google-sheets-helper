from datetime import datetime
from zoneinfo import ZoneInfo
from core.config import TIMEZONE
from core.time_provider import TimeProvider

try:
    timezone = ZoneInfo(TIMEZONE)
except Exception as e:
    raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

def now():
    """[DEPRECATED] Используйте TimeProvider.now() для централизованного времени!"""
    return TimeProvider.now()
