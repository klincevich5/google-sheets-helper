from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from core.config import TIMEZONE

def get_current_shift_and_date(now: datetime = None):
    """
    Возвращает (shift_type, date) с учётом ночной смены после полуночи.
    """
    if now is None:
        now = datetime.now(ZoneInfo(TIMEZONE))
    hour = now.hour
    if 9 <= hour < 21:
        return "day", now.date()
    else:
        # Ночная смена: если после полуночи до 9 утра — это ночь предыдущего дня
        if hour < 9:
            return "night", (now - timedelta(days=1)).date()
        else:
            return "night", now.date()

def day_or_night(now: datetime = None) -> str:
    shift_type, _ = get_current_shift_and_date(now)
    return shift_type