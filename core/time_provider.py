# core/time_provider.py
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from core.config import TIMEZONE

class _ThreadLocalTimeProvider:
    def __init__(self):
        self._local = threading.local()

    def set_time(self, dt):
        """Установить фиксированное время (dt — datetime с tzinfo или str)."""
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=self.timezone())
        self._local.now = dt

    def now(self):
        return getattr(self._local, 'now', datetime.now(self.timezone()))

    def reset(self):
        if hasattr(self._local, 'now'):
            del self._local.now

    def timezone(self):
        return ZoneInfo(TIMEZONE)

TimeProvider = _ThreadLocalTimeProvider()
