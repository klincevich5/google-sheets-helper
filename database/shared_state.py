# core/shared_state.py

import threading
from database.session import SessionLocal
from core.data import return_tracked_tables

class SharedDocIDMap:
    def __init__(self, session_factory=SessionLocal):
        self._lock = threading.Lock()
        self._map = {}
        self._session_factory = session_factory

    def refresh(self):
        session = self._session_factory()
        try:
            new_map = return_tracked_tables(session)
            with self._lock:
                self._map = new_map
        finally:
            session.close()

    def get(self):
        with self._lock:
            return self._map.copy()
