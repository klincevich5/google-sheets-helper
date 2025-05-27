# core.token_manager.py

from datetime import date, datetime, timedelta
from sqlalchemy.orm import Session
from core.config import THRESHOLD
from database.db_models import ApiUsage
from utils.logger import log_to_file


class TokenManager:
    def __init__(self, token_map):
        """
        token_map: словарь вида {'token_name': 'tokens/file.json', ...}
        """
        self.token_map = token_map

    def get_usage(self, session: Session, token_name: str) -> int:
        today = date.today()
        tomorrow = today + timedelta(days=1)

        results = (
            session.query(ApiUsage)
            .filter(ApiUsage.token == token_name)
            .filter(ApiUsage.date >= datetime.combine(today, datetime.min.time()))
            .filter(ApiUsage.date < datetime.combine(tomorrow, datetime.min.time()))
            .with_entities(ApiUsage.counter)
            .all()
        )

        # Если записей нет — вернём 0
        if not results:
            return 0

        return sum(r[0] for r in results if r[0])


    def select_best_token(self, log_file: str, session: Session) -> tuple[str, str]:
        usage_data = {
            name: self.get_usage(session, name)
            for name in self.token_map.keys()
        }

        for name, used in usage_data.items():
            log_to_file(log_file, f"🔍 {name} использован: {used}/10000")

        available = {
            name: used for name, used in usage_data.items() if used < THRESHOLD
        }

        if not available:
            raise RuntimeError("❌ Нет доступных токенов ниже лимита.")

        best = min(available.items(), key=lambda x: x[1])[0]
        return best, self.token_map[best]
