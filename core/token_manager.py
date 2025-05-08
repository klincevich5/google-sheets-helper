# core/token_manager.py

import sqlite3
from datetime import date
from core.config import DB_PATH, THRESHOLD
from utils.logger import log_to_file

class TokenManager:
    def __init__(self, token_map):
        """
        token_map: словарь вида {'token_name': 'tokens/file.json', ...}
        """
        self.token_map = token_map

    def get_usage(self, token_name):
        today = date.today().isoformat()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT SUM(counter) FROM ApiUsage
            WHERE token = ? AND date LIKE ?
        """, (token_name, f"{today}%"))
        result = cursor.fetchone()
        conn.close()
        return result[0] or 0

    def select_best_token(self, log_file):
        # Находит токен с минимальной нагрузкой из допустимых
        usage_data = {
            name: self.get_usage(name) for name in self.token_map.keys()
        }

        for name, used in usage_data.items():
            log_to_file(log_file, f"🔍 {name} использован: {used}/10000")

        available = {
            name: used for name, used in usage_data.items() if used < THRESHOLD
        }

        if not available:
            raise RuntimeError("❌ Нет доступных токенов ниже лимита.")

        # Возвращает имя и путь к токену
        best = min(available.items(), key=lambda x: x[1])[0]
        return best, self.token_map[best]
