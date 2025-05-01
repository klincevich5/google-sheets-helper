# google_service.py

from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from config import TOKEN_PATH
from logger import log_info, log_error
from notifier import send_telegram_message

class GoogleSheetsService:
    def __init__(self, log_file: str):
        self.log_file = log_file  # Сохраняем путь к логу при создании
        self.service = self.load_service()

    def load_service(self):
        """Загрузить или обновить Google Sheets API сервис."""
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH)
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                log_info(self.log_file, "🔄 Токен Google обновлён автоматически.")
            service = build("sheets", "v4", credentials=creds)
            log_info(self.log_file, "✅ Сервис Google Sheets успешно загружен.")
            return service
        except Exception as e:
            log_error(self.log_file, f"❌ Ошибка загрузки сервиса Google Sheets: {e}")
            send_telegram_message(self.log_file, f"❌ Ошибка загрузки Google Sheets API: {e}")
            raise

    def refresh_if_needed(self):
        """Проверка состояния токена во время работы."""
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH)
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                log_info(self.log_file, "🔄 Токен Google успешно обновлён во время работы.")
        except Exception as e:
            log_error(self.log_file, f"❌ Ошибка обновления токена во время работы: {e}")
            send_telegram_message(self.log_file, f"❌ Ошибка обновления токена: {e}")
