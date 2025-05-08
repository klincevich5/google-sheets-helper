# utils/utils.py

import os
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from utils.logger import log_to_file


def load_credentials(token_path, log_file):
    """
    Загружает токен авторизации из указанного пути,
    при необходимости обновляет, и возвращает Google Sheets service.
    """
    creds = None
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"❌ Файл токена не найден: {token_path}")

    creds = Credentials.from_authorized_user_file(token_path)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            log_to_file(log_file, f"🔄 Токен в {token_path} был обновлен.")

            with open(token_path, "w", encoding="utf-8") as token_file:
                token_file.write(creds.to_json())
        except Exception as e:
            log_to_file(log_file, f"❌ Ошибка при обновлении токена {token_path}: {e}")
            raise

    if not creds or not creds.valid:
        raise RuntimeError(f"❌ Невалидный токен: {token_path}")

    service = build("sheets", "v4", credentials=creds)
    return service
