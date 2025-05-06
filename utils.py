# utils.py
from datetime import datetime, timedelta
import os
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from zoneinfo import ZoneInfo

from config import TOKEN_PATH, TIMEZONE

def load_credentials():
    """Загрузка токена, авто-обновление и возврат готового service."""
    creds = None

    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            print("🔄 Токен был обновлен автоматически.")
            with open(TOKEN_PATH, "w", encoding="utf-8") as token_file:
                token_file.write(creds.to_json())
        except Exception as e:
            print(f"❌ Ошибка обновления токена: {e}")

    if not creds:
        raise RuntimeError("❌ Токен не найден или некорректен. Требуется авторизация.")

    # ⚡ Самое главное: создаем сервис после загрузки токена
    service = build("sheets", "v4", credentials=creds)
    return service

def build_doc_id_map(tracked_tables):
    """Построение карты соответствия table_type -> spreadsheet_id."""
    from datetime import datetime

    today = datetime.now().date()
    # today = datetime(2025, 4, 5).date()
    print(f"📅 Сегодня: {today}")

    doc_id_map = {}

    for table in tracked_tables:
        valid_from = datetime.strptime(table["valid_from"], "%d.%m.%Y").date()
        valid_to = datetime.strptime(table["valid_to"], "%d.%m.%Y").date()
        if valid_from <= today <= valid_to:
            doc_id_map[table["table_type"]] = table["spreadsheet_id"]
    return doc_id_map

def get_active_tabs(now=None):
    if not now:
        now = datetime.now(ZoneInfo(TIMEZONE))
    hour = now.hour
    tab_list = []

    if 9 <= hour < 19:
        tab_list.append(f"DAY {now.day}")
    elif 19 <= hour < 21:
        tab_list.append(f"DAY {now.day}")
        tab_list.append(f"NIGHT {now.day}")
    elif 21 <= hour <= 23:
        tab_list.append(f"NIGHT {now.day}")
    elif 0 <= hour < 7:
        yesterday = now - timedelta(days=1)
        tab_list.append(f"NIGHT {yesterday.day}")
    elif 7 <= hour < 9:
        yesterday = now - timedelta(days=1)
        tab_list.append(f"DAY {now.day}")
        tab_list.append(f"NIGHT {yesterday.day}")
    
    # tab_list = ["DAY 1"]

    return tab_list
