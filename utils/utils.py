import time
import os
import json

from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from datetime import datetime, timedelta
from core.config import TIMEZONE
from zoneinfo import ZoneInfo

from utils.logger import log_to_file
from utils.db_orm import insert_usage

try:
    timezone = ZoneInfo(TIMEZONE)
except Exception as e:
    raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

def get_current_shift_and_date(now: datetime = None) -> tuple[str, datetime.date]:
    """
    Определяет актуальный тип смены и корректную дату смены.

    Возвращает:
        ("day"/"night", date) — где date может быть вчера, если ночь после полуночи
    """
    if now is None:
        now = datetime.now(timezone)

    hour = now.hour

    if 9 <= hour < 21:
        return "day", now.date()
    else:
        # Ночная смена
        if hour < 9:  # 00:00–08:59 — считается ночной сменой предыдущего дня
            shift_date = (now - timedelta(days=1)).date()
        else:  # 21:00–23:59 — ночь текущего дня
            shift_date = now.date()
        return "night", shift_date

##################################################################################
# Авторизация
##################################################################################

def load_credentials(token_path, log_file, session):
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"❌ Файл токена не найден: {token_path}")

    token_name = os.path.basename(token_path).replace("_token.json", "")
    success = False

    try:
        with open(token_path, encoding="utf-8") as f:
            token = json.load(f)
        creds = Credentials(
            token=token["access_token"],
            refresh_token=token.get("refresh_token"),
            token_uri=token["token_uri"],
            client_id=token["client_id"],
            client_secret=token["client_secret"],
            scopes=token.get("scopes", ["https://www.googleapis.com/auth/spreadsheets"])
        )

        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(token_path, "w", encoding="utf-8") as token_file:
                    token_file.write(creds.to_json())
                log_to_file(log_file, f"🔄 Токен обновлён: {token_path}")
                insert_usage(session, token=token_name, count=1, scan_group="token_refresh", success=True)
            except Exception as e:
                log_to_file(log_file, f"❌ Ошибка обновления токена {token_path}: {e}")
                insert_usage(session, token=token_name, count=1, scan_group="token_refresh", success=False)
                raise

        if not creds.valid:
            raise RuntimeError(f"❌ Недействительный токен: {token_path}")

        service = build("sheets", "v4", credentials=creds)
        log_to_file(log_file, f"✅ Авторизация выполнена: {token_name}")
        success = True
        return service

    finally:
        insert_usage(session, token=token_name, count=1, scan_group="load_credentials", success=success)
        log_to_file(log_file, f"after insert_usage: {token_name}")

##################################################################################
# Проверка существования листа
##################################################################################

def check_sheet_exists(service, spreadsheet_id, sheet_name, log_file, token_name, session):
    success = False
    try:
        metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = metadata.get('sheets', [])
        for sheet in sheets:
            title = sheet.get('properties', {}).get('title')
            if title == sheet_name:
                success = True
                return True
        return False

    except Exception as e:
        log_to_file(log_file, f"❌ Ошибка при проверке листа в {spreadsheet_id}: {e}")
        return False

    finally:
        token_name = os.path.basename(token_name).replace("_token.json", "")
        insert_usage(session, token=token_name, count=1, scan_group="check_sheet", success=success)

##################################################################################
# Получение данных из Google Sheets
##################################################################################

def batch_get(service, spreadsheet_id, ranges, scan_group, log_file, token_name, session, retries=5, delay_seconds=5):
    attempt = 0
    success = False
    data = {}

    while attempt < retries:
        try:
            response = service.spreadsheets().values().batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=ranges,
                majorDimension="ROWS"
            ).execute()

            value_ranges = response.get("valueRanges", [])
            if not value_ranges:
                log_to_file(log_file, "⚠️ batchGet вернул пустые valueRanges.")
                attempt += 1
                time.sleep(delay_seconds)
                continue

            data = {vr.get("range", ""): vr.get("values", []) for vr in value_ranges}
            success = True
            break

        except HttpError as e:
            status_code = e.resp.status
            log_to_file(log_file, f"❌ HttpError {status_code} при batchGet: {e}")
            if status_code in (429, 500, 503):
                attempt += 1
                time.sleep(delay_seconds)
            elif status_code == 401:
                break
            else:
                break

        except Exception as e:
            msg = str(e).lower()
            if any(term in msg for term in ["ssl", "handshake", "decryption", "timed out"]):
                attempt += 1
                time.sleep(delay_seconds)
            else:
                log_to_file(log_file, f"❌ Ошибка batchGet: {e}")
                break

    insert_usage(
        session,
        token=os.path.basename(token_name).replace("_token.json", ""),
        count=attempt + 1,
        scan_group=scan_group,
        success=success
    )

    return data if success else {}

##################################################################################
# Обновление данных в Google Sheets
##################################################################################

def batch_update(service, spreadsheet_id, batch_data, token_name, update_group, log_file, session, retries=3, delay_seconds=10):
    success = False
    attempt = 0

    token_name = os.path.basename(token_name).replace("_token.json", "")

    # Предварительная очистка
    try:
        clear_ranges = [entry["range"] for entry in batch_data if "range" in entry]
        if clear_ranges:
            service.spreadsheets().values().batchClear(
                spreadsheetId=spreadsheet_id,
                body={"ranges": clear_ranges}
            ).execute()
    except Exception as e:
        log_to_file(log_file, f"⚠️ Ошибка очистки диапазонов: {e}")

    # Попытки записи
    while attempt < retries:
        try:
            response = service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "valueInputOption": "USER_ENTERED",
                    "data": batch_data
                }
            ).execute()

            if response and 'responses' in response:
                success = True
                break
            else:
                log_to_file(log_file, "⚠️ batchUpdate вернул пустой или некорректный ответ.")
                attempt += 1
                time.sleep(delay_seconds)

        except HttpError as e:
            status = e.resp.status
            log_to_file(log_file, f"❌ HttpError {status} при batchUpdate: {e}")
            if status in [429, 500, 503]:
                attempt += 1
                time.sleep(delay_seconds)
            elif status == 401:
                break
            else:
                break

        except Exception as e:
            log_to_file(log_file, f"❌ Ошибка batchUpdate: {e}")
            break

    insert_usage(session,
                 token=token_name,
                 count=attempt + 1,
                 update_group=update_group,
                 success=success
    )
    
    return (True, None) if success else (False, "Превышено число попыток" if attempt == retries else "Ошибка запроса")
