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
# from utils.db_orm import insert_usage

try:
    timezone = ZoneInfo(TIMEZONE)
except Exception as e:
    raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

##################################################################################
# Авторизация
##################################################################################

def load_credentials(token_path, log_file):
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"❌ Файл токена не найден: {token_path}")

    token_name = os.path.basename(token_path).replace("_token.json", "")
    success = False

    # try:
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

    if not creds.valid and creds.refresh_token:
        try:
            creds.refresh(Request())
            with open(token_path, "w", encoding="utf-8") as token_file:
                token_file.write(creds.to_json())
            log_to_file(log_file, f"🔄 Токен обновлён: {token_path}")
            
        except Exception as e:
            log_to_file(log_file, f"❌ Ошибка обновления токена {token_path}: {e}")
        raise


    if not creds.valid:
        raise RuntimeError(f"❌ Недействительный токен: {token_path}")

    service = build("sheets", "v4", credentials=creds)
    log_to_file(log_file, f"✅ Авторизация выполнена: {token_name}")
    success = True
    return service

##################################################################################
# Проверка существования листа
##################################################################################

def check_sheet_exists(service, spreadsheet_id, sheet_name, log_file, token_name):
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
 
##################################################################################
# Получение данных из Google Sheets
##################################################################################

def batch_get(service, spreadsheet_id, ranges, scan_group, log_file, token_name, retries=5, delay_seconds=5):
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

    return data if success else {}

##################################################################################
# Обновление данных в Google Sheets
##################################################################################

def batch_update(service, spreadsheet_id, batch_data, token_name, update_group, log_file, retries=3, delay_seconds=10):
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
    
    return (True, None) if success else (False, "Превышено число попыток" if attempt == retries else "Ошибка запроса")
