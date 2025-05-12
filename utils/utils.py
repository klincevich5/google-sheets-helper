# utils/utils.py

import time
import json
from googleapiclient.errors import HttpError

import os
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from utils.logger import log_to_file
from database.database import insert_usage  # Добавляем импорт для логирования

from googleapiclient.http import HttpRequest

##################################################################################
# Логирование квоты токена
##################################################################################

class LoggingHttpRequest(HttpRequest):
    def execute(self, **kwargs):
        response = super().execute(**kwargs)

        # Сохраняем заголовки отдельно, чтобы их можно было использовать
        self._response_headers = getattr(self, "resp", {}).headers if hasattr(self, "resp") else {}

        return response

    def get_response_headers(self):
        return getattr(self, "_response_headers", {})

#################################################################################
# Подключение к Google Sheets API
#################################################################################

def load_credentials(token_path, log_file):
    """
    Загружает и при необходимости обновляет токен авторизации из файла.Ы
    Возвращает Google Sheets API клиент с логированием квоты токена.
    """
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"❌ Файл токена не найден: {token_path}")

    token_name = os.path.basename(token_path).replace("_token.json", "")
    success = False

    try:
        creds = Credentials.from_authorized_user_file(token_path)

        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(token_path, "w", encoding="utf-8") as token_file:
                    token_file.write(creds.to_json())

                log_to_file(log_file, f"🔄 Токен в {token_path} был обновлен.")

                insert_usage(
                    token=token_name,
                    count=1,
                    scan_group="token_refresh",
                    success=True
                )
            except Exception as e:
                log_to_file(log_file, f"❌ Ошибка при обновлении токена {token_path}: {e}")
                insert_usage(
                    token=token_name,
                    count=1,
                    scan_group="token_refresh",
                    success=False
                )
                raise

        if not creds or not creds.valid:
            raise RuntimeError(f"❌ Невалидный токен: {token_path}")

        service = build("sheets", "v4", credentials=creds, requestBuilder=LoggingHttpRequest)
        log_to_file(log_file, f"✅ Авторизация выполнена с токеном '{token_name}'")
        success = True
        return service

    finally:
        # Логируем вызов load_credentials независимо от результата
        
        token_name = os.path.basename(token_name).replace("_token.json", "")
        insert_usage(
            token=token_name,
            count=1,
            scan_group="load_credentials",
            success=success
        )

##################################################################################
# Проверка существования листа в таблице
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
        # Логирование попытки независимо от результата
        token_name = os.path.basename(token_name).replace("_token.json", "")
        insert_usage(
            token=token_name,
            count=1,
            scan_group="check_sheet",
            success=success
        )
###################################################################################
# Получение данных из Google Sheets
###################################################################################

def batch_get(service, spreadsheet_id, ranges, scan_group, log_file, token_name, retries=5, delay_seconds=5):
    attempt = 0
    success = False
    data = {}

    while attempt < retries:
        try:
            # log_to_file(
            #     log_file,
            #     f"📡 Попытка {attempt + 1}/{retries} — batchGet для {len(ranges)} диапазонов в документе {spreadsheet_id}"
            # )

            request = service.spreadsheets().values().batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=ranges,
                majorDimension="ROWS"
            )

            response = request.execute()

            value_ranges = response.get("valueRanges", [])
            data = {vr.get("range", ""): vr.get("values", []) for vr in value_ranges}

            # log_to_file(log_file, f"✅ Успешный batchGet. Получено {len(data)} диапазонов.")

            # Попробуем получить заголовки квоты из запроса, если используется LoggingHttpRequest
            if hasattr(request, "get_response_headers"):
                headers = request.get_response_headers()
                if headers and isinstance(headers, dict):
                    quota_info = {
                        "X-RateLimit-Remaining": headers.get("X-RateLimit-Remaining"),
                        "X-Goog-Quota-Used": headers.get("X-Goog-Quota-Used"),
                        "X-Goog-Quota-Limit": headers.get("X-Goog-Quota-Limit")
                    }
                    # log_to_file(log_file, f"📊 Квота токена {token_name}: {quota_info}")
            else:
                log_to_file(log_file, "❌ Не удалось получить заголовки квоты из запроса.")
            success = True
            break

        except HttpError as e:
            status_code = e.resp.status
            log_to_file(log_file, f"❌ HttpError {status_code} при batchGet: {e}")

            if status_code in (429, 500, 503):
                attempt += 1
                log_to_file(log_file, f"⏳ Повтор через {delay_seconds} секунд...")
                time.sleep(delay_seconds)
            elif status_code == 401:
                log_to_file(log_file, "🔒 Ошибка авторизации (401). Прерываю batchGet.")
                break
            else:
                break

        except Exception as e:
            msg = str(e).lower()
            if any(term in msg for term in ["ssl", "handshake", "decryption", "timed out"]):
                attempt += 1
                log_to_file(log_file, f"⏳ Сетевая ошибка '{e}', повтор через {delay_seconds} секунд...")
                time.sleep(delay_seconds)
            else:
                log_to_file(log_file, f"❌ Непредвиденная ошибка batchGet: {e}")
                break

    token_name = os.path.basename(token_name).replace("_token.json", "")

    insert_usage(
        token=token_name,
        count=attempt + 1,
        scan_group=scan_group,
        success=success
    )

    if not success:
        log_to_file(log_file, "❌ batchGet завершён неудачно.")

    return data if success else {}

###################################################################################
# Обновление данных в Google Sheets
###################################################################################

def batch_update(service, spreadsheet_id, batch_data, token_name, update_group, log_file, retries=3, delay_seconds=10):
    success = False
    attempt = 0

    # Нормализуем имя токена
    token_name = os.path.basename(token_name).replace("_token.json", "")

    # Сначала очищаем все диапазоны
    try:
        clear_ranges = [entry["range"] for entry in batch_data if "range" in entry]
        if clear_ranges:
            service.spreadsheets().values().batchClear(
                spreadsheetId=spreadsheet_id,
                body={"ranges": clear_ranges}
            ).execute()
            log_to_file(log_file, f"🧹 Очищены диапазоны перед вставкой: {len(clear_ranges)}")
    except Exception as e:
        log_to_file(log_file, f"⚠️ Ошибка при очистке диапазонов: {e}")

    while attempt < retries:
        try:
            request = service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "valueInputOption": "USER_ENTERED",
                    "data": batch_data
                }
            )

            response = request.execute()

            if hasattr(request, "get_response_headers"):
                headers = request.get_response_headers()
                if headers and isinstance(headers, dict):
                    quota_info = {
                        "X-RateLimit-Remaining": headers.get("X-RateLimit-Remaining"),
                        "X-Goog-Quota-Used": headers.get("X-Goog-Quota-Used"),
                        "X-Goog-Quota-Limit": headers.get("X-Goog-Quota-Limit")
                    }
            else:
                log_to_file(log_file, "❌ Не удалось получить заголовки квоты из запроса.")

            success = True
            break

        except HttpError as e:
            status = e.resp.status
            log_to_file(log_file, f"❌ HttpError {status} при batchUpdate: {e}")

            if status in [429, 500, 503]:
                attempt += 1
                log_to_file(log_file, f"⏳ Повтор через {delay_seconds} сек...")
                time.sleep(delay_seconds)
            elif status == 401:
                log_to_file(log_file, "🔒 Ошибка авторизации (401). Прерываю batchUpdate.")
                break
            else:
                break

        except Exception as e:
            log_to_file(log_file, f"❌ Ошибка при batchUpdate: {e}")
            break

    insert_usage(
        token=token_name,
        count=attempt + 1,
        update_group=update_group,
        success=success
    )

    return (True, None) if success else (False, "Превышено число попыток" if attempt == retries else "Ошибка запроса")

##################################################################################
# Обработка данных из БД
##################################################################################

def update_task_process_fields(conn, task, log_file, table_name):
    cursor = conn.cursor()
    table = table_name
    cursor.execute(f"""
        UPDATE {table}
        SET
            hash = ?,
            values_json = ?
        WHERE id = ?
    """, (
        task.hash,
        json.dumps(task.values_json) if task.values_json else None,
        task.id
    ))
    # log_to_file(log_file, f"💾 Обновлён values_json и hash для задачи {task.name_of_process}")
    conn.commit()

##################################################################################
# Обновление полей задачи в БД
##################################################################################

def update_task_scan_fields(conn, task, log_file, table_name):
    cursor = conn.cursor()
    cursor.execute(f"""
        UPDATE {table_name}
        SET
            last_scan = ?,
            scan_quantity = ?,
            scan_failures = ?
        WHERE id = ?
    """, (
        task.last_scan.isoformat() if task.last_scan else None,
        task.scan_quantity,
        task.scan_failures,
        task.id
    ))

    #log_to_file(log_file, f"💾 Сохраняю в БД [Task {task.name_of_process}] → proceed={task.proceed} → changed={task.changed}, hash={task.hash}")
    conn.commit()

###################################################################################
# Обновление полей задачи в БД
###################################################################################

def update_task_update_fields(conn, task, log_file, table_name):
    cursor = conn.cursor()
    cursor.execute(f"""
        UPDATE {table_name}
        SET
            last_update = ?,
            update_quantity = ?,
            update_failures = ?
        WHERE id = ?
    """, (
        task.last_update.isoformat() if task.last_update else None,
        task.update_quantity,
        task.update_failures,
        task.id
    ))
    conn.commit()
    # log_to_file(log_file, f"💾 Обновление полей update для задачи [{task.name_of_process}] завершено.")
