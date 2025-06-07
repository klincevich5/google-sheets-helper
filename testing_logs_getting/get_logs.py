from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import datetime
import pprint
import os
import json

def analyze_token(token_path, project_id):
    if not os.path.exists(token_path):
        print(f"❌ Token file not found: {token_path}")
        return

    creds = Credentials.from_authorized_user_file(token_path)

    print("📌 Token info:")
    print(f"- Valid: {creds.valid}")
    print(f"- Expired: {creds.expired}")
    print(f"- Refresh token: {'Yes' if creds.refresh_token else 'No'}")
    print(f"- Token expiry: {creds.expiry}")
    print(f"- Scopes: {creds.scopes}")

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        print("🔁 Token was refreshed.")

    # Проверка Drive API (необязательно)
    try:
        drive_service = build("drive", "v3", credentials=creds)
        about = drive_service.about().get(fields="user, storageQuota").execute()
        print("✅ Drive token works.")
        pprint.pprint(about)
    except Exception as api_err:
        print(f"⚠️ Drive API error (можно игнорировать): {api_err}")

    # Получение данных из Monitoring API
    try:

        sheets_service = build("sheets", "v4", credentials=creds)
        sheet_data = sheets_service.spreadsheets().values().get(
            spreadsheetId="YOUR_SPREADSHEET_ID",
            range="A1"
        ).execute()
        print("✅ Sheets API call made.")




        monitoring_service = build("monitoring", "v3", credentials=creds)

        now = datetime.datetime.utcnow()
        start_time = (now - datetime.timedelta(hours=1)).isoformat("T") + "Z"
        end_time = now.isoformat("T") + "Z"

        request = monitoring_service.projects().timeSeries().list(
            name=f"projects/{project_id}",
            filter='metric.type = "serviceruntime.googleapis.com/api/request_count" '
                   'AND resource.label."service" = "sheets.googleapis.com"',
            view="FULL",
            interval_startTime=start_time,
            interval_endTime=end_time
        )

        response = request.execute()

        print("📊 Monitoring data (Google Sheets API, last 1 hour):")
        if "timeSeries" in response:
            for ts in response["timeSeries"]:
                pprint.pprint(ts)
        else:
            print("⚠️ Нет данных за указанный период.")

        with open("sheets_api_usage.json", "w", encoding="utf-8") as f:
            json.dump(response, f, indent=2, ensure_ascii=False)
        print("📁 Сохранено в sheets_api_usage.json")

    except Exception as e:
        print(f"❌ Monitoring API error: {e}")

if __name__ == "__main__":
    analyze_token("RotationsInfo_scanner_2_token.json", "storied-chariot-446307-v6")
