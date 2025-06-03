import os
import json
import requests
from datetime import datetime, timedelta

TOKENS_DIR = os.path.dirname(__file__)
SPREADSHEET_ID = "10n0pf5qP26ALbpe0-vVlaILzmdUF8d529yN6Mt8hS9c"  # замените на нужный ID

def is_token_expired(expiry_str):
    expiry = datetime.fromisoformat(expiry_str)
    return expiry < datetime.utcnow() + timedelta(minutes=1)

def check_token(token_path):
    with open(token_path, encoding="utf-8") as f:
        token = json.load(f)
    access_token = token["access_token"]
    expiry = token.get("expiry")
    if expiry and is_token_expired(expiry):
        print(f"{os.path.basename(token_path)}: access_token истёк, пробую обновить...")
        return refresh_token(token, token_path)
    # Проверяем access_token через API
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        print(f"{os.path.basename(token_path)}: access_token действителен ✅")
        return True
    else:
        print(f"{os.path.basename(token_path)}: access_token не действителен, пробую обновить...")
        return refresh_token(token, token_path)

def refresh_token(token, token_path):
    data = {
        "client_id": token["client_id"],
        "client_secret": token["client_secret"],
        "refresh_token": token["refresh_token"],
        "grant_type": "refresh_token",
    }
    resp = requests.post(token["token_uri"], data=data)
    if resp.ok:
        tokens = resp.json()
        token["access_token"] = tokens["access_token"]
        # Google не всегда возвращает новый refresh_token
        token["expiry"] = (datetime.utcnow() + timedelta(seconds=int(tokens["expires_in"]))).isoformat()
        with open(token_path, "w", encoding="utf-8") as f:
            json.dump(token, f, ensure_ascii=False, indent=2)
        print(f"{os.path.basename(token_path)}: access_token обновлён ✅")
        return True
    else:
        print(f"{os.path.basename(token_path)}: ошибка обновления токена: {resp.text}")
        return False

def main():
    for fname in os.listdir(TOKENS_DIR):
        if fname.endswith("_token.json"):
            check_token(os.path.join(TOKENS_DIR, fname))

if __name__ == "__main__":
    main()