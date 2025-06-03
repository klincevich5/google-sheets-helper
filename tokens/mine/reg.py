import json
import webbrowser
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from google_auth_oauthlib.flow import Flow
from datetime import datetime

# ===== Настройки =====
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
REDIRECT_PORT = 8888
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}/"

# Список (файл_секрета, файл_токена)
CLIENTS = [
    ("client_secret_SheetsInfo_scanner_1.json", "SheetsInfo_scanner_1_token.json"),
    ("client_secret_RotationsInfo_scanner_1.json", "RotationsInfo_scanner_1_token.json"),
    ("client_secret_RotationsInfo_scanner_2.json", "RotationsInfo_scanner_2_token.json"),
]

# ===== Фабрика обработчиков =====
def make_handler(flow, token_filename):
    class CustomOAuthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            query = urlparse(self.path).query
            params = parse_qs(query)

            if 'code' in params:
                code = params['code'][0]

                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                html = "<h1>✅ Авторизация прошла успешно. Можешь закрыть это окно.</h1>"
                self.wfile.write(html.encode("utf-8"))

                # Получаем токены
                flow.fetch_token(code=code)
                creds = flow.credentials
                token_data = {
                    "access_token": creds.token,
                    "refresh_token": creds.refresh_token,
                    "token_uri": creds.token_uri,
                    "client_id": creds.client_id,
                    "client_secret": creds.client_secret,
                    "scopes": creds.scopes,
                    "expiry": creds.expiry.isoformat() if creds.expiry else None
                }

                with open(token_filename, "w", encoding="utf-8") as f:
                    json.dump(token_data, f, indent=2, ensure_ascii=False)
                print(f"✅ Токен сохранён в {token_filename}\n")

                # Завершаем сервер
                def shutdown():
                    self.server.shutdown()
                threading.Thread(target=shutdown).start()

    return CustomOAuthHandler

# ===== Основной цикл =====
for client_secret_file, token_filename in CLIENTS:
    print(f"🔐 Начинаем авторизацию для: {token_filename}")

    flow = Flow.from_client_secrets_file(
        client_secret_file,
        scopes=SCOPES,
        redirect_uri=REDIRECT_URI
    )

    auth_url, _ = flow.authorization_url(prompt='consent')
    webbrowser.open(auth_url)
    print(f"🔗 Перейди по ссылке в браузере:\n{auth_url}\n")

    handler_class = make_handler(flow, token_filename)
    server = HTTPServer(('localhost', REDIRECT_PORT), handler_class)
    print(f"⌛ Ожидаем подтверждение на http://localhost:{REDIRECT_PORT}/ ...")
    server.serve_forever()

    time.sleep(1)

print("🎉 Все токены успешно получены!")
