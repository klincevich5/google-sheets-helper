import json
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow

# Настройки
SCOPES = [
    "https://www.googleapis.com/auth/drive",                      # Полный доступ к Google Drive
    "https://www.googleapis.com/auth/drive.file",                # Доступ к созданным/открытым файлам
    "https://www.googleapis.com/auth/spreadsheets",              # Работа с Google Таблицами
    "https://www.googleapis.com/auth/documents",                 # Работа с Google Документами
    "https://www.googleapis.com/auth/script.projects",           # Работа с Apps Script проектами
    "https://www.googleapis.com/auth/script.deployments",        # Развёртывание скриптов (если нужно)
    "https://www.googleapis.com/auth/userinfo.email",            # Email текущего пользователя
    "https://www.googleapis.com/auth/userinfo.profile",          # Профиль текущего пользователя
    "openid"                                                      # Стандартизированный идентификатор
]


REDIRECT_PORT = 8888
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}/"

# Шаг 1: Создаём Flow вручную с фиксированным redirect_uri
flow = Flow.from_client_secrets_file(
    'client_secret.json',
    scopes=SCOPES,
    redirect_uri=REDIRECT_URI
)

auth_url, _ = flow.authorization_url(prompt='consent')
print(f"\nОткрой в браузере:\n{auth_url}\n")
webbrowser.open(auth_url)


# Шаг 2: Поднимаем сервер для приёма redirect с кодом
class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        query = urlparse(self.path).query
        params = parse_qs(query)
        if 'code' in params:
            code = params['code'][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write("<h1>Авторизация прошла успешно! Можешь закрыть это окно.</h1>".encode('utf-8'))

            # Обмениваем код на токены
            flow.fetch_token(code=code)
            creds = flow.credentials
            token_data = {
                "access_token": creds.token,
                "refresh_token": creds.refresh_token,
                "token_uri": creds.token_uri,
                "client_id": creds.client_id,
                "client_secret": creds.client_secret,
                "scopes": creds.scopes,
                "expiry": creds.expiry.isoformat()
            }
            with open("token.json", "w") as f:
                json.dump(token_data, f, indent=2)
            print("\n✅ Токены сохранены в token.json")

            # Завершаем сервер
            def shutdown():
                self.server.shutdown()
            import threading
            threading.Thread(target=shutdown).start()


server = HTTPServer(('localhost', REDIRECT_PORT), OAuthHandler)
print(f"Ожидаем код подтверждения на http://localhost:{REDIRECT_PORT}/ ...")
server.serve_forever()
