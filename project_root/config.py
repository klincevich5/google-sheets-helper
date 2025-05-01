# config.py

# Пути для логов
SHEETS_LOG_FILE = "logs/sheets_scanner.log"     # лог для scanner_sheets
ROTATIONS_LOG_FILE = "logs/rotations_scanner.log" # лог для scanner_rotations

# Пути для токенов
TOKEN_PATH = "token.json"  # Путь к сохранённому токену OAuth

# Путь к базе данных SQLite
db_path = "scheduler.db"  # Путь к базе данных SQLite

# Настройки Telegram уведомлений
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"    # Сюда вставишь свой токен
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"        # ID своего чата/канала

# Параметры паузы между циклами сканирования
SHEETS_SLEEP_SECONDS = 60        # Пауза между циклами для scanner_sheets
ROTATIONS_SLEEP_SECONDS = 60     # Пауза между циклами для scanner_rotations

# Кеширование активных вкладок (если понадобится динамическое определение)
CACHE_TABS_SECONDS = 600          # Сколько секунд кэшировать активные вкладки

# Порядок блоков для главного экрана
ROTATION_ORDER = [
    "SHUFFLE Main",
    "VIP Main",
    "TURKISH Main",
    "GENERIC Main",
    "GSBJ Main",
    "LEGENDZ Main",
    "TRI-STAR Main",
    "TritonRL Main",
]