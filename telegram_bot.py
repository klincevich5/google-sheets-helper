
import ast
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
from config import BOT_TOKEN, AUTHORIZED_USERS
from utils_bot import get_active_tabs, get_rotations_tasks_by_tab, get_task_by_id

from db_access import get_rotations_stats


# Разбор ID
try:
    parsed = ast.literal_eval(AUTHORIZED_USERS)
    AUTHORIZED_USERS = [int(uid) for uid in parsed] if isinstance(parsed, list) else [int(parsed)]
except Exception:
    AUTHORIZED_USERS = [int(uid.strip()) for uid in AUTHORIZED_USERS.split(",") if uid.strip().isdigit()]

def is_authorized(user_id):
    return user_id in AUTHORIZED_USERS

# === /start ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE, from_query=False):
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        text = "🚫 Доступ запрещён"
        if from_query and update.callback_query:
            await update.callback_query.edit_message_text(text)
        elif update.message:
            await update.message.reply_text(text)
        return

    stats = get_rotations_stats()
    stats_text = (
        f"👋 Привет! Вот текущая статистика:\n\n"
        f"📊 RotationsInfo: {stats['total']} задач, {stats['errors']} ошибок\n"
        f"📊 SheetsInfo: [заглушка]\n"
        f"📦 TrackedTables: [заглушка]"
    )

    keyboard = [
        [InlineKeyboardButton("RotationsInfo", callback_data="rotations")],
        [InlineKeyboardButton("SheetsInfo", callback_data="sheets")],
        [InlineKeyboardButton("TrackedTables", callback_data="tracked")]
    ]

    if from_query and update.callback_query:
        await update.callback_query.edit_message_text(stats_text, reply_markup=InlineKeyboardMarkup(keyboard))
    elif update.message:
        await update.message.reply_text(stats_text, reply_markup=InlineKeyboardMarkup(keyboard))

# === CALLBACK ROUTER ===

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if not is_authorized(user_id):
        await query.edit_message_text("🚫 Доступ запрещён")
        return

    data = query.data

    if data == "home":
        return await start(update, context, from_query=True)
    elif data == "rotations":
        return await handle_rotations(query)
    elif data.startswith("rotations_tab:"):
        return await handle_rotations_tab(query, data)
    elif data.startswith("task:"):
        return await handle_task_selection(query, data)
    elif data.startswith("action:"):
        return await handle_task_action(query, data)

    await query.edit_message_text("🔧 Раздел в разработке.")


# === ВЫБОР СМЕНЫ ===
async def handle_rotations(query):
    tabs = get_active_tabs()
    buttons = [[InlineKeyboardButton(tab, callback_data=f"rotations_tab:{tab}")] for tab in tabs]
    buttons.append([InlineKeyboardButton("🏠 Домой", callback_data="home")])
    await query.edit_message_text("🕒 Выберите смену:", reply_markup=InlineKeyboardMarkup(buttons))


# === СПИСОК ЗАДАЧ ПО СМЕНЕ ===
async def handle_rotations_tab(query, data):
    tab = data.split(":")[1]
    tasks = get_rotations_tasks_by_tab(tab)

    if not tasks:
        await query.edit_message_text(f"📭 В смене {tab} задач не найдено.")
        return

    rows, row = [], []
    for task in tasks:
        row.append(InlineKeyboardButton(task['name'], callback_data=f"task:rotations:{task['id']}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([
        InlineKeyboardButton("🔙 Назад", callback_data="rotations"),
        InlineKeyboardButton("🏠 Домой", callback_data="home")
    ])

    await query.edit_message_text(f"📋 Задачи в смене: {tab}", reply_markup=InlineKeyboardMarkup(rows))


# === КАРТОЧКА ЗАДАЧИ ===
async def handle_task_selection(query, data):
    _, group, task_id = data.split(":")
    task = get_task_by_id(task_id)

    if not task:
        await query.edit_message_text("⚠️ Задача не найдена.")
        return

    details = (
        f"📄 Процесс: {task['name']}\n"
        f"📍 Источник: {task['source']}\n"
        f"🧮 Хеш: {task['hash']}\n"
        f"🔁 Последний скан: {task['last_scan']}\n"
        f"📤 Последняя вставка: {task['last_update']}\n"
        f"⚠️ Ошибки сканирования: {task['scan_failures']}"
    )

    buttons = [
        [InlineKeyboardButton("🔁 Сканировать", callback_data=f"action:scan:{task_id}:{group}"),
        InlineKeyboardButton("♻️ Обработать", callback_data=f"action:process:{task_id}:{group}")],
        [InlineKeyboardButton("📤 Обновить", callback_data=f"action:update:{task_id}:{group}")],
        [InlineKeyboardButton("🧹 Сброс", callback_data=f"action:clear:{task_id}:{group}"),
        InlineKeyboardButton("🪵 Логи", callback_data=f"action:logs:{task_id}:{group}")],
        [InlineKeyboardButton("🔙 Назад", callback_data="rotations"),
        InlineKeyboardButton("🏠 Домой", callback_data="home")]
    ]

    await query.edit_message_text(details, reply_markup=InlineKeyboardMarkup(buttons))


# === ОБРАБОТКА ДЕЙСТВИЙ ===
async def handle_task_action(query, data):
    _, action, task_id, group = data.split(":")
    text_map = {
        "scan": "🔁 Сканирование инициировано",
        "process": "♻️ Обработка данных",
        "update": "📤 Обновление в Google Sheets",
        "clear": "🧹 Флаг сброшен",
        "logs": "🪵 Логи: (заглушка)"
    }
    await query.edit_message_text(f"{text_map.get(action, '❔ Неизвестное действие')} для задачи {task_id}")


# === ЗАПУСК ===
if __name__ == '__main__':
    print("🚀 Запуск Telegram бота...")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_callback))

    print("🤖 Бот запущен")
    app.run_polling()
