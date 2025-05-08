# bot/handlers/menu.py

from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.exceptions import TelegramBadRequest
from bot.states import MenuStates
from zoneinfo import ZoneInfo
from core.config import AUTHORIZED_USERS, TIMEZONE
from bot.keyboards import main_menu_kb, rotations_shift_kb, task_kb, scanner_log_kb
from bot.utils_bot import (
    get_surrounding_tabs, get_logs_for_scanner, get_logs_for_shift,
    get_logs_for_rot_task, get_logs_for_sheet_task, get_current_datetime, get_connection
)
from bot.db_access import (
    get_rotations_stats, get_all_tracked_tables,
    get_rotations_tasks_by_tab, get_task_by_id,
    get_sheets_stats, get_sheets_tasks, get_sheet_by_id
)
from datetime import datetime

from bot.utils_bot import format_datetime_pl
from bot.settings_access import is_scanner_enabled, set_scanner_enabled

router = Router()

def generate_main_menu_text():
    now = get_current_datetime()
    stats_rot = get_rotations_stats()
    stats_sheets = get_sheets_stats()
    tracked = get_all_tracked_tables()

    return (
        f"<b>👋 Привет! Я — бот мониторинга Google Sheets.</b>\n\n"
        f"📊 <b>RotationsInfo:</b> {stats_rot['total']} задач, {stats_rot['errors']} ошибок\n"
        f"📋 <b>SheetsInfo:</b> {stats_sheets['total']} задач, {stats_sheets['errors']} ошибок\n"
        f"📦 <b>TrackedTables:</b> {len(tracked)} документов\n\n"
        f"🔐 <b>Токен:</b> активен\n"
        f"🕒 <i>{now}</i>"
    )

@router.message(F.text == "/start")
async def handle_start(msg: types.Message, state: FSMContext):
    if msg.from_user.id not in AUTHORIZED_USERS:
        return await msg.answer("🚫 Доступ запрещён")
    
    await state.clear()
    await state.set_state(MenuStates.MAIN_MENU)

    await msg.answer(generate_main_menu_text(), reply_markup=main_menu_kb())

@router.callback_query(F.data == "rotations")
async def handle_rotations(query: CallbackQuery, state: FSMContext):
    await state.set_state(MenuStates.ROTATIONS_MENU)

    now = datetime.now(ZoneInfo(TIMEZONE))
    shift = "Day shift" if 6 <= now.hour < 18 else "Night shift"
    time_str = now.strftime("%d.%m.%Y %H:%M")

    # Группируем задачи по сменам
    tabs = get_surrounding_tabs()
    all_tasks = []
    for tab in tabs:
        tasks = get_rotations_tasks_by_tab(tab)
        for t in tasks:
            t["shift"] = tab
        all_tasks.extend(tasks)

    stats = get_rotations_stats()
    total = stats["total"]
    errors = stats["errors"]

    # Найти проблемные задачи (по scan_failures, сортировка и лимит)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name_of_process, scan_failures FROM RotationsInfo
        WHERE scan_failures > 0
        ORDER BY scan_failures DESC
        LIMIT 5
    """)
    top_errors = cursor.fetchall()
    conn.close()

    lines = [
        f"🕒 {shift} {time_str}",
        f"🔁 Найдено {total} задач в RotationsInfo",
        f"⚠️ Ошибок: {errors}\n",
        "<b>🔍 Проблемные задачи:</b>"
    ]

    for name, fails in top_errors:
        lines.append(f"❌ {name} — {fails} ошибок")

    text = "\n".join(lines)
    await query.message.edit_text(text, reply_markup=rotations_shift_kb(tabs))

@router.callback_query(F.data.startswith("shift:"))
async def handle_shift(query: CallbackQuery, state: FSMContext):
    tab = query.data.split(":")[1]
    await state.update_data(selected_shift=tab)
    tasks = get_rotations_tasks_by_tab(tab)
    await state.set_state(MenuStates.SHIFT_SELECTED)
    await query.message.edit_text(f"📋 Задачи в смене {tab}:", reply_markup=task_kb(tasks))

@router.callback_query(F.data.startswith("task:"))
async def handle_task(query: CallbackQuery, state: FSMContext):
    task_id = query.data.split(":")[1]
    task = get_task_by_id(task_id)
    if not task:
        return await query.message.edit_text("⚠️ Задача не найдена")

    await state.set_state(MenuStates.TASK_SELECTED)
    await state.update_data(selected_task_id=task_id, task_source="rotations")

    scan_ok = max(0, task['scan_quantity'] - task['scan_failures'])
    update_ok = max(0, task['update_quantity'] - task['update_failures'])

    text = (
        f"<b>📄 Процесс:</b> {task['name']}\n"
        f"<b>📍 Источник:</b> {task['source']}\n"
        f"<b>🧮 Хеш:</b> <code>{task['hash']}</code>\n"
        f"<b>🕓 Интервал сканирования:</b> {task['scan_interval']} сек\n"
        "\n"
        f"<b>🔁 Сканирование</b>\n"
        f"Последний: {format_datetime_pl(task['last_scan'])}\n"
        f"Всего: {task['scan_quantity']} | ✅ Успешно: {scan_ok} | ❌ Ошибки: {task['scan_failures']}\n"
        "\n"
        f"<b>📤 Вставка</b>\n"
        f"Последняя: {format_datetime_pl(task['last_update'])}\n"
        f"Всего: {task['update_quantity']} | ✅ Успешно: {update_ok} | ❌ Ошибки: {task['update_failures']}"
    )

    if task["scan_failures"] > 0 or task["update_failures"] > 0:
        text += (
            "\n\n<b>🧾 Детали ошибок:</b>\n"
            "<i>🔍 Здесь будет подробный лог</i>"
        )

    await query.message.edit_text(text, reply_markup=scanner_log_kb("task", "rotations"))

@router.callback_query(F.data.startswith("rotations_action:"))
async def handle_rotations_action(query: CallbackQuery, state: FSMContext):
    action = query.data.split(":")[1]
    data = await state.get_data()
    task_id = data.get("selected_task_id")

    if not task_id:
        return await query.answer("⚠️ Задача не выбрана", show_alert=True)

    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        if action == "scan":
            cursor.execute("UPDATE RotationsInfo SET last_scan = ?, scan_quantity = scan_quantity + 1 WHERE id = ?", (now, task_id))
            result = "🔁 Сканирование обновлено"
        elif action == "update":
            cursor.execute("UPDATE RotationsInfo SET last_update = ?, update_quantity = update_quantity + 1 WHERE id = ?", (now, task_id))
            result = "📤 Обновление записано"
        elif action == "clear":
            cursor.execute("UPDATE RotationsInfo SET scan_failures = 0, update_failures = 0 WHERE id = ?", (task_id,))
            result = "🧹 Ошибки сброшены"
        else:
            result = "❔ Неизвестное действие"
        conn.commit()
    except Exception as e:
        result = f"❌ Ошибка: {e}"
    finally:
        conn.close()

    await query.answer(result)
    await update_task_view(query, task_id)

@router.callback_query(F.data.startswith("logs:"))
async def handle_logs(query: CallbackQuery, state: FSMContext):
    level = query.data.split(":")[1]
    data = await state.get_data()
    source = data.get("task_source")
    
    if level == "scanner":
        text = get_logs_for_scanner()
    elif level == "shift":
        text = get_logs_for_shift(data.get("selected_shift"))
    elif level == "task":
        task_id = data.get("selected_task_id") if source == "rotations" else data.get("selected_sheet_id")
        if source == "rotations":
            text = get_logs_for_rot_task(task_id)
        elif source == "sheets":
            text = get_logs_for_sheet_task(task_id)
        else:
            text = "⚠️ Источник задачи неизвестен"
    else:
        text = "⚠️ Уровень логов неизвестен"

    try:
        await query.message.edit_text(f"🪵 Логи:\n\n{text[-4000:]}", reply_markup=scanner_log_kb(level, source))
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await query.answer("⚠️ Уже актуально", show_alert=False)
        else:
            raise

# Остальная часть (назад, sheets, tracked) — без изменений, но аналогично обнови `task_source` при выборе sheet

# --- Назад / Домой ---
@router.callback_query(F.data == "back:main")
async def back_main(query: CallbackQuery, state: FSMContext):
    await state.set_state(MenuStates.MAIN_MENU)
    await query.message.edit_text(generate_main_menu_text(), reply_markup=main_menu_kb())

@router.callback_query(F.data == "back:rotations")
async def back_rotations(query: CallbackQuery, state: FSMContext):
    await state.set_state(MenuStates.ROTATIONS_MENU)
    tabs = get_surrounding_tabs()
    await query.message.edit_text("🔁 Выберите смену:", reply_markup=rotations_shift_kb(tabs))

@router.callback_query(F.data == "back:shift")
async def back_shift(query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    tab = data.get("selected_shift")
    tasks = get_rotations_tasks_by_tab(tab)
    await state.set_state(MenuStates.SHIFT_SELECTED)
    await query.message.edit_text(f"📋 Задачи в смене {tab}:", reply_markup=task_kb(tasks))

@router.callback_query(F.data == "sheets")
async def handle_sheets_menu(query: CallbackQuery, state: FSMContext):
    await state.set_state(MenuStates.SHEETS_MENU)
    tasks = get_sheets_tasks()

    if not tasks:
        return await query.message.edit_text("⚠️ Задачи SheetsInfo не найдены.")

    now = datetime.now(ZoneInfo(TIMEZONE))
    shift = "Day shift" if 6 <= now.hour < 18 else "Night shift"
    time_str = now.strftime("%d.%m.%Y %H:%M")

    # Средние показатели
    total = len(tasks)
    failures = sum(t["failures"] for t in tasks)
    avg_fail = round(failures / total, 2) if total else 0

    # ТОП-5 задач с ошибками
    problematic = sorted(tasks, key=lambda x: x["failures"], reverse=True)[:5]

    lines = [
        f"🕒 {shift} {time_str}",
        f"📋 Найдено {total} задач в SheetsInfo",
        f"⚠️ Всего ошибок: {failures}, средне: {avg_fail}\n",
        "<b>🔍 Проблемные задачи:</b>"
    ]

    for t in problematic:
        if t["failures"] == 0:
            continue
        lines.append(f"❌ {t['name']} — {t['failures']} ошибок")

    text = "\n".join(lines)[:4000]

    # Кнопки по 2 в ряд
    rows = []
    row = []
    for task in tasks:
        row.append(InlineKeyboardButton(text=task["name"], callback_data=f"sheet:{task['id']}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="back:main")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=rows)

    await query.message.edit_text(text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("sheet:"))
async def handle_sheet_task(query: CallbackQuery, state: FSMContext):
    sheet_id = query.data.split(":")[1]
    sheet = get_sheet_by_id(sheet_id)
    if not sheet:
        return await query.message.edit_text("⚠️ Задача не найдена")

    await state.set_state(MenuStates.SHEET_TASK_SELECTED)
    await state.update_data(selected_sheet_id=sheet_id, task_source="sheets")

    scan_ok = max(0, sheet['scan_quantity'] - sheet['scan_failures'])
    update_ok = max(0, sheet['update_quantity'] - sheet['update_failures'])

    text = (
        f"<b>📄 Процесс:</b> {sheet['name']}\n"
        f"<b>📍 Источник:</b> {sheet['source']}\n"
        f"<b>🧮 Хеш:</b> <code>{sheet['hash']}</code>\n"
        f"<b>🕓 Интервал сканирования:</b> {sheet['scan_interval']} сек\n"
        "\n"
        f"<b>🔁 Сканирование</b>\n"
        f"Последний: {format_datetime_pl(sheet['last_scan'])}\n"
        f"Всего: {sheet['scan_quantity']} | ✅ Успешно: {scan_ok} | ❌ Ошибки: {sheet['scan_failures']}\n"
        "\n"
        f"<b>📤 Вставка</b>\n"
        f"Последняя: {format_datetime_pl(sheet['last_update'])}\n"
        f"Всего: {sheet['update_quantity']} | ✅ Успешно: {update_ok} | ❌ Ошибки: {sheet['update_failures']}"
    )

    if sheet["scan_failures"] > 0 or sheet["update_failures"] > 0:
        text += (
            "\n\n<b>🧾 Детали ошибок:</b>\n"
            "<i>🔍 Здесь будет подробный лог</i>"
        )

    action_buttons = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔁 Сканировать", callback_data="sheet_action:scan"),
            InlineKeyboardButton(text="📤 Обновить", callback_data="sheet_action:update")
        ],
        [
            InlineKeyboardButton(text="🧹 Сброс", callback_data="sheet_action:clear"),
            InlineKeyboardButton(text="🪵 Логи", callback_data="logs:task")
        ],
        [
            InlineKeyboardButton(text="🔙 Назад", callback_data="back:sheets"),
            InlineKeyboardButton(text="🏠 Домой", callback_data="back:main")
        ]
    ])

    await query.message.edit_text(text, reply_markup=scanner_log_kb("task", "sheets"))

@router.callback_query(F.data.startswith("sheets_action:"))
async def handle_sheet_action(query: CallbackQuery, state: FSMContext):
    action = query.data.split(":")[1]
    data = await state.get_data()
    sheet_id = data.get("selected_sheet_id")

    if not sheet_id:
        return await query.answer("⚠️ Задача не выбрана", show_alert=True)

    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        if action == "scan":
            cursor.execute("UPDATE SheetsInfo SET last_scan = ?, scan_quantity = scan_quantity + 1 WHERE id = ?", (now, sheet_id))
            result = "🔁 Сканирование обновлено"
        elif action == "update":
            cursor.execute("UPDATE SheetsInfo SET last_update = ?, update_quantity = update_quantity + 1 WHERE id = ?", (now, sheet_id))
            result = "📤 Обновление записано"
        elif action == "clear":
            cursor.execute("UPDATE SheetsInfo SET scan_failures = 0, update_failures = 0 WHERE id = ?", (sheet_id,))
            result = "🧹 Ошибки сброшены"
        else:
            result = "❔ Неизвестное действие"
        conn.commit()
    except Exception as e:
        result = f"❌ Ошибка: {e}"
    finally:
        conn.close()

    await query.answer(result)
    await update_sheet_view(query, state)

@router.callback_query(F.data == "back:sheets")
async def back_sheets_menu(query: CallbackQuery, state: FSMContext):
    await state.set_state(MenuStates.SHEETS_MENU)
    return await handle_sheets_menu(query, state)

# === 🧩 Обновление карточки задачи (Rotations) ===
async def update_task_view(query: CallbackQuery, task_id: str):
    task = get_task_by_id(task_id)
    if not task:
        return await query.answer("⚠️ Задача не найдена", show_alert=True)

    scan_ok = max(0, task['scan_quantity'] - task['scan_failures'])
    update_ok = max(0, task['update_quantity'] - task['update_failures'])

    text = (
        f"<b>📄 Процесс:</b> {task['name']}\n"
        f"<b>📍 Источник:</b> {task['source']}\n"
        f"<b>🧮 Хеш:</b> <code>{task['hash']}</code>\n"
        f"<b>🕓 Интервал сканирования:</b> {task['scan_interval']} сек\n\n"
        f"<b>🔁 Сканирование</b>\n"
        f"Последний: {format_datetime_pl(task['last_scan'])}\n"
        f"Всего: {task['scan_quantity']} | ✅ Успешно: {scan_ok} | ❌ Ошибки: {task['scan_failures']}\n\n"
        f"<b>📤 Вставка</b>\n"
        f"Последняя: {format_datetime_pl(task['last_update'])}\n"
        f"Всего: {task['update_quantity']} | ✅ Успешно: {update_ok} | ❌ Ошибки: {task['update_failures']}"
    )

    if task["scan_failures"] > 0 or task["update_failures"] > 0:
        text += "\n\n<b>🧾 Детали ошибок:</b>\n<i>🔍 Здесь будет подробный лог</i>"

    try:
        await query.message.edit_text(text, reply_markup=scanner_log_kb("task", "rotations"))
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await query.answer("⚠️ Уже актуально", show_alert=False)
        elif "message to edit not found" in str(e):
            await query.answer("❌ Сообщение не найдено", show_alert=True)
        else:
            raise

# === 🧩 Обновление карточки задачи (Sheets) ===
async def update_sheet_view(query: CallbackQuery, sheet_id: str):
    task = get_sheet_by_id(sheet_id)
    if not task:
        return await query.answer("⚠️ Задача не найдена", show_alert=True)

    scan_ok = max(0, task['scan_quantity'] - task['scan_failures'])
    update_ok = max(0, task['update_quantity'] - task['update_failures'])

    text = (
        f"<b>📄 Процесс:</b> {task['name']}\n"
        f"<b>📍 Источник:</b> {task['source']}\n"
        f"<b>🧮 Хеш:</b> <code>{task['hash']}</code>\n"
        f"<b>🕓 Интервал сканирования:</b> {task['scan_interval']} сек\n\n"
        f"<b>🔁 Сканирование</b>\n"
        f"Последний: {format_datetime_pl(task['last_scan'])}\n"
        f"Всего: {task['scan_quantity']} | ✅ Успешно: {scan_ok} | ❌ Ошибки: {task['scan_failures']}\n\n"
        f"<b>📤 Вставка</b>\n"
        f"Последняя: {format_datetime_pl(task['last_update'])}\n"
        f"Всего: {task['update_quantity']} | ✅ Успешно: {update_ok} | ❌ Ошибки: {task['update_failures']}"
    )

    if task["scan_failures"] > 0 or task["update_failures"] > 0:
        text += "\n\n<b>🧾 Детали ошибок:</b>\n<i>🔍 Здесь будет подробный лог</i>"

    try:
        await query.message.edit_text(text, reply_markup=scanner_log_kb("task", "sheets"))
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await query.answer("⚠️ Уже актуально", show_alert=False)
        elif "message to edit not found" in str(e):
            await query.answer("❌ Сообщение не найдено", show_alert=True)
        else:
            raise

# Получение таблиц активных в выбранном месяце
def get_tracked_tables_by_month(month: int, year: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT table_type, label, spreadsheet_id, valid_from, valid_to FROM TrackedTables")
    rows = cursor.fetchall()
    conn.close()

    target = datetime(year, month, 1)
    current_month = []
    for row in rows:
        table_type, label, spreadsheet_id, valid_from, valid_to = row
        if not valid_from or not valid_to:
            continue  # Без полного интервала — игнорируем
        valid_from_dt = datetime.strptime(valid_from, "%d.%m.%Y")
        valid_to_dt = datetime.strptime(valid_to, "%d.%m.%Y")

        if valid_from_dt <= target <= valid_to_dt:
            current_month.append({
                "table_type": table_type,
                "label": label,
                "spreadsheet_id": spreadsheet_id,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "status": "✅" if spreadsheet_id else "❌"
            })

    expected_types = get_all_table_types()
    table_map = {entry['table_type']: entry for entry in current_month}

    full_result = []
    for t_type in expected_types:
        entry = table_map.get(t_type)
        if entry:
            full_result.append(entry)
        else:
            full_result.append({
                "table_type": t_type,
                "label": t_type.upper(),
                "spreadsheet_id": None,
                "valid_from": None,
                "valid_to": None,
                "status": "❌"
            })
    return full_result

@router.callback_query(F.data == "tracked")
async def handle_tracked_tables(query: CallbackQuery, state: FSMContext):
    now = datetime.now(ZoneInfo(TIMEZONE))
    await state.update_data(month=now.month, year=now.year)
    await show_tracked_tables(query, state)

@router.callback_query(F.data == "tracked:month")
async def change_tracked_month(query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    month = data.get("month")
    year = data.get("year")

    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year += 1

    await state.update_data(month=next_month, year=next_year)

    tables = get_tracked_tables_by_month(next_month, next_year)
    if any(t['status'] == "❌" for t in tables):
        await query.answer("⚠️ Похоже, шаблоны на этот месяц ещё не подготовлены.", show_alert=True)

    await show_tracked_tables(query, state)

async def show_tracked_tables(query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    month = data.get("month")
    year = data.get("year")

    tables = get_tracked_tables_by_month(month, year)
    now = datetime.now(ZoneInfo(TIMEZONE))
    time_str = now.strftime("%d.%m.%Y %H:%M")
    shift = "Day shift" if 6 <= now.hour < 18 else "Night shift"

    display_month = datetime(year, month, 1).strftime("%B %Y").capitalize()

    lines = [
        f"🕒 {shift} {time_str}",
        f"📦 Документы на {display_month}",
        "",
        "<b>📌 Список документов:</b>"
    ]
    for t in tables:
        lines.append(f"{t['status']} <b>{t['label']}</b> — <code>{t['spreadsheet_id'] or '—'}</code>")
    text = "\n".join(lines)

    buttons = []
    row = []
    for t in tables:
        row.append(InlineKeyboardButton(text=f"{t['status']} {t['label']}", callback_data=f"doc:{t['table_type']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    if any(t['status'] == '❌' for t in tables):
        buttons.insert(-1, [InlineKeyboardButton(text="➕ Подготовить шаблоны", callback_data="prepare:templates")])

    buttons.append([
        InlineKeyboardButton(text="📅 Сменить месяц", callback_data="tracked:month"),
        InlineKeyboardButton(text="🔙 Назад", callback_data="back:main")
    ])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await query.message.edit_text(text, reply_markup=keyboard)

# Получение всех шаблонов по типу таблицы
def get_all_table_types():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT table_type FROM TrackedTables")
    types = [row[0] for row in cursor.fetchall()]
    conn.close()
    return types

@router.callback_query(F.data == "prepare:templates")
async def handle_prepare_templates(query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    month = data.get("month")
    year = data.get("year")

    table_types = get_all_table_types()
    existing = get_tracked_tables_by_month(month, year)
    existing_types = [e["table_type"] for e in existing if e["spreadsheet_id"]]

    to_create = [t for t in table_types if t not in existing_types]
    if not to_create:
        return await query.answer("✅ Все шаблоны уже есть", show_alert=True)

    from calendar import monthrange
    valid_from = f"01.{month:02d}.{year}"
    valid_to = f"{monthrange(year, month)[1]:02d}.{month:02d}.{year}"

    conn = get_connection()
    cursor = conn.cursor()
    for table_type in to_create:
        cursor.execute(
            "INSERT INTO TrackedTables (table_type, label, spreadsheet_id, valid_from, valid_to) VALUES (?, ?, ?, ?, ?)",
            (table_type, table_type.upper(), '', valid_from, valid_to)
        )
    conn.commit()
    conn.close()

    await query.answer(f"✅ Добавлены {len(to_create)} шаблонов")
    await show_tracked_tables(query, state)

@router.callback_query(F.data.startswith("toggle:"))
async def toggle_scanner(query: CallbackQuery):
    scanner_type = query.data.split(":")[1]
    key = f"{scanner_type}_scanner"

    current_status = is_scanner_enabled(key)
    set_scanner_enabled(key, not current_status)

    await query.answer(
        f"{'✅ Включено' if not current_status else '⛔ Выключено'}: {scanner_type.capitalize()}"
    )

    await query.message.edit_text(
        generate_main_menu_text(),
        reply_markup=main_menu_kb()
    )
