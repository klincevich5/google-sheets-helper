# keyboards.py

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 RotationsInfo", callback_data="rotations")],
        [InlineKeyboardButton(text="📋 SheetsInfo", callback_data="sheets")],
        [InlineKeyboardButton(text="📦 TrackedTables", callback_data="tracked")],
        [InlineKeyboardButton(text="🪵 Логи сканеров", callback_data="logs:scanner")]
    ])

def rotations_shift_kb(tabs: list[str]):
    buttons = []
    row = []
    for i, tab in enumerate(tabs):
        row.append(InlineKeyboardButton(text=tab, callback_data=f"shift:{tab}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="🏠 Домой", callback_data="back:main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def task_kb(tasks: list[dict]):
    rows = []
    row = []
    for task in tasks:
        row.append(InlineKeyboardButton(text=task['name'], callback_data=f"task:{task['id']}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton(text="🔙 Назад", callback_data="back:rotations"),
        InlineKeyboardButton(text="🏠 Домой", callback_data="back:main")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def scanner_log_kb(level: str, source: str):
    buttons = []

    if level == "task":
        buttons.append([
            InlineKeyboardButton(text="🔁 Сканировать", callback_data=f"{source}_action:scan"),
            InlineKeyboardButton(text="📤 Обновить", callback_data=f"{source}_action:update")
        ])
        buttons.append([
            InlineKeyboardButton(text="🧹 Сброс", callback_data=f"{source}_action:clear"),
            InlineKeyboardButton(text="🪵 Логи", callback_data="logs:task")
        ])
        if source == "sheets":
            buttons.append([
                InlineKeyboardButton(text="🔙 Назад", callback_data="back:sheets"),
                InlineKeyboardButton(text="🏠 Домой", callback_data="back:main")
            ])
        else:
            buttons.append([
                InlineKeyboardButton(text="🔙 Назад", callback_data="back:shift"),
                InlineKeyboardButton(text="🏠 Домой", callback_data="back:main")
            ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def tracked_tables_kb(docs: dict):
    types = sorted(docs.keys())
    rows = []
    row = []
    for t in types:
        doc = docs[t]
        label = doc["label"]
        status = doc["status"]
        row.append(InlineKeyboardButton(f"{status} {label}", callback_data=f"tracked:{label}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("📆 Сменить месяц", callback_data="change:month"),
        InlineKeyboardButton("🔙 Назад", callback_data="back:main")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)
