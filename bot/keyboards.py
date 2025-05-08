# bot/keyboards.py

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from bot.settings_access import is_scanner_enabled

def main_menu_kb():
    buttons = [[
        InlineKeyboardButton(text="📊 RotationsInfo", callback_data="rotations"),
        InlineKeyboardButton(text="📋 SheetsInfo", callback_data="sheets")
    ], [
        InlineKeyboardButton(text="📦 TrackedTables", callback_data="tracked")
    ]]

    rot = is_scanner_enabled("rotations_scanner")
    sheets = is_scanner_enabled("sheets_scanner")

    buttons.append([
        InlineKeyboardButton(
            text="🟢 Rotations ON" if rot else "🔴 Rotations OFF",
            callback_data="toggle:rotations"
        ),
        InlineKeyboardButton(
            text="🟢 Sheets ON" if sheets else "🔴 Sheets OFF",
            callback_data="toggle:sheets"
        )
    ])

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

def rotations_shift_kb(tabs: list[str]) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for tab in tabs:
        row.append(InlineKeyboardButton(text=tab, callback_data=f"shift:{tab}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="back:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
