from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.fsm.context import FSMContext
from sqlalchemy import select
from datetime import datetime
from zoneinfo import ZoneInfo
import json

from core.config import TIMEZONE, ROTATION_ORDER
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.common_callbacks import push_state, check_stranger_callback
from database.db_models import RotationsInfo
from database.session import get_session

router = Router()

def shorten_name(name: str, max_len: int = 14) -> str:
    return (name[:max_len - 1] + "…") if len(name) > max_len else name

@router.callback_query(F.data == "select_rotation", ShiftNavigationState.VIEWING_SHIFT)
async def select_rotation(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
    await push_state(state, ShiftNavigationState.VIEWING_SHIFT)
    await state.set_state(ShiftNavigationState.VIEWING_SHIFT)
    await state.update_data(rotation_page=0)

    buttons = [
        InlineKeyboardButton(text=name, callback_data=f"rotation:{name}")
        for name in ROTATION_ORDER
    ]

    keyboard_rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    keyboard_rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="return_shift")])

    await callback.message.edit_text(
        text="🔄 <b>Ротация по этажам</b>\n\nВыберите процесс для просмотра ротации:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_rows),
        parse_mode="HTML"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("rotation:"), ShiftNavigationState.VIEWING_SHIFT)
async def view_rotation_detail(callback: CallbackQuery, state: FSMContext, bot: Bot, process_name: str = None):
    if await check_stranger_callback(callback): return
    data = await state.get_data()
    if process_name is None:
        process_name = callback.data.removeprefix("rotation:")
    selected_date = data.get("selected_date")
    selected_shift_type = data.get("selected_shift_type")
    page = data.get("rotation_page", 0)

    if not selected_date or not selected_shift_type:
        await callback.answer("Дата или смена не выбраны", show_alert=True)
        return

    timezone = ZoneInfo(TIMEZONE)
    first_of_month = datetime(selected_date.year, selected_date.month, 1, tzinfo=timezone)
    source_page_name = f"{selected_shift_type.upper()} {selected_date.day}"

    with get_session() as session:
        stmt = select(RotationsInfo).where(
            RotationsInfo.related_month == first_of_month,
            RotationsInfo.name_of_process == process_name,
            RotationsInfo.source_page_name == source_page_name,
        )
        result = session.execute(stmt)
        rotation: RotationsInfo = result.scalars().first()

    if not rotation or not rotation.values_json:
        # ⛔ Очистим выбранный процесс и страницу
        await state.update_data(current_rotation=None, rotation_page=0)

        await callback.message.edit_text(
            f"ℹ️ <b>Нет данных по ротации</b>\n"
            f"<b>Процесс:</b> {process_name}\n"
            f"<b>Страница:</b> {source_page_name}\n"
            f"<b>Месяц:</b> {first_of_month.date()}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="select_rotation")]]
            )
        )
        return


    try:
        values = json.loads(rotation.values_json)
    except Exception as e:
        await callback.message.edit_text(
            f"❌ <b>Ошибка при чтении values_json</b>\n<pre>{e}</pre>",
            parse_mode="HTML"
        )
        return

    if not isinstance(values, list) or len(values) < 3:
        await callback.message.edit_text(
            "⚠️ Некорректный формат данных в values_json.",
            parse_mode="HTML"
        )
        return

    header = values[1]
    rows = values[2:-1]

    max_columns = 3  # ⬅️ отображаем только 4 колонки
    start = 1 + page * max_columns
    end = start + max_columns

    visible_header = [header[0]] + header[start:end]
    table_lines = []

    # Заголовок
    table_lines.append("│ {:<16} │ {}".format(visible_header[0], " │ ".join(f"{h:^7}" for h in visible_header[1:])))
    table_lines.append("├" + "─" * 18 + "┼" + "┼".join(["───────"] * len(visible_header[1:])) + "┤")

    for row in rows:
        if not row or len(row) <= start:
            continue
        name = row[0][:16]
        cells = row[start:end]
        line = "│ {:<16} │ {}".format(name, " │ ".join(f"{c:^7}" for c in cells))
        table_lines.append(line)

    table_text = "<pre>" + "\n".join(table_lines) + "</pre>"

    # Кнопки навигации
    nav_buttons = []
    if start > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀", callback_data=f"rotation_scroll:left:{process_name}"))
    if end < len(header):
        nav_buttons.append(InlineKeyboardButton(text="▶", callback_data=f"rotation_scroll:right:{process_name}"))

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            nav_buttons if nav_buttons else [],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="select_rotation")]
        ]
    )

    await state.update_data(current_rotation=process_name)
    await callback.message.edit_text(
        text=f"📋 <b>Ротация: {process_name}</b> — {source_page_name}\n\n{table_text}",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("rotation_scroll:"), ShiftNavigationState.VIEWING_SHIFT)
async def scroll_rotation(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
    try:
        _, direction, process_name = callback.data.split(":", maxsplit=2)
    except ValueError:
        await callback.answer("Некорректный формат callback", show_alert=True)
        return

    data = await state.get_data()
    page = data.get("rotation_page", 0)
    if direction == "right":
        page += 1
    elif direction == "left" and page > 0:
        page -= 1

    await state.update_data(rotation_page=page, current_rotation=process_name)
    await view_rotation_detail(callback, state, bot, process_name = process_name)



@router.callback_query(F.data == "noop")
async def noop_handler(callback: CallbackQuery):
    if await check_stranger_callback(callback): return
    await callback.answer()  # Просто молча глотаем клик
