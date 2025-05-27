# tg_bot/handlers/calendar.py

from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.viewing_shift import render_shift_dashboard
from utils.utils import get_current_shift_and_date
from core.config import TIMEZONE
from datetime import datetime, timedelta
import calendar
from zoneinfo import ZoneInfo

router = Router()

try:
    timezone = ZoneInfo(TIMEZONE)
except Exception as e:
    raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

def build_calendar(year: int, month: int, selected_date, selected_shift_type) -> InlineKeyboardBuilder:
    now = datetime.now(timezone)
    current_shift_type, current_date = get_current_shift_and_date(now)

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=f"{calendar.month_name[month]} {year}", callback_data="ignore"))

    week_days = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su']
    builder.row(*[InlineKeyboardButton(text=day, callback_data="ignore") for day in week_days])

    for week in calendar.monthcalendar(year, month):
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(text=" ", callback_data="ignore"))
            else:
                day_date = datetime(year, month, day).date()
                if day_date == current_date:
                    icon = "🌞" if current_shift_type == "day" else "🌙"
                    label = f"{icon}{day}"
                else:
                    label = str(day)

                row.append(InlineKeyboardButton(
                    text=label,
                    callback_data=f"day:{day}:{month}:{year}"
                ))
        builder.row(*row)

    builder.row(
        InlineKeyboardButton(text="◀️", callback_data=f"prev_month:{month}:{year}"),
        InlineKeyboardButton(text="▶️", callback_data=f"next_month:{month}:{year}")
    )
    builder.row(InlineKeyboardButton(text="🔙 Back", callback_data="calendar_back"))

    return builder

@router.callback_query(F.data == "select_shift")
async def open_calendar(callback: CallbackQuery, state: FSMContext, bot: Bot):
    now = datetime.now()
    data = await state.get_data()
    selected_date = data.get("selected_date")
    selected_shift_type = data.get("selected_shift_type")

    # 🛡 Fallback если сообщение не сохранено
    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    if not chat_id or not message_id:
        fallback = await callback.message.answer("📅 Calendar not available (no base message)")
        await state.update_data(chat_id=fallback.chat.id, message_id=fallback.message_id)
        return

    builder = build_calendar(now.year, now.month, selected_date, selected_shift_type)

    await state.set_state(ShiftNavigationState.CALENDAR)
    await state.update_data(calendar_year=now.year, calendar_month=now.month)

    header = (
        f"📅 Select a date\n"
        f"Currently viewing: <b>{selected_date.strftime('%d %b')} — "
        f"{'🌞 Day' if selected_shift_type == 'day' else '🌙 Night'} shift</b>"
    )

    await bot.edit_message_text(
        text=header,
        chat_id=chat_id,
        message_id=message_id,
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("prev_month"))
async def prev_month(callback: CallbackQuery, state: FSMContext, bot: Bot):
    _, m, y = callback.data.split(":")
    date = datetime(int(y), int(m), 1) - timedelta(days=1)

    data = await state.get_data()
    builder = build_calendar(date.year, date.month, data["selected_date"], data["selected_shift_type"])

    await state.update_data(calendar_year=date.year, calendar_month=date.month)

    header = (
        f"📅 Select a date\n"
        f"Currently viewing: <b>{data['selected_date'].strftime('%d %b')} — "
        f"{'🌞 Day' if data['selected_shift_type'] == 'day' else '🌙 Night'} shift</b>"
    )

    await bot.edit_message_text(
        text=header,
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )


@router.callback_query(F.data.startswith("next_month"))
async def next_month(callback: CallbackQuery, state: FSMContext, bot: Bot):
    _, m, y = callback.data.split(":")
    date = datetime(int(y), int(m), 28) + timedelta(days=4)
    date = date.replace(day=1)

    data = await state.get_data()
    builder = build_calendar(date.year, date.month, data["selected_date"], data["selected_shift_type"])

    await state.update_data(calendar_year=date.year, calendar_month=date.month)

    header = (
        f"📅 Select a date\n"
        f"Currently viewing: <b>{data['selected_date'].strftime('%d %b')} — "
        f"{'🌞 Day' if data['selected_shift_type'] == 'day' else '🌙 Night'} shift</b>"
    )

    await bot.edit_message_text(
        text=header,
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )


@router.callback_query(F.data.startswith("day:"))
async def pick_day(callback: CallbackQuery, state: FSMContext, bot: Bot):
    _, d, m, y = callback.data.split(":")
    selected = datetime(int(y), int(m), int(d)).date()

    await state.set_state(ShiftNavigationState.SHIFT_TYPE)
    await state.update_data(selected_date=selected)

    builder = InlineKeyboardBuilder()
    builder.button(text="🌞 Day", callback_data="shift_type:day")
    builder.button(text="🌙 Night", callback_data="shift_type:night")
    builder.button(text="🔙 Back", callback_data="select_shift")
    builder.adjust(1)

    data = await state.get_data()
    await bot.edit_message_text(
        text=f"Selected: <b>{selected.strftime('%d %b %Y')}</b>\nSelect shift type:",
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )


@router.callback_query(F.data.startswith("shift_type:"))
async def select_shift_type(callback: CallbackQuery, state: FSMContext, bot: Bot):
    _, shift_type = callback.data.split(":")
    await state.set_state(ShiftNavigationState.VIEWING_SHIFT)
    await state.update_data(selected_shift_type=shift_type, is_current_shift=False)
    await render_shift_dashboard(callback.message, state, bot)


@router.callback_query(F.data == "calendar_back")
async def back_to_dashboard(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(ShiftNavigationState.VIEWING_SHIFT)
    await render_shift_dashboard(callback.message, state, bot)
