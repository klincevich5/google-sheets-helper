from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.viewing_shift import render_shift_dashboard
from tg_bot.utils.utils import get_current_shift_and_date
from core.config import TIMEZONE
from datetime import datetime, timedelta
import calendar
from zoneinfo import ZoneInfo
from tg_bot.handlers.common_callbacks import push_state
import logging

router = Router()

timezone = ZoneInfo(TIMEZONE)

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
                if day_date == selected_date:
                    label = f"🟩{day}"
                elif day_date == current_date:
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
    try:
        now = datetime.now()
        data = await state.get_data()
        selected_date = data.get("selected_date")
        selected_shift_type = data.get("selected_shift_type")
        chat_id = data.get("chat_id")
        message_id = data.get("message_id")
        if not chat_id or not message_id:
            fallback = await callback.message.answer("📅 Calendar not available (no base message)")
            await state.update_data(chat_id=fallback.chat.id, message_id=fallback.message_id)
            return
        builder = build_calendar(now.year, now.month, selected_date, selected_shift_type)
        await push_state(state, ShiftNavigationState.CALENDAR)
        await state.set_state(ShiftNavigationState.CALENDAR)
        await state.update_data(calendar_year=now.year, calendar_month=now.month)
        header = (
            f"📅 Select a date\n"
            f"Current shift: <b>{now.strftime('%d %b %Y')} — {'🌞 Day' if selected_shift_type == 'day' else '🌙 Night'} shift</b>\n"
            f"Selected: <b>{selected_date.strftime('%d %b %Y')}</b>"
        )
        await bot.edit_message_text(
            text=header,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=builder.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        logging.exception("Error in open_calendar")
        await callback.answer("Произошла ошибка!", show_alert=True)

@router.callback_query(F.data.startswith("prev_month"))
async def prev_month(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        _, m, y = callback.data.split(":")
        date = datetime(int(y), int(m), 1) - timedelta(days=1)
        data = await state.get_data()
        builder = build_calendar(date.year, date.month, data["selected_date"], data["selected_shift_type"])
        await state.update_data(calendar_year=date.year, calendar_month=date.month)
        header = (
            f"📅 Select a date\n"
            f"Current shift: <b>{datetime.now().strftime('%d %b %Y')} — {'🌞 Day' if data['selected_shift_type'] == 'day' else '🌙 Night'} shift</b>\n"
            f"Selected: <b>{data['selected_date'].strftime('%d %b %Y')}</b>"
        )
        await bot.edit_message_text(
            text=header,
            chat_id=data["chat_id"],
            message_id=data["message_id"],
            reply_markup=builder.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        logging.exception("Error in prev_month")
        await callback.answer("Произошла ошибка!", show_alert=True)

@router.callback_query(F.data.startswith("next_month"))
async def next_month(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        _, m, y = callback.data.split(":")
        date = datetime(int(y), int(m), 28) + timedelta(days=4)
        date = date.replace(day=1)
        data = await state.get_data()
        builder = build_calendar(date.year, date.month, data["selected_date"], data["selected_shift_type"])
        await state.update_data(calendar_year=date.year, calendar_month=date.month)
        header = (
            f"📅 Select a date\n"
            f"Current shift: <b>{datetime.now().strftime('%d %b %Y')} — {'🌞 Day' if data['selected_shift_type'] == 'day' else '🌙 Night'} shift</b>\n"
            f"Selected: <b>{data['selected_date'].strftime('%d %b %Y')}</b>"
        )
        await bot.edit_message_text(
            text=header,
            chat_id=data["chat_id"],
            message_id=data["message_id"],
            reply_markup=builder.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        logging.exception("Error in next_month")
        await callback.answer("Произошла ошибка!", show_alert=True)

@router.callback_query(F.data.startswith("day:"))
async def pick_day(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        _, d, m, y = callback.data.split(":")
        selected = datetime(int(y), int(m), int(d)).date()
        await push_state(state, ShiftNavigationState.SHIFT_TYPE)
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
    except Exception:
        logging.exception("Error in pick_day")
        await callback.answer("Произошла ошибка!", show_alert=True)

@router.callback_query(F.data.startswith("shift_type:"))
async def select_shift_type(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        _, shift_type = callback.data.split(":")
        await push_state(state, ShiftNavigationState.VIEWING_SHIFT)
        await state.set_state(ShiftNavigationState.VIEWING_SHIFT)
        await state.update_data(selected_shift_type=shift_type, is_current_shift=False)
        await render_shift_dashboard(callback, state, bot)
    except Exception:
        logging.exception("Error in select_shift_type")
        await callback.answer("Произошла ошибка!", show_alert=True)

@router.callback_query(F.data == "calendar_back")
async def back_to_dashboard(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        # Не пушим состояние при возврате назад!
        await state.set_state(ShiftNavigationState.VIEWING_SHIFT)
        await render_shift_dashboard(callback, state, bot)
    except Exception:
        logging.exception("Error in back_to_dashboard")
        await callback.answer("Произошла ошибка!", show_alert=True)

@router.callback_query(F.data == "select_rotation")
async def select_rotation(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        await push_state(state, ShiftNavigationState.VIEW_ROTATION)
        await state.set_state(ShiftNavigationState.VIEW_ROTATION)
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back", callback_data="return_shift")
        kb.adjust(1)
        await callback.message.edit_text(
            text="<b>🔁 Rotation by floor</b>\n\nЗаглушка: здесь будет ротация по этажам.",
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        logging.exception("Error in select_rotation")
        await callback.answer("Произошла ошибка!", show_alert=True)

# --- Fallback обработчик ---
@router.callback_query()
async def fallback_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    logging.warning(f"Unhandled callback: {callback.data}")
    await callback.answer("Функция в разработке или недоступна", show_alert=True)