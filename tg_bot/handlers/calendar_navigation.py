# handlers/calendar_navigation.py

from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.utils.utils import get_current_shift_and_date
from core.config import TIMEZONE
from datetime import datetime, timedelta
import calendar
from zoneinfo import ZoneInfo
from tg_bot.handlers.common_callbacks import push_state

router = Router()
timezone = ZoneInfo(TIMEZONE)

# --- Утилита для безопасного редактирования/отправки сообщений ---
async def safe_edit_or_send(callback: CallbackQuery, bot: Bot, text: str, markup, state: FSMContext):
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id,
            reply_markup=markup,
            parse_mode="HTML"
        )
        await state.update_data(chat_id=callback.message.chat.id, message_id=callback.message.message_id)
    except Exception as e:
        print(f"[safe_edit_or_send] edit failed: {e}")
        sent: Message = await bot.send_message(
            chat_id=callback.from_user.id,
            text=text,
            reply_markup=markup,
            parse_mode="HTML"
        )
        await state.update_data(chat_id=sent.chat.id, message_id=sent.message_id)

# --- Календарь ---
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

# --- Handlers ---

@router.callback_query(F.data == "select_shift")
async def open_calendar(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.answer()
    try:
        now = datetime.now()
        data = await state.get_data()
        selected_date = data.get("selected_date")
        selected_shift_type = data.get("selected_shift_type")

        builder = build_calendar(now.year, now.month, selected_date, selected_shift_type)
        await push_state(state, ShiftNavigationState.CALENDAR)
        await state.set_state(ShiftNavigationState.CALENDAR)
        await state.update_data(calendar_year=now.year, calendar_month=now.month)

        current_shift_type, current_date = get_current_shift_and_date(datetime.now(timezone))

        # Формируем текст с защитой от None
        selected_text = (
            f"{selected_date.strftime('%d %b %Y')}" if selected_date else "not selected"
        )

        header = (
            "📅 Select a date\n"
            f"Current shift: <b>{current_date.strftime('%d %b %Y')} — "
            f"{'🌞 Day' if current_shift_type == 'day' else '🌙 Night'} shift</b>\n"
            f"Selected: <b>{selected_text}</b>"
        )

        await safe_edit_or_send(callback, bot, header, builder.as_markup(), state)

    except Exception as e:
        print(f"[calendar_navigation] open_calendar exception: {e}")
        await callback.answer("Произошла ошибка!", show_alert=True)


@router.callback_query(F.data.startswith("prev_month"))
async def prev_month(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        _, m, y = callback.data.split(":")
        date = datetime(int(y), int(m), 1) - timedelta(days=1)
        data = await state.get_data()
        builder = build_calendar(date.year, date.month, data["selected_date"], data["selected_shift_type"])
        await state.update_data(calendar_year=date.year, calendar_month=date.month)

        current_shift_type, current_date = get_current_shift_and_date(datetime.now(timezone))
        header = (
            "📅 Select a date\n"
            f"Current shift: <b>{current_date.strftime('%d %b %Y')} — "
            f"{'🌞 Day' if current_shift_type == 'day' else '🌙 Night'} shift</b>\n"
            f"Selected: <b>{data['selected_date'].strftime('%d %b %Y')}</b>"
        )

        await safe_edit_or_send(callback, bot, header, builder.as_markup(), state)

    except Exception as e:
        print(f"[calendar_navigation] prev_month failed: {e}")
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

        current_shift_type, current_date = get_current_shift_and_date(datetime.now(timezone))
        header = (
            "📅 Select a date\n"
            f"Current shift: <b>{current_date.strftime('%d %b %Y')} — "
            f"{'🌞 Day' if current_shift_type == 'day' else '🌙 Night'} shift</b>\n"
            f"Selected: <b>{data['selected_date'].strftime('%d %b %Y')}</b>"
        )

        await safe_edit_or_send(callback, bot, header, builder.as_markup(), state)

    except Exception as e:
        print(f"[calendar_navigation] next_month failed: {e}")
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

        await safe_edit_or_send(
            callback,
            bot,
            text=f"Selected: <b>{selected.strftime('%d %b %Y')}</b>\nSelect shift type:",
            markup=builder.as_markup(),
            state=state
        )
    except Exception as e:
        print(f"[calendar_navigation] pick_day failed: {e}")
        await callback.answer("Произошла ошибка!", show_alert=True)

@router.callback_query(F.data.startswith("shift_type:"))
async def select_shift_type(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        _, shift_type = callback.data.split(":")
        from tg_bot.handlers.viewing_shift import render_shift_dashboard
        await push_state(state, ShiftNavigationState.VIEWING_SHIFT)
        await state.set_state(ShiftNavigationState.VIEWING_SHIFT)
        await state.update_data(selected_shift_type=shift_type, is_current_shift=False)
        await render_shift_dashboard(callback, state, bot)
    except Exception as e:
        print(f"[calendar_navigation] shift_type failed: {e}")
        await callback.answer("Произошла ошибка!", show_alert=True)

@router.callback_query(F.data == "calendar_back")
async def back_to_dashboard(callback: CallbackQuery, state: FSMContext, bot: Bot):
    from tg_bot.handlers.viewing_shift import render_shift_dashboard
    await state.update_data(state_stack=[], current_state=None)
    await state.set_state(ShiftNavigationState.VIEWING_SHIFT)
    await render_shift_dashboard(callback, state, bot)

@router.callback_query(ShiftNavigationState.CALENDAR)
async def fallback_calendar_handler(callback: CallbackQuery, state: FSMContext, bot: Bot):
    print(f"[calendar_navigation] unhandled callback in CALENDAR: {callback.data}")
    await callback.answer("Выберите дату и смену, прежде чем продолжить.", show_alert=True)
