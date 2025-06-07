# handlers/viewing_shift.py

from aiogram import Router, Bot, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from core.config import TIMEZONE
from datetime import datetime
from zoneinfo import ZoneInfo
from tg_bot.utils.utils import get_current_shift_and_date
from tg_bot.keyboards.main_menu import (
    get_service_manager_keyboard,
    get_dealer_keyboard,
    get_architect_keyboard,
)
from tg_bot.utils.formatting import (
    get_sm_main_view,
    get_dealer_main_view,
    get_architect_main_view,
)
from tg_bot.services.db import get_user_role

from tg_bot.handlers.architect.tasks import view_tasks
from tg_bot.handlers.service_manager.reports import select_report
from tg_bot.handlers.service_manager.dealers_list import view_dealers_list
from tg_bot.handlers.service_manager.feedback import view_feedbacks, view_mistakes
from tg_bot.handlers.service_manager.rotations import select_rotation
from tg_bot.handlers.common_callbacks import contact_info
from tg_bot.handlers.dealer.feedback import view_my_feedback
from tg_bot.handlers.dealer.mistakes import view_my_mistakes

router = Router()

# --- Кнопки ---
@router.callback_query(F.data == "select_shift", ShiftNavigationState.VIEWING_SHIFT)
async def select_shift_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    current_state = await state.get_state()
    if current_state == ShiftNavigationState.CALENDAR.state:
        await callback.answer("Календарь уже открыт.", show_alert=True)
        return

    from tg_bot.handlers.calendar_navigation import open_calendar
    await open_calendar(callback, state, bot)

@router.callback_query(F.data == "select_current_shift", ShiftNavigationState.VIEWING_SHIFT)
async def select_current_shift(callback: CallbackQuery, state: FSMContext, bot: Bot):
    now = datetime.now(ZoneInfo(TIMEZONE))
    current_shift_type, current_shift_date = get_current_shift_and_date(now)

    data = await state.get_data()
    selected_date = data.get("selected_date")
    selected_type = data.get("selected_shift_type")

    if selected_date == current_shift_date and selected_type == current_shift_type:
        await callback.answer("Уже выбрана актуальная смена", show_alert=True)
        return

    await state.update_data(
        selected_date=current_shift_date,
        selected_shift_type=current_shift_type,
        is_current_shift=True
    )
    await render_shift_dashboard(callback, state, bot)

@router.callback_query(F.data == "contact_info", ShiftNavigationState.VIEWING_SHIFT)
async def contact_info_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await contact_info(callback, state, bot)

# Dealer
@router.callback_query(F.data == "view_my_feedback", ShiftNavigationState.VIEWING_SHIFT)
async def view_my_feedback_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await view_my_feedback(callback, state, bot)

@router.callback_query(F.data == "view_my_mistakes", ShiftNavigationState.VIEWING_SHIFT)
async def view_my_mistakes_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await view_my_mistakes(callback, state, bot)

# SM & Architect
@router.callback_query(F.data == "select_report", ShiftNavigationState.VIEWING_SHIFT)
async def select_report_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await select_report(callback, state, bot)

@router.callback_query(F.data == "view_dealers_list", ShiftNavigationState.VIEWING_SHIFT)
async def view_dealers_list_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await view_dealers_list(callback, state, bot)

@router.callback_query(F.data == "view_shift_feedbacks", ShiftNavigationState.VIEWING_SHIFT)
async def view_shift_feedbacks_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await view_feedbacks(callback, state, bot)

@router.callback_query(F.data == "view_shift_mistakes", ShiftNavigationState.VIEWING_SHIFT)
async def view_shift_mistakes_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await view_mistakes(callback, state, bot)

@router.callback_query(F.data == "select_rotation", ShiftNavigationState.VIEWING_SHIFT)
async def select_rotation_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await select_rotation(callback, state, bot)

# Architect
@router.callback_query(F.data == "view_tasks", ShiftNavigationState.VIEWING_SHIFT)
async def view_tasks_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await view_tasks(callback, state, bot)

# --- Отображение главной панели (универсально) ---
async def render_shift_dashboard(msg_or_cb: Message | CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    user_id = data.get("user_id") or msg_or_cb.from_user.id
    chat_id = msg_or_cb.chat.id if isinstance(msg_or_cb, Message) else msg_or_cb.message.chat.id

    # ⚠️ Подстраховка, если нет даты или типа смены
    selected_date = data.get("selected_date")
    selected_shift_type = data.get("selected_shift_type")

    if selected_date is None or selected_shift_type is None:
        now = datetime.now()
        current_shift_type, current_date = get_current_shift_and_date(now)
        selected_date = selected_date or current_date
        selected_shift_type = selected_shift_type or current_shift_type
        await state.update_data(
            selected_date=selected_date,
            selected_shift_type=selected_shift_type
        )

    role = await get_user_role(user_id)

    if role == "dealer":
        text = await get_dealer_main_view(user_id, selected_date, selected_shift_type)
        keyboard = get_dealer_keyboard()
    elif role == "service_manager":
        text = await get_sm_main_view(user_id, selected_date, selected_shift_type)
        keyboard = get_service_manager_keyboard()
    elif role == "architect":
        text = await get_architect_main_view(user_id, selected_date, selected_shift_type)
        keyboard = get_architect_keyboard()
    else:
        text = "❌ Ваша роль не определена."
        keyboard = None

    try:
        if isinstance(msg_or_cb, CallbackQuery):
            await msg_or_cb.answer()

            current_text = msg_or_cb.message.text or msg_or_cb.message.caption

            def markup_equal(m1, m2):
                if m1 is None and m2 is None:
                    return True
                if m1 is None or m2 is None:
                    return False
                def buttons_to_dicts(markup):
                    return [[b.__dict__ for b in row] for row in markup.inline_keyboard]
                return buttons_to_dicts(m1) == buttons_to_dicts(m2)

            if current_text == text and markup_equal(msg_or_cb.message.reply_markup, keyboard):
                await msg_or_cb.answer("Вы уже находитесь на этой странице", show_alert=True)
                return

            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_or_cb.message.message_id,
                    text=text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
                await state.update_data(message_id=msg_or_cb.message.message_id, chat_id=chat_id)
            except Exception as e:
                print(f"[render_shift_dashboard] edit failed: {e}")
                sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard, parse_mode="HTML")
                await state.update_data(message_id=sent.message_id, chat_id=sent.chat.id)
        else:
            sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard, parse_mode="HTML")
            await state.update_data(message_id=sent.message_id, chat_id=sent.chat.id)

    except Exception as e:
        print(f"[render_shift_dashboard] unexpected error: {e}")
