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
    get_main_menu_keyboard_by_role,
)
from tg_bot.utils.formatting import (
    get_sm_main_view,
    get_dealer_main_view,
    get_architect_main_view,
)
from tg_bot.services.db import get_user_role

from tg_bot.handlers.architect.tasks import select_tasks
from tg_bot.handlers.manager.reports import select_report
from tg_bot.handlers.manager.dealers_list import view_dealers_list
from tg_bot.handlers.manager.feedback import view_feedbacks, view_mistakes
from tg_bot.handlers.manager.rotations import select_rotation
from tg_bot.handlers.common_callbacks import contact_info, check_stranger_callback
from tg_bot.handlers.dealer.feedback import view_my_feedback
from tg_bot.handlers.dealer.mistakes import view_my_mistakes
from core.timezone import now

router = Router()

# Временно возвращаем render_shift_dashboard в viewing_shift.py как универсальный роутер, который делегирует вызов нужного render_*_dashboard в зависимости от роли пользователя.
from tg_bot.handlers.dealer.main import render_dealer_dashboard
from tg_bot.handlers.manager.main import render_manager_dashboard
from tg_bot.handlers.architect.main import render_architect_dashboard
from tg_bot.services.db import get_user_role

async def render_shift_dashboard(msg_or_cb, state, bot):
    data = await state.get_data()
    user_id = data.get("user_id") or msg_or_cb.from_user.id
    role = await get_user_role(user_id)
    if role.lower() in ("dealer", "shuffler"):
        await render_dealer_dashboard(msg_or_cb, state, bot)
    elif role.lower() == "architect":
        await render_architect_dashboard(msg_or_cb, state, bot)
    elif role.lower() in ("manager", "qa_manager", "hr_manager", "chief_sm_manager", "trainer_manager", "floor_manager", "admin"):
        await render_manager_dashboard(msg_or_cb, state, bot)
    else:
        # fallback для stranger и неизвестных ролей
        if hasattr(msg_or_cb, "answer"):
            await msg_or_cb.answer("⛔️ Access not granted.", show_alert=True)

# --- Buttons ---
@router.callback_query(F.data == "select_shift", ShiftNavigationState.VIEWING_SHIFT)
async def select_shift_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return

    current_state = await state.get_state()
    if current_state == ShiftNavigationState.CALENDAR.state:
        await callback.answer("Calendar is already open.", show_alert=True)
        return

    from tg_bot.handlers.calendar_navigation import open_calendar
    await open_calendar(callback, state, bot)

@router.callback_query(F.data == "select_current_shift", ShiftNavigationState.VIEWING_SHIFT)
async def select_current_shift(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return

    current_time = now()
    current_shift_type, current_shift_date = get_current_shift_and_date(current_time)

    data = await state.get_data()
    selected_date = data.get("selected_date")
    selected_type = data.get("selected_shift_type")

    if selected_date == current_shift_date and selected_type == current_shift_type:
        await callback.answer("Current shift is already selected", show_alert=True)
        return

    await state.update_data(
        selected_date=current_shift_date,
        selected_shift_type=current_shift_type,
        is_current_shift=True
    )
    await render_shift_dashboard(callback, state, bot)

@router.callback_query(F.data == "contact_info", ShiftNavigationState.VIEWING_SHIFT)
async def contact_info_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return

    await contact_info(callback, state, bot)

# Dealer
@router.callback_query(F.data == "view_my_feedback", ShiftNavigationState.VIEWING_SHIFT)
async def view_my_feedback_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return

    await view_my_feedback(callback, state, bot)

@router.callback_query(F.data == "view_my_mistakes", ShiftNavigationState.VIEWING_SHIFT)
async def view_my_mistakes_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return

    await view_my_mistakes(callback, state, bot)

# SM & Architect
@router.callback_query(F.data == "select_report", ShiftNavigationState.VIEWING_SHIFT)
async def select_report_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return

    await select_report(callback, state, bot)

@router.callback_query(F.data == "view_dealers_list", ShiftNavigationState.VIEWING_SHIFT)
async def view_dealers_list_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return

    await view_dealers_list(callback, state, bot)

@router.callback_query(F.data == "view_shift_feedbacks", ShiftNavigationState.VIEWING_SHIFT)
async def view_shift_feedbacks_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return

    await view_feedbacks(callback, state, bot)

@router.callback_query(F.data == "view_shift_mistakes", ShiftNavigationState.VIEWING_SHIFT)
async def view_shift_mistakes_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return

    await view_mistakes(callback, state, bot)

@router.callback_query(F.data == "select_rotation", ShiftNavigationState.VIEWING_SHIFT)
async def select_rotation_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return

    await select_rotation(callback, state, bot)

# Architect
@router.callback_query(F.data == "view_tasks", ShiftNavigationState.VIEWING_SHIFT)
async def view_tasks_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return

    await select_tasks(callback, state, bot)
