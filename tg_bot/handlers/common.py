# tg_bot/handlers/common.py

from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.viewing_shift import render_shift_dashboard
from utils.utils import get_current_shift_and_date
from core.config import TIMEZONE
from datetime import datetime
from zoneinfo import ZoneInfo

router = Router()

# Временная заглушка — потом заменить на БД или middleware
FAKE_USER_ROLES = {
    385561891: "manager",  # сюда твой ID
    111111111: "dealer",
}

try:
    timezone = ZoneInfo(TIMEZONE)
except Exception as e:
    raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

@router.message(F.text == "/start")
async def start_handler(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    role = FAKE_USER_ROLES.get(user_id)

    if not role:
        await message.answer("❌ Access denied. Please contact your manager.")
        return

    now = datetime.now(timezone)
    current_shift_type, current_date = get_current_shift_and_date(now)

    await state.set_state(ShiftNavigationState.VIEWING_SHIFT)
    await state.update_data(
        role=role,
        selected_date=current_date,
        selected_shift_type=current_shift_type,
        is_current_shift=True
    )

    await render_shift_dashboard(message, state, bot)

