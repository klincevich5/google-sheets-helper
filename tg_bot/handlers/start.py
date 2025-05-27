# tg_bot/handlers/start.py

from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.viewing_shift import render_shift_dashboard
from utils.utils import get_current_shift_and_date

router = Router()

FAKE_EMPLOYEES = {
    385561891: "manager",
    111111111: "dealer",
}


@router.message(F.text == "/start")
async def start_handler(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    role = FAKE_EMPLOYEES.get(user_id)

    if not role:
        await message.answer("❌ Access denied.")
        return

    shift_type, shift_date = get_current_shift_and_date()

    dashboard_message = await message.answer("⌛ Loading shift dashboard...")

    await state.set_state(ShiftNavigationState.VIEWING_SHIFT)
    await state.update_data({
        "role": role,
        "selected_date": shift_date,
        "selected_shift_type": shift_type,
        "is_current_shift": True,
        "chat_id": dashboard_message.chat.id,
        "message_id": dashboard_message.message_id
    })

    await render_shift_dashboard(dashboard_message, state, bot)
