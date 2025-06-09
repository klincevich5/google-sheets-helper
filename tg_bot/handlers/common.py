# handlers/common.py

from aiogram import Router
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command
from core.timezone import now
from zoneinfo import ZoneInfo
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.services.db import get_user_role, get_or_create_user
from tg_bot.utils.utils import get_current_shift_and_date
from core.config import TIMEZONE

router = Router()

async def ensure_user_and_role(message: Message):
    user = await get_or_create_user(message.from_user.id, dealer_name=message.from_user.full_name)
    role = user["role"].lower() if user.get("role") else "stranger"
    # Allow forwarded messages to pass even for stranger
    is_forwarded = bool(getattr(message, "forward_from", None) or getattr(message, "forward_sender_name", None))
    if role == "stranger" and not is_forwarded:
        await message.answer(
            "⛔️ Access not granted.\nPlease contact your manager to get access."
        )
        return None
    return role

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.update_data(state_stack=[], current_state=None)
    # User and role check
    role = await ensure_user_and_role(message)
    if not role:
        return

    current_time = now()
    shift_type, shift_date = get_current_shift_and_date(current_time)

    from tg_bot.handlers.common_callbacks import push_state
    await push_state(state, ShiftNavigationState.VIEWING_SHIFT)
    await state.set_state(ShiftNavigationState.VIEWING_SHIFT)

    await state.update_data(
        selected_date=shift_date,
        selected_shift_type=shift_type,
        is_current_shift=True,
        user_role=role,
        user_id=message.from_user.id,
        chat_id=message.chat.id,
        message_id=message.message_id
    )
    from tg_bot.handlers.viewing_shift import render_shift_dashboard
    await render_shift_dashboard(message, state, bot=message.bot)

@router.message()
async def any_message(message: Message, state: FSMContext):
    # User and role check
    role = await ensure_user_and_role(message)
    if not role:
        return

    # Other message logic
    if message.text == "/start":
        await state.update_data(state_stack=[], current_state=None)
        current_time = now()
        shift_type, shift_date = get_current_shift_and_date(current_time)
        from tg_bot.handlers.common_callbacks import push_state
        await push_state(state, ShiftNavigationState.VIEWING_SHIFT)
        await state.set_state(ShiftNavigationState.VIEWING_SHIFT)
        await state.update_data(
            selected_date=shift_date,
            selected_shift_type=shift_type,
            is_current_shift=True,
            user_role=role,
            user_id=message.from_user.id,
            chat_id=message.chat.id,
            message_id=message.message_id
        )
        from tg_bot.handlers.viewing_shift import render_shift_dashboard
        await render_shift_dashboard(message, state, bot=message.bot)
