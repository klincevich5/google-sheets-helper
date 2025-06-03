# handlers/common.py

from aiogram import Router
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command
from datetime import datetime
from zoneinfo import ZoneInfo
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.services.db import get_user_role
from tg_bot.utils.utils import get_current_shift_and_date
from core.config import TIMEZONE

router = Router()

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.update_data(state_stack=[], current_state=None)

    role = await get_user_role(message.from_user.id)
    now = datetime.now(ZoneInfo(TIMEZONE))
    shift_type, shift_date = get_current_shift_and_date(now)

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
