# tg_bot/handlers/viewing_shift.py

from aiogram import Router, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
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
from services.db import get_user_role

router = Router()


@router.message(ShiftNavigationState.VIEWING_SHIFT)
@router.callback_query(ShiftNavigationState.VIEWING_SHIFT)
async def view_shift(msg_or_cb: Message | CallbackQuery, state: FSMContext, bot: Bot):
    await render_shift_dashboard(msg_or_cb, state, bot)


async def render_shift_dashboard(msg_or_cb: Message | CallbackQuery, state: FSMContext, bot: Bot):
    from_user = msg_or_cb.from_user
    role = await get_user_role(from_user.id)

    data = await state.get_data()
    selected_date = data.get("selected_date")
    selected_shift_type = data.get("selected_shift_type")

    # Получение текста и клавиатуры по роли
    if role == "dealer":
        text = await get_dealer_main_view(from_user.id, selected_date, selected_shift_type)
        keyboard = get_dealer_keyboard()
    elif role == "service_manager":
        text = await get_sm_main_view(from_user.id, selected_date, selected_shift_type)
        keyboard = get_service_manager_keyboard()
    elif role == "architect":
        text = await get_architect_main_view(from_user.id, selected_date, selected_shift_type)
        keyboard = get_architect_keyboard()
    else:
        text = "❌ Ваша роль не определена."
        keyboard = None

    # Ответ — в зависимости от типа события
    if isinstance(msg_or_cb, CallbackQuery):
        await bot.edit_message_text(
            chat_id=msg_or_cb.message.chat.id,
            message_id=msg_or_cb.message.message_id,
            text=text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    else:
        await msg_or_cb.answer(text, reply_markup=keyboard, parse_mode="HTML")
