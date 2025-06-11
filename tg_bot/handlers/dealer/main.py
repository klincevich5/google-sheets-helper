from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.keyboards.dealer import get_dealer_keyboard
from tg_bot.formatting.dealer import get_dealer_main_view
from tg_bot.services.db import get_user_role
from tg_bot.handlers.dealer.feedback import router as feedback_router, view_my_feedback
from tg_bot.handlers.dealer.mistakes import router as mistakes_router, view_my_mistakes
from tg_bot.handlers.common_callbacks import check_stranger_callback, contact_info
from core.timezone import now
from tg_bot.utils.utils import get_current_shift_and_date
from tg_bot.keyboards.main_menu import get_main_menu_keyboard_by_role

router = Router()
router.include_router(feedback_router)
router.include_router(mistakes_router)

@router.callback_query(F.data == "select_shift", ShiftNavigationState.VIEWING_SHIFT)
async def select_shift_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
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
    await render_dealer_dashboard(callback, state, bot)

@router.callback_query(F.data == "view_my_feedback", ShiftNavigationState.VIEWING_SHIFT)
async def view_my_feedback_proxy(callback: CallbackQuery, state: FSMContext, bot: Bot):
    # Заглушка для совместимости с проксирующими вызовами (ожидается 3 аргумента)
    from tg_bot.handlers.dealer.feedback import view_my_feedback as real_view_my_feedback
    await real_view_my_feedback(callback, state)

@router.callback_query(F.data == "view_my_mistakes", ShiftNavigationState.VIEWING_SHIFT)
async def view_my_mistakes_proxy(callback: CallbackQuery, state: FSMContext, bot: Bot):
    # Заглушка для совместимости с проксирующими вызовами (ожидается 3 аргумента)
    from tg_bot.handlers.dealer.mistakes import view_my_mistakes as real_view_my_mistakes
    await real_view_my_mistakes(callback, state)

@router.callback_query(F.data == "contact_info", ShiftNavigationState.VIEWING_SHIFT)
async def contact_info_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
    await contact_info(callback, state, bot)

async def render_dealer_dashboard(msg_or_cb: Message | CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    user_id = data.get("user_id") or msg_or_cb.from_user.id
    chat_id = msg_or_cb.chat.id if isinstance(msg_or_cb, Message) else msg_or_cb.message.chat.id
    selected_date = data.get("selected_date")
    selected_shift_type = data.get("selected_shift_type")
    if selected_date is None or selected_shift_type is None:
        current_time = now()
        current_shift_type, current_date = get_current_shift_and_date(current_time)
        selected_date = selected_date or current_date
        selected_shift_type = selected_shift_type or current_shift_type
        await state.update_data(
            selected_date=selected_date,
            selected_shift_type=selected_shift_type
        )
    role = await get_user_role(user_id)
    if role.lower() == "stranger":
        text = "⛔️ Access not granted.\nPlease contact your manager to get access."
        keyboard = None
    else:
        from tg_bot.services.db import get_or_create_user
        user = await get_or_create_user(user_id)
        text = get_dealer_main_view(user, selected_date, selected_shift_type)
        keyboard = get_main_menu_keyboard_by_role(role)
    if isinstance(msg_or_cb, CallbackQuery):
        await msg_or_cb.answer()
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_or_cb.message.message_id,
            text=text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await state.update_data(message_id=msg_or_cb.message.message_id, chat_id=chat_id)
    else:
        sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard, parse_mode="HTML")
        await state.update_data(message_id=sent.message_id, chat_id=sent.chat.id)

# В каждом main.py для роли (dealer, manager, architect) реализовать свой render_*_dashboard, который использует get_main_menu_keyboard_by_role(role) для показа только нужных кнопок.
