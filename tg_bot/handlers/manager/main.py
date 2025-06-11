from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.keyboards.manager import get_manager_keyboard
from tg_bot.formatting.manager import get_manager_main_view
from tg_bot.services.db import get_user_role
from tg_bot.keyboards.main_menu import get_main_menu_keyboard_by_role
from tg_bot.handlers.common_callbacks import check_stranger_callback, contact_info
from core.timezone import now
from tg_bot.utils.utils import get_current_shift_and_date

# Подключаем все нужные роутеры
from tg_bot.handlers.manager.feedback import router as feedback_router
from tg_bot.handlers.manager.reports import router as reports_router
from tg_bot.handlers.manager.dealers_list import router as dealers_list_router
from tg_bot.handlers.manager.rotations import router as rotations_router
from tg_bot.handlers.manager.team import router as team_router

router = Router()
router.include_router(feedback_router)
router.include_router(reports_router)
router.include_router(dealers_list_router)
router.include_router(rotations_router)
router.include_router(team_router)

from tg_bot.handlers.manager.reports import select_report
from tg_bot.handlers.manager.dealers_list import view_dealers_list
from tg_bot.handlers.manager.feedback import view_feedbacks, view_mistakes
from tg_bot.handlers.manager.rotations import select_rotation

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
        await callback.answer("Текущая смена уже выбрана", show_alert=True)
        return
    await state.update_data(
        selected_date=current_shift_date,
        selected_shift_type=current_shift_type,
        is_current_shift=True
    )
    await render_manager_dashboard(callback, state, bot)

@router.callback_query(F.data == "contact_info", ShiftNavigationState.VIEWING_SHIFT)
async def contact_info_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if await check_stranger_callback(callback): return
    await contact_info(callback, state, bot)

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

async def render_manager_dashboard(msg_or_cb: Message | CallbackQuery, state: FSMContext, bot: Bot):
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
        text = "⛔️ Доступ запрещен.\nПожалуйста, свяжитесь с вашим менеджером для получения доступа."
        keyboard = None
    else:
        from tg_bot.services.db import get_or_create_user
        user = await get_or_create_user(user_id)
        text = get_manager_main_view(user, selected_date, selected_shift_type)
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
