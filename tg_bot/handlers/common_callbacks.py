from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.viewing_shift import render_shift_dashboard
import logging

router = Router()

# --- Стек состояний ---
async def push_state(state: FSMContext, new_state):
    data = await state.get_data()
    stack = data.get("state_stack", [])
    current = data.get("current_state")
    if current:
        stack.append(current)
    await state.update_data(state_stack=stack, current_state=new_state)

async def pop_state(state: FSMContext):
    data = await state.get_data()
    stack = data.get("state_stack", [])
    if stack:
        prev = stack.pop()
        await state.update_data(state_stack=stack, current_state=prev)
        return prev
    return ShiftNavigationState.VIEWING_SHIFT

@router.callback_query(F.data == "return_shift")
async def return_to_dashboard(callback: CallbackQuery, state: FSMContext, bot: Bot):
    prev_state = await pop_state(state)
    await state.set_state(prev_state)
    await render_shift_dashboard(callback, state, bot)

@router.callback_query(F.data == "contact_info")
async def contact_info(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        data = await state.get_data()
        role = data.get("user_role", "dealer")
        if role == "dealer":
            text = "<b>📞 Contacts for Dealer</b>\n• SM: @sm_manager\n• Tech: @tech_support"
        elif role == "service_manager":
            text = "<b>📞 Contacts for Service Manager</b>\n• HR: @hr_manager\n• Tech: @tech_support"
        elif role == "architect":
            text = "<b>📞 Contacts for Architect</b>\n• CTO: @cto\n• Tech: @tech_support"
        else:
            text = "No contacts available."
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back", callback_data="return_shift")
        kb.adjust(1)
        await push_state(state, ShiftNavigationState.VIEWING_SHIFT)
        await callback.message.edit_text(
            text=text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        logging.exception("Error in contact_info handler")
        await callback.answer("Произошла ошибка!", show_alert=True)

@router.callback_query(F.data == "refresh_dashboard")
async def refresh_dashboard(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        await render_shift_dashboard(callback, state, bot)
    except Exception:
        logging.exception("Error in refresh_dashboard handler")
        await callback.answer("Произошла ошибка!", show_alert=True)

# --- Архитектор: заглушки для analytics и team ---
@router.callback_query(F.data == "view_analytics")
async def view_analytics(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back", callback_data="return_shift")
        kb.adjust(1)
        await push_state(state, ShiftNavigationState.VIEW_TASKS)
        await callback.message.edit_text(
            text="<b>📊 Analytics</b>\n\nЗаглушка: здесь будет аналитика.",
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        logging.exception("Error in view_analytics handler")
        await callback.answer("Произошла ошибка!", show_alert=True)

@router.callback_query(F.data == "view_team")
async def view_team(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back", callback_data="return_shift")
        kb.adjust(1)
        await push_state(state, ShiftNavigationState.VIEW_TASKS)
        await callback.message.edit_text(
            text="<b>🧑‍🤝‍🧑 Team</b>\n\nЗаглушка: здесь будет список команды.",
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        logging.exception("Error in view_team handler")
        await callback.answer("Произошла ошибка!", show_alert=True)

# --- Универсальный fallback для неизвестных callback_data ---
@router.callback_query()
async def fallback_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    logging.warning(f"Unhandled callback: {callback.data}")
    await callback.answer("Функция в разработке или недоступна", show_alert=True)
