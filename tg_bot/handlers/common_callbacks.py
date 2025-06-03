# handlers/common_callbacks.py

from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState

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
    print("[common_callbacks] return_to_dashboard: resetting to dashboard")

    await state.update_data(state_stack=[], current_state=None)
    await state.set_state(ShiftNavigationState.VIEWING_SHIFT)

    from tg_bot.handlers.viewing_shift import render_shift_dashboard
    await render_shift_dashboard(callback, state, bot)

@router.callback_query(F.data == "contact_info")
async def contact_info(callback: CallbackQuery, state: FSMContext, bot: Bot):
    print("[common_callbacks] contact_info")
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

        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back", callback_data="return_shift")
        kb.adjust(1)

        await push_state(state, ShiftNavigationState.VIEWING_SHIFT)

        try:
            await callback.message.edit_text(
                text=text,
                reply_markup=kb.as_markup(),
                parse_mode="HTML"
            )
            await state.update_data(message_id=callback.message.message_id, chat_id=callback.message.chat.id)
        except Exception as e:
            print(f"[contact_info] edit failed: {e}")
            sent = await bot.send_message(
                chat_id=callback.from_user.id,
                text=text,
                reply_markup=kb.as_markup(),
                parse_mode="HTML"
            )
            await state.update_data(message_id=sent.message_id, chat_id=sent.chat.id)

        await callback.answer()

    except Exception as e:
        print(f"[contact_info] general failure: {e}")
        await callback.answer("Произошла ошибка!", show_alert=True)


# --- Универсальный fallback для неизвестных callback_data ---
@router.callback_query()
async def fallback_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    import logging
    print(f"[common_callbacks] fallback_callback: {callback.data}")
    logging.warning(f"Unhandled callback: {callback.data}")
    await callback.answer("Функция в разработке или недоступна", show_alert=True)
