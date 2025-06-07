# handlers/service_manager/team.py

from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.common_callbacks import push_state

router = Router()


# 🔹 Просмотр дилера
@router.callback_query(F.data.startswith("dealer:"))
async def view_dealer(callback: CallbackQuery, state: FSMContext):
    print("[service_manager/team] view_dealer")
    try:
        await push_state(state, ShiftNavigationState.VIEW_EMPLOYEE)
        await state.set_state(ShiftNavigationState.VIEW_EMPLOYEE)

        _, user_id = callback.data.split(":")
        await state.update_data(selected_dealer=user_id)

        text = f"<b>👤 Dealer: {user_id.capitalize()}</b>\n\nChoose what to view:"

        kb = InlineKeyboardBuilder()
        kb.button(text="💬 Feedbacks", callback_data="dealer_feedbacks")
        kb.button(text="⚠️ Mistakes", callback_data="dealer_mistakes")
        # Кнопка возврата к списку дилеров
        kb.button(text="🔙 Back", callback_data="view_dealers_list")
        kb.adjust(1)

        await callback.message.edit_text(
            text=text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        await callback.answer("Произошла ошибка!", show_alert=True)

# 🔹 Фидбеки по дилеру
@router.callback_query(F.data == "dealer_feedbacks")
async def view_dealer_feedbacks(callback: CallbackQuery, state: FSMContext):
    print("[service_manager/team] view_dealer_feedbacks")
    try:
        await push_state(state, ShiftNavigationState.VIEW_DEALER_FEEDBACKS)
        await state.set_state(ShiftNavigationState.VIEW_DEALER_FEEDBACKS)
        data = await state.get_data()
        user_id = data.get("selected_dealer")

        text = (
            f"<b>💬 Feedbacks for {user_id.capitalize()}</b>\n"
            "• «Excellent presence on camera.»\n"
            "• «Quick reactions and confident dealing.»"
        )

        kb = InlineKeyboardBuilder()
        # Кнопка возврата к карточке дилера
        kb.button(text="🔙 Back", callback_data=f"dealer:{user_id}")
        kb.adjust(1)

        await callback.message.edit_text(
            text=text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        await callback.answer("Произошла ошибка!", show_alert=True)

# 🔹 Ошибки по дилеру
@router.callback_query(F.data == "dealer_mistakes")
async def view_dealer_mistakes(callback: CallbackQuery, state: FSMContext):
    print("[service_manager/team] view_dealer_mistakes")
    try:
        await push_state(state, ShiftNavigationState.VIEW_DEALER_MISTAKES)
        await state.set_state(ShiftNavigationState.VIEW_DEALER_MISTAKES)
        data = await state.get_data()
        user_id = data.get("selected_dealer")

        text = (
            f"<b>⚠️ Mistakes for {user_id.capitalize()}</b>\n"
            "• «Missed payout on table 3.»\n"
            "• «Incorrect card placement in round 7.»"
        )

        kb = InlineKeyboardBuilder()
        # Кнопка возврата к карточке дилера
        kb.button(text="🔙 Back", callback_data=f"dealer:{user_id}")
        kb.adjust(1)

        await callback.message.edit_text(
            text=text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        await callback.answer("Произошла ошибка!", show_alert=True)

@router.callback_query(F.data == "return_shift")
async def proxy_return_shift(callback: CallbackQuery, state: FSMContext, bot):
    from tg_bot.handlers.common_callbacks import return_to_dashboard
    await return_to_dashboard(callback, state, bot)
