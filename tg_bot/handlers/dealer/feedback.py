from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.common_callbacks import push_state

router = Router()

@router.callback_query(F.data == "view_my_feedback")
async def view_my_feedback(callback: CallbackQuery, state: FSMContext):
    print("[dealer/feedback] view_my_feedback")
    try:
        await push_state(state, ShiftNavigationState.VIEW_DEALER_FEEDBACKS)
        await state.set_state(ShiftNavigationState.VIEW_DEALER_FEEDBACKS)

        text = (
            "<b>💬 Your feedbacks</b>\n"
            "• «Great attitude!»\n"
            "• «Fast dealing, good interaction.»"
        )

        kb = InlineKeyboardBuilder()
        # Кнопка возврата к дашборду
        kb.button(text="🔙 Back", callback_data="return_shift")
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
