from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.common_callbacks import push_state

router = Router()

@router.callback_query(F.data == "view_shift_feedbacks")
async def view_feedbacks(callback: CallbackQuery, state: FSMContext):
    try:
        await push_state(state, ShiftNavigationState.VIEW_SHIFT_FEEDBACKS)
        await state.set_state(ShiftNavigationState.VIEW_SHIFT_FEEDBACKS)

        data = await state.get_data()
        date = data.get("selected_date")
        shift = data.get("selected_shift_type")

        # Заглушка: список фидбеков
        feedbacks = [
            "• @dealer_anna — «Great job!»",
            "• @dealer_ivan — «Slow dealing in round 5.»",
            "• @dealer_pavel — «Excellent speed.»"
        ]

        text = (
            f"<b>💬 Feedbacks for {date.strftime('%d %b %Y')} ({shift})</b>\n\n"
            + "\n".join(feedbacks)
        )

        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back to dashboard", callback_data="return_shift")
        kb.adjust(1)

        await callback.message.edit_text(
            text=text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        await callback.answer("Произошла ошибка!", show_alert=True)


@router.callback_query(F.data == "view_shift_mistakes")
async def view_mistakes(callback: CallbackQuery, state: FSMContext):
    try:
        await push_state(state, ShiftNavigationState.VIEW_SHIFT_MISTAKES)
        await state.set_state(ShiftNavigationState.VIEW_SHIFT_MISTAKES)

        data = await state.get_data()
        date = data.get("selected_date")
        shift = data.get("selected_shift_type")

        # Заглушка: список ошибок
        mistakes = [
            "• @dealer_anna — «Missed payout on table 4»",
            "• @dealer_ivan — «Incorrect dealing in round 2»"
        ]

        text = (
            f"<b>⚠️ Mistakes for {date.strftime('%d %b %Y')} ({shift})</b>\n\n"
            + "\n".join(mistakes)
        )

        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back to dashboard", callback_data="return_shift")
        kb.adjust(1)

        await callback.message.edit_text(
            text=text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        await callback.answer("Произошла ошибка!", show_alert=True)
