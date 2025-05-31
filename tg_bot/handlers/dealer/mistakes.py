from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.common_callbacks import push_state

router = Router()

@router.callback_query(F.data == "view_my_mistakes")
async def view_my_mistakes(callback: CallbackQuery, state: FSMContext):
    try:
        await push_state(state, ShiftNavigationState.VIEW_MY_MISTAKES)
        await state.set_state(ShiftNavigationState.VIEW_MY_MISTAKES)

        text = (
            "<b>⚠️ Your mistakes</b>\n"
            "• «Wrong payout on round 3»\n"
            "• «Delayed reaction on table 5»"
        )

        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back", callback_data="return_shift")
        kb.adjust(1)

        await callback.message.edit_text(
            text=text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        await callback.answer("Произошла ошибка!", show_alert=True)
