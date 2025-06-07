from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.common_callbacks import push_state

router = Router()

@router.callback_query(F.data == "select_report")
async def select_report(callback: CallbackQuery, state: FSMContext, bot):
    print("[service_manager/reports] select_report")
    try:
        await push_state(state, ShiftNavigationState.SELECT_REPORT)
        await state.set_state(ShiftNavigationState.SELECT_REPORT)

        kb = InlineKeyboardBuilder()
        kb.button(text="🏛 VIP/GENERIC", callback_data="report:vip_generic")
        kb.button(text="🎰 GSBJ", callback_data="report:gsbj")
        kb.button(text="🔥 LEGENDZ", callback_data="report:legendz")
        # Кнопка возврата на дашборд
        kb.button(text="🔙 Back", callback_data="return_shift")
        kb.adjust(1)

        await callback.message.edit_text(
            text="📋 Select studio to view report:",
            reply_markup=kb.as_markup()
        )
    except Exception:
        await callback.answer("Произошла ошибка!", show_alert=True)


@router.callback_query(F.data.startswith("report:"))
async def view_report(callback: CallbackQuery, state: FSMContext, bot):
    print("[service_manager/reports] view_report")
    try:
        await push_state(state, ShiftNavigationState.VIEW_REPORT)
        await state.set_state(ShiftNavigationState.VIEW_REPORT)

        _, studio_key = callback.data.split(":")
        report_text = await get_studio_report_text(studio_key)

        kb = InlineKeyboardBuilder()
        # Кнопка возврата к выбору студии
        kb.button(text="🔙 Back to studios", callback_data="select_report")
        kb.adjust(1)

        await callback.message.edit_text(
            text=report_text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception:
        await callback.answer("Произошла ошибка!", show_alert=True)


@router.callback_query(F.data == "return_shift")
async def proxy_return_shift(callback: CallbackQuery, state: FSMContext, bot):
    from tg_bot.handlers.common_callbacks import return_to_dashboard
    await return_to_dashboard(callback, state, bot)


async def get_studio_report_text(studio_key: str) -> str:
    studio_names = {
        "vip_generic": "🏛 VIP/GENERIC",
        "gsbj": "🎰 GSBJ",
        "legendz": "🔥 LEGENDZ"
    }

    studio_name = studio_names.get(studio_key, "❓ Unknown Studio")

    return (
        f"<b>{studio_name} Report</b>\n\n"
        f"• Dealers: 12\n"
        f"• Active tables: 6\n"
        f"• Issues reported: 1\n"
        f"• Feedbacks: 4"
    )
