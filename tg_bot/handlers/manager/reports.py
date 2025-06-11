import asyncio
from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.common_callbacks import push_state, check_stranger_callback
from database.session import get_session
from tg_bot.utils.reports_creator import generate_studio_report_text
from tg_bot.utils.utils import get_current_shift_and_date
from datetime import datetime
from zoneinfo import ZoneInfo
from core.config import TIMEZONE
import asyncio

# Проверка корректности TIMEZONE
try:
    timezone = ZoneInfo(TIMEZONE)
except Exception as e:
    raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

router = Router()

@router.callback_query(F.data == "select_report")
async def select_report(callback: CallbackQuery, state: FSMContext, bot):
    if await check_stranger_callback(callback): return
    print("[service_manager/reports] select_report")
    try:
        await push_state(state, ShiftNavigationState.SELECT_REPORT)
        await state.set_state(ShiftNavigationState.SELECT_REPORT)

        kb = InlineKeyboardBuilder()
        kb.button(text="🏛 VIP/GENERIC", callback_data="report:vip_generic")
        kb.button(text="🎰 GSBJ", callback_data="report:gsbj")
        kb.button(text="🔥 LEGENDZ", callback_data="report:legendz")
        kb.button(text="🔙 Back", callback_data="return_shift")
        kb.adjust(1)

        # Получаем дату/смену из состояния или, если нет — устанавливаем актуальные
        data = await state.get_data()
        related_date = data.get("selected_date")
        related_shift = data.get("selected_shift_type")

        current_shift_type, current_date = get_current_shift_and_date(datetime.now(timezone))

        if not related_date or not related_shift:
            related_date = related_date or current_date
            related_shift = related_shift or current_shift_type
            await state.update_data(selected_date=related_date, selected_shift_type=related_shift)

        final_shift = related_shift.lower()
        text = (
            "📋 Select studio to view report:\n"
            f"<b>{related_date.strftime('%d %b %Y')} — "
            f"{'🌞 Day' if final_shift == 'day' else '🌙 Night'} shift</b>"
        )

        await callback.message.edit_text(
            text=text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )

    except Exception as e:
        print(f"Error in select_report: {e}")
        await callback.answer("Произошла ошибка!", show_alert=True)


@router.callback_query(F.data.startswith("report:"))
async def view_report(callback: CallbackQuery, state: FSMContext, bot):
    if await check_stranger_callback(callback): return
    print("[service_manager/reports] view_report")
    try:
        await push_state(state, ShiftNavigationState.VIEW_REPORT)
        await state.set_state(ShiftNavigationState.VIEW_REPORT)

        data = await state.get_data()
        related_date = data.get("selected_date")
        related_shift = data.get("selected_shift_type")

        if not related_date or not related_shift:
            now = datetime.now()
            related_date, related_shift = now.date(), "Day"

        _, studio_key = callback.data.split(":")

        report_floors = {
            "vip_generic": ["VIP", "GENERIC", "TURKISH", "TritonRL"],
            "gsbj": ["GSBJ"],
            "legendz": ["LEGENDZ"]
        }
        floors = report_floors.get(studio_key.lower(), [])

        def get_report():
            with get_session() as db:
                return generate_studio_report_text(
                    floors=floors,
                    related_date=related_date,
                    related_shift=related_shift,
                    db=db
                )

        report_text = await asyncio.to_thread(get_report)

        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back to studios", callback_data="select_report")
        kb.adjust(1)

        await callback.message.edit_text(
            text=f"<pre>{report_text}</pre>",
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Error in view_report: {e}")
        await callback.answer("Произошла ошибка!", show_alert=True)


@router.callback_query(F.data == "return_shift")
async def proxy_return_shift(callback: CallbackQuery, state: FSMContext, bot):
    if await check_stranger_callback(callback): return
    from tg_bot.handlers.common_callbacks import return_to_dashboard
    await return_to_dashboard(callback, state, bot)