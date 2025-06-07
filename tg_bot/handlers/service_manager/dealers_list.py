from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.common_callbacks import push_state
from tg_bot.utils.utils import get_current_shift_and_date
from tg_bot.utils.dealers_list_creator import generate_dealers_list_text
from database.session import SessionLocal
from datetime import datetime
from core.config import TIMEZONE
from zoneinfo import ZoneInfo
import asyncio

# Проверка корректности TIMEZONE
try:
    timezone = ZoneInfo(TIMEZONE)
except Exception as e:
    raise ValueError(f"Некорректное значение TIMEZONE: {TIMEZONE}. Ошибка: {e}")

router = Router()

@router.callback_query(F.data == "view_dealers_list")
async def view_dealers_list(callback: CallbackQuery, state: FSMContext, bot):
    print("[service_manager/team] view_dealers_list")

    try:
        await push_state(state, ShiftNavigationState.VIEW_DEALERS_LIST)
        await state.set_state(ShiftNavigationState.VIEW_DEALERS_LIST)

        data = await state.get_data()
        related_date = data.get("selected_date")
        related_shift = data.get("selected_shift_type")

        current_shift, current_date = get_current_shift_and_date(datetime.now(timezone))

        if not related_date or not related_shift:
            related_date = related_date or current_date
            related_shift = related_shift or current_shift
            await state.update_data(
                selected_date=related_date,
                selected_shift_type=related_shift
            )

        def load_data():
            with SessionLocal() as db:
                return generate_dealers_list_text(db, related_date, related_shift)

        message_text = await asyncio.to_thread(load_data)

        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Back", callback_data="return_shift")
        kb.adjust(1)

        await callback.message.edit_text(
            text=message_text,
            reply_markup=kb.as_markup(),
            parse_mode="HTML"
        )

    except Exception as e:
        print(f"Error in view_dealers_list: {e}")
        await callback.answer("Произошла ошибка!", show_alert=True)
