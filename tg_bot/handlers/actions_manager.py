# tg_bot/handlers/actions_manager.py

from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, BufferedInputFile
from aiogram.fsm.context import FSMContext
from tg_bot.states.shift_navigation import ShiftNavigationState
from tg_bot.handlers.viewing_shift import render_shift_dashboard
from aiogram.utils.keyboard import InlineKeyboardBuilder
from io import BytesIO
from datetime import date as dt_date
from database.session import SessionLocal
from utils.report_generator import generate_structured_shift_report

router = Router()


@router.callback_query(F.data == "get_shift_report")
async def get_shift_report(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    date: dt_date = data.get("selected_date")
    shift_type: str = data.get("selected_shift_type")
    chat_id = data.get("chat_id", callback.message.chat.id)
    message_id = data.get("message_id", callback.message.message_id)

    if not date or not shift_type:
        await callback.answer("❗️Дата или тип смены не найдены.")
        return

    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=(
                f"📄 <b>Shift Report</b>\n"
                f"🗓 <b>{date.strftime('%d %b %Y')}</b>\n"
                f"Type: {'🌞 Day' if shift_type == 'day' else '🌙 Night'}\n\n"
                f"⏳ Generating report..."
            ),
            parse_mode="HTML"
        )
        with SessionLocal() as session:
            try:
                report_text = generate_structured_shift_report(date, shift_type, session)
            finally:
                session.close()

        buffer = BytesIO(report_text.encode("utf-8"))
        filename = f"Shift_Report_{date.strftime('%Y-%m-%d')}_{shift_type}.txt"
        document = BufferedInputFile(buffer.getvalue(), filename=filename)

        await bot.send_document(
            chat_id=chat_id,
            document=document,
            caption=f"📄 <b>Shift Report</b>\n🗓 <b>{date.strftime('%d %b %Y')}</b>\nType: {'🌞 Day' if shift_type == 'day' else '🌙 Night'}",
            parse_mode="HTML"
        )

    except Exception as e:
        await bot.send_message(chat_id, f"🚫 Ошибка генерации отчёта: {e}")


@router.callback_query(F.data == "report_back")
async def report_back(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(ShiftNavigationState.VIEWING_SHIFT)
    await render_shift_dashboard(callback.message, state, bot)
