from aiogram import Router, F
from aiogram.types import Message, BufferedInputFile
from sqlalchemy.orm import Session
from datetime import datetime
from io import BytesIO

from database import get_session  # реализуй, если ещё нет
from utils.report_generator import generate_structured_shift_report  # см. ниже

router = Router()

@router.message(F.text.startswith("/shift_report"))
async def get_shift_report_handler(message: Message):
    try:
        parts = message.text.split()
        if len(parts) < 3:
            await message.answer("❗ Укажи дату и смену. Пример: `/shift_report 2025-05-24 day`")
            return

        shift_date = datetime.strptime(parts[1], "%Y-%m-%d").date()
        shift_type = parts[2].lower()

        async with get_session() as session:  # SQLAlchemy AsyncSession или sync в зависимости от твоей реализации
            report_text = await generate_structured_shift_report(shift_date, shift_type, session)

        buffer = BytesIO(report_text.encode("utf-8"))
        buffer.name = f"Shift_Report_{shift_date}_{shift_type}.txt"
        await message.answer_document(BufferedInputFile(buffer.getvalue(), filename=buffer.name))

    except Exception as e:
        await message.answer(f"🚫 Ошибка генерации отчёта: {e}")
