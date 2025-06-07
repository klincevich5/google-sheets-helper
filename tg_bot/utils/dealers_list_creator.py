from sqlalchemy.orm import Session
from database.db_models import ScheduleOT
from datetime import date


def normalize_shift(shift: str) -> str:
    return "Day" if shift.strip().lower().startswith("day") else "Night"


def generate_dealers_list_text(db: Session, selected_date: date, shift: str) -> str:
    shift_type = normalize_shift(shift)  # "Day" или "Night"
    shift_label = f"{shift_type} shift"

    related_month = selected_date.replace(day=1)

    # Получаем записи
    records = db.query(ScheduleOT).filter(
        ScheduleOT.date == selected_date,
        ScheduleOT.related_month == related_month
    ).all()

    # Определяем смены
    if shift_type == "Day":
        dealer_codes = ["D"]
        shuffler_codes = ["DS"]
    else:
        dealer_codes = ["N"]
        shuffler_codes = ["NS"]

    # Фильтруем и сортируем
    dealers = sorted([r.dealer_name for r in records if r.shift_type in dealer_codes])
    shufflers = sorted([r.dealer_name for r in records if r.shift_type in shuffler_codes])

    # Собираем текст отчёта
    body_lines = [
        f"{shift_label} {selected_date.strftime('%d.%m.%Y')}",
        "",
        f"Man power: {len(dealers)} + {len(shufflers)}.\n",
        f"Floor: {len(dealers)}.",
        ""
    ]
    body_lines.extend(dealers or ["No dealers."])
    body_lines.append("")
    body_lines.append(f"Shufflers: {len(shufflers)}.\n")
    body_lines.extend(shufflers or ["No shufflers."])

    # Оборачиваем в блок кода Telegram
    full_message = "<pre>\n" + "\n".join(body_lines) + "\n</pre>"
    return full_message
