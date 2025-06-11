from datetime import datetime, time, timedelta
from collections import defaultdict
from sqlalchemy.orm import Session
from core.timezone import timezone
from database.db_models import MistakeStorage, FeedbackStorage

def get_shift_datetime_range(date, shift_type):
    if shift_type == "day":
        start = datetime.combine(date, time(9, 0), tzinfo=timezone)
        end = datetime.combine(date, time(21, 0), tzinfo=timezone)
    else:
        start = datetime.combine(date, time(21, 0), tzinfo=timezone)
        end = datetime.combine(date + timedelta(days=1), time(9, 0), tzinfo=timezone)
    return start, end

async def generate_structured_shift_report(date, shift_type, session: Session) -> str:
    shift_start, shift_end = get_shift_datetime_range(date, shift_type)
    date_str = date.strftime("%d.%m.%Y")
    shift_label = shift_type.capitalize()

    # ==== MISTAKES ====
    mistakes = session.query(MistakeStorage).filter(
        MistakeStorage.date + MistakeStorage.time >= shift_start,
        MistakeStorage.date + MistakeStorage.time < shift_end
    ).all()

    grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    cancel_count = 0
    for m in mistakes:
        key = f"{m.mistake} (Cancelled)" if m.is_cancel else m.mistake
        if m.is_cancel:
            cancel_count += 1
        grouped[m.floor][m.table_name][key] += 1

    # ==== FEEDBACKS ====
    feedbacks = session.query(FeedbackStorage).filter(
        FeedbackStorage.date == date,
        FeedbackStorage.shift.ilike(shift_type)
    ).all()
    

    positive_by_gp = defaultdict(list)
    negative_by_gp = defaultdict(list)
    for f in feedbacks:
        gp = f.gp_name_surname
        reason = f.reason
        if reason.startswith("✅"):
            positive_by_gp[gp].append(reason.strip("✅").strip())
        elif reason.startswith("❗"):
            negative_by_gp[gp].append(reason.strip("❗").strip())

    # ==== HEAD ====
    lines = [
        "Live88 Gaming Floor",
        "Queen Bee Poland Studio",
        f"{shift_label} shift {date_str}",
        "",
        "Total Shuffles Reviewed: 99",
        "Issues Identified: 1",
        "Reworked: 1",
        ""
    ]

    # ==== VIP & GENERIC ====
    for zone in ["VIP", "GENERIC"]:
        if zone in grouped:
            lines.append(f"{zone}:\n")
            for table, mistakes in grouped[zone].items():
                lines.append(f"{table}:")
                for mistake, count in mistakes.items():
                    line = f"{mistake} x{count}" if count > 1 else mistake
                    lines.append(line)
                lines.append("")

    # ==== FOOTER ====
    lines.append(f"In total: {cancel_count} canceled rounds.\n")

    # Заглушки по умолчанию (можно потом заменить на реальные данные)
    lines.append(f"Replacements for the {shift_label} shift:")
    lines += [
        "No one.",
        ""
    ]
    lines.append("Incidents:")
    lines.append("No one.\n")
    lines.append("Equipment was broken:")
    lines.append("None.\n")

    lines.append("GP issues re-worked:")
    for gp, issues in negative_by_gp.items():
        joined = " ❗" + " ❗".join(issues)
        lines.append(f"{gp}: {joined}")
    lines.append("")

    lines.append("Positive feedbacks:")
    for gp, good in positive_by_gp.items():
        joined = " ✅" + " ✅".join(good)
        lines.append(f"{gp}: {joined}")
    lines.append("")

    # In/Out — заглушка
    lines.append("Out: Artem Kad., Anton Kl.")
    lines.append("In: Mykyta Kr., Yevhenii Ish.")

    return "\n".join(lines)
