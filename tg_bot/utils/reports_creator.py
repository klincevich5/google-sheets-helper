from collections import defaultdict
from datetime import date
from sqlalchemy.orm import Session
from database.db_models import MistakeStorage, FeedbackStorage


def normalize_shift_name(shift: str) -> str:
    shift = shift.strip().lower()
    if "day" in shift:
        return "Day shift"
    elif "night" in shift:
        return "Night shift"
    return shift.title()


def opposite_shift_name(shift: str) -> str:
    if "day" in shift.lower():
        return "night shift"
    elif "night" in shift.lower():
        return "day shift"
    return shift.lower()


def format_feedback_block(title: str, data: dict[str, list[str]]) -> str:
    if not data:
        return f"{title}\n\nNo one\n"
    lines = [title, ""]
    for name, reasons in data.items():
        joined = ", ".join(reasons)
        lines.append(f"{name} : {joined}")
        lines.append("")  # отступ между людьми
    return "\n".join(lines)


def is_reworked(fb) -> bool:
    return fb.forwarded_feedback and fb.forwarded_feedback.strip()


def generate_studio_report_text(floors: list[str], related_date: date, related_shift: str, db: Session) -> str:
    floor_titles = {
        "GSBJ": "Live88 Gaming Floor (GS)",
        "VIP": "Live88 Gaming Floor",
        "GENERIC": "Live88 Gaming Floor",
        "TURKISH": "Live88 Gaming Floor",
        "TRITONRL": "Live88 Gaming Floor",
        "LEGENDZ": "LegendZ Gaming Floor"
    }
    main_floor = floors[0].upper()
    studio_title = floor_titles.get(main_floor, f"Unknown Floor ({main_floor})")

    normalized_shift = normalize_shift_name(related_shift)
    mistake_shift_key = normalized_shift.split()[0].upper()
    opposite_shift = opposite_shift_name(normalized_shift)

    header = (
        f"{studio_title}\n"
        f"Queen Bee Poland Studio\n"
        f"{normalized_shift} {related_date.strftime('%d.%m.%Y')}\n"
    )

    report_lines = [header]

    # --- Mistakes ---
    mistakes = db.query(MistakeStorage).filter(
        MistakeStorage.floor.in_(floors),
        MistakeStorage.related_date == related_date,
        MistakeStorage.related_shift == mistake_shift_key
    ).all()

    mistake_summary = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # floor -> table -> (mistake, is_cancel) -> count
    canceled_rounds = 0

    for m in mistakes:
        key = (m.mistake, m.is_cancel)
        mistake_summary[m.floor][m.table_name][key] += 1

    # --- Feedbacks (до подсчёта SH) ---
    feedbacks = db.query(FeedbackStorage).filter(
        FeedbackStorage.floor.in_(floors),
        FeedbackStorage.related_date == related_date,
        FeedbackStorage.related_shift == normalized_shift
    ).all()

    if main_floor == "VIP":  # для vip_generic
        # Подсчёт SH shuffle-метрик
        sh_feedbacks = [f for f in feedbacks if f.game == "SH"]
        total_shuffles = len(sh_feedbacks)
        issues_identified = sum(
            1 for f in sh_feedbacks if any(r.strip().startswith("❗") for r in f.reason.split(","))
        )
        reworked = sum(
            1 for f in sh_feedbacks
            if any(r.strip().startswith("❗") for r in f.reason.split(","))
            and is_reworked(f)
        )

        report_lines.append(f"Total Shuffles Reviewed: {total_shuffles}")
        report_lines.append(f"Issues Identified: {issues_identified}")
        report_lines.append(f"Reworked: {reworked}\n")

        for floor in ["VIP", "GENERIC", "TURKISH", "TritonRL"]:
            if floor not in mistake_summary:
                continue
            report_lines.append(f"{floor}:\n")
            for table_name in sorted(mistake_summary[floor]):
                report_lines.append(f"-{table_name}:")
                for (mistake, is_cancel), count in mistake_summary[floor][table_name].items():
                    if " - " in mistake:
                        mistake = mistake.split(" - ", 1)[1]
                    label = f"{mistake} (canceled round)" if is_cancel else mistake
                    report_lines.append(f"{label} x{count}")
                    if is_cancel:
                        canceled_rounds += count
                report_lines.append("")
    else:
        for floor in mistake_summary:
            for table_name in sorted(mistake_summary[floor]):
                report_lines.append(f"-{table_name}:")
                for (mistake, is_cancel), count in mistake_summary[floor][table_name].items():
                    if " - " in mistake:
                        mistake = mistake.split(" - ", 1)[1]
                    label = f"{mistake} (canceled round)" if is_cancel else mistake
                    report_lines.append(f"{label} x{count}")
                    if is_cancel:
                        canceled_rounds += count
                report_lines.append("")

    report_lines.append(f"In total: {canceled_rounds} canceled round{'s' if canceled_rounds != 1 else ''}.\n")
    report_lines.append(f"Replacements for the {opposite_shift}:\nNo one.\n")
    report_lines.append("Incidents:\nNo incidents.\n")
    report_lines.append("Equipment was broken:\nNo one.\n")

    # --- Feedback blocks ---
    gpi_issues = defaultdict(list)
    positive_feedbacks = defaultdict(list)

    for f in feedbacks:
        reasons = [r.strip() for r in f.reason.split(",") if r.strip()]
        for r in reasons:
            if r.startswith("❗"):
                gpi_issues[f.dealer_name].append(r)
            elif r.startswith("✅"):
                positive_feedbacks[f.dealer_name].append(r)

    report_lines.append(format_feedback_block("GP's issues re-worked:", gpi_issues))
    report_lines.append(format_feedback_block("Positive feedbacks:", positive_feedbacks))
    report_lines.append("Out: _____.\nIn: _____.\n")

    return "\n".join(report_lines)
