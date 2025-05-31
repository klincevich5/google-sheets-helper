from datetime import date

async def get_sm_main_view(user_id: int, shift_date: date, shift_type: str) -> str:
    icon = "🌞" if shift_type == "day" else "🌙"
    return (
        f"🔎 Hello, I am SM bot. How can I help you?\n"
        f"📅 Viewing: {shift_date.strftime('%d %b %Y')} — {icon} {'Day' if shift_type == 'day' else 'Night'} shift\n\n"
        f"🧮 Shift stats:\n"
        f"• Full shift: 45\n"
        f"• Scheduled: 45\n"
        f"• Actual: 42\n"
        f"• Manpower: -3\n\n"
        f"💬 Feedbacks: 5 | ⚠️ Mistakes: 3"
    )


async def get_dealer_main_view(user_id: int, shift_date: date, shift_type: str) -> str:
    icon = "🌞" if shift_type == "day" else "🌙"
    return (
        f"👋 Hello, Dealer!\n"
        f"📅 Your shift: {shift_date.strftime('%d %b %Y')} — {icon} {'Day' if shift_type == 'day' else 'Night'}\n\n"
        f"💬 Feedbacks: 2\n"
        f"⚠️ Mistakes: 1"
    )


async def get_architect_main_view(user_id: int, shift_date: date, shift_type: str) -> str:
    icon = "🌞" if shift_type == "day" else "🌙"
    return (
        f"🧠 Architect dashboard\n"
        f"📅 Viewing: {shift_date.strftime('%d %b %Y')} — {icon} {'Day' if shift_type == 'day' else 'Night'} shift\n\n"
        f"📊 Analytics access ready.\n"
        f"🛠 Tasks, team diagnostics, system checks."
    )
