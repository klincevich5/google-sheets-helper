def get_manager_main_view(user, shift_date, shift_type):
    icon = "🌞" if shift_type == "day" else "🌙"
    return (
        f"<b>👔 {user['dealer_name']} (Manager)</b>\n\n"
        f"<code>{shift_date.strftime('%d %b %Y')} — {icon} {'Day' if shift_type == 'day' else 'Night'} shift</code>\n\n"
        "Manager dashboard."
    )
