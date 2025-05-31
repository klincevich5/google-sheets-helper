from datetime import date

# Заглушка — позже подключим к PostgreSQL

async def get_shift_stats(selected_date: date, shift_type: str) -> dict:
    # Пример возврата статистики
    return {
        "full": 45,
        "scheduled": 45,
        "actual": 42,
        "manpower": 42 - 45
    }


async def get_feedbacks(selected_date: date, shift_type: str) -> list[str]:
    return [
        "• @dealer_anna — «Great job!»",
        "• @dealer_ivan — «Slow dealing in round 5.»",
        "• @dealer_pavel — «Excellent speed.»"
    ]


async def get_mistakes(selected_date: date, shift_type: str) -> list[str]:
    return [
        "• @dealer_anna — «Missed payout on table 4»",
        "• @dealer_ivan — «Incorrect dealing in round 2»"
    ]
