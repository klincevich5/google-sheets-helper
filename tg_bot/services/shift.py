from datetime import date

# Заглушка — позже подключим к PostgreSQL

async def get_shift_stats(selected_date: date, shift_type: str) -> dict:
    """
    Получить статистику смены за выбранную дату.

    :param selected_date: Дата, за которую требуется получить статистику.
    :param shift_type: Тип смены (например, "утренняя", "вечерняя").
    :return: Словарь со статистикой смены.
    """
    # Пример возврата статистики
    return {
        "full": 45,
        "scheduled": 45,
        "actual": 42,
        "manpower": 42 - 45
    }


async def get_feedbacks(selected_date: date, shift_type: str) -> list[str]:
    """
    Получить отзывы дилеров за выбранную дату.

    :param selected_date: Дата, за которую требуется получить отзывы.
    :param shift_type: Тип смены (например, "утренняя", "вечерняя").
    :return: Список строк с отзывами дилеров.
    """
    return [
        "• @dealer_anna — «Great job!»",
        "• @dealer_ivan — «Slow dealing in round 5.»",
        "• @dealer_pavel — «Excellent speed.»"
    ]


async def get_mistakes(selected_date: date, shift_type: str) -> list[str]:
    """
    Получить список ошибок дилеров за выбранную дату.

    :param selected_date: Дата, за которую требуется получить список ошибок.
    :param shift_type: Тип смены (например, "утренняя", "вечерняя").
    :return: Список строк с ошибками дилеров.
    """
    return [
        "• @dealer_anna — «Missed payout on table 4»",
        "• @dealer_ivan — «Incorrect dealing in round 2»"
    ]

# если потребуется текущее время, используйте:
# from core.timezone import now
