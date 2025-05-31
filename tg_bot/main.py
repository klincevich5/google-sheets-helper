import asyncio
from bot import bot, dp
from handlers import (
    common, viewing_shift, calendar_navigation,
    dealer, service_manager, architect, common_callbacks
)

async def main():
    # Подключаем роутеры
    dp.include_routers(
        common.router,
        viewing_shift.router,
        calendar_navigation.router,

        # дилер
        dealer.feedback.router,
        dealer.mistakes.router,

        # СМ
        service_manager.feedback.router,
        service_manager.reports.router,
        service_manager.team.router,

        # архитектор
        architect.tasks.router,

        # общие callback-и
        common_callbacks.router,
    )

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
