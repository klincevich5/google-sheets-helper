# tg_bot.main.py

import asyncio
from tg_bot.bot import bot, dp
from tg_bot.handlers import (
    common, viewing_shift, calendar_navigation,
    common_callbacks
)
from tg_bot.handlers import architect, dealer, service_manager

async def main():
    # Подключаем роутеры
    dp.include_routers(
        common,  # Исправлено: теперь просто common, а не common.router
        viewing_shift,
        calendar_navigation,

        # дилер
        dealer.feedback.router,
        dealer.mistakes.router,

        # СМ
        service_manager.feedback.router,
        service_manager.reports.router,
        service_manager.team.router,
        service_manager.dealers_list.router,
        service_manager.rotations.router,

        # архитектор
        architect.tasks.router,

        # общие callback-и
        common_callbacks,
    )

    await dp.start_polling(bot, handle_signals=False)

if __name__ == "__main__":
    asyncio.run(main())
