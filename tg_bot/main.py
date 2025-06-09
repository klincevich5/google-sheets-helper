# tg_bot.main.py

import asyncio
from tg_bot.bot import bot, dp
from tg_bot.handlers import (
    common, viewing_shift, calendar_navigation,
    common_callbacks
)
from tg_bot.handlers import architect, dealer, service_manager
from tg_bot.handlers.common_callbacks import setup_callback_security

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

    # Настройка безопасности callback для всех роутеров
    for r in [
        viewing_shift, calendar_navigation,
        dealer.feedback.router, dealer.mistakes.router,
        service_manager.feedback.router, service_manager.reports.router,
        service_manager.team.router, service_manager.dealers_list.router,
        service_manager.rotations.router,
        architect.tasks.router,
        common_callbacks,
    ]:
        setup_callback_security(r)

    await dp.start_polling(bot, handle_signals=False)

if __name__ == "__main__":
    asyncio.run(main())
