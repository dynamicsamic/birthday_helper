# import logging
# from logging.config import fileConfig

# from aiogram import Bot, Dispatcher, executor, types

# from app.bot import dispatcher as dp
# from app.db.shared import Base, db_engine
# from app.handlers import register_birthday_handlers, register_common_handlers
# from app.scheduler import Scheduler

# fileConfig(fname="log_config.conf", disable_existing_loggers=False)
# logger = logging.getLogger(__name__)


# async def set_bot_commands(bot: Bot):
#     commands = [
#         types.BotCommand("addchat", "добавить чат в рассылку"),
#         types.BotCommand("removechat", "удалить чат из рассылки"),
#         types.BotCommand("sendbdays", "получить список ближайших ДР"),
#         types.BotCommand("newbirthday", "добавить новый ДР"),
#         types.BotCommand("cancel", "отменить команду"),
#     ]
#     await bot.set_my_commands(commands)


# async def on_startup(dp: Dispatcher):
#     """Execute before Bot start polling."""
#     await set_bot_commands(dp.bot)
#     Base.metadata.create_all(db_engine)
#     Scheduler.start()


# async def on_shutdown(_: Dispatcher):
#     """Execute before Bot stops polling."""
#     Scheduler.remove_all_jobs()
#     Scheduler.shutdown()


# if __name__ == "__main__":
#     register_common_handlers(dp)
#     register_birthday_handlers(dp)
#     executor.start_polling(
#         dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown
#     )
import asyncio

from src.telegram.bot import init_bot

if __name__ == "__main__":
    asyncio.run(init_bot())
