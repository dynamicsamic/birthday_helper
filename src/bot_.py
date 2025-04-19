from aiogram import Bot, Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage

from . import settings

bot = Bot(token=settings.BOT_TOKEN)
dispatcher = Dispatcher(bot, storage=MemoryStorage())
