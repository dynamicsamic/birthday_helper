from aiogram import types
from aiogram.dispatcher import FSMContext

from src.utils import message_or_call

START_MESSAGE = (
    "Привет!👋 Я бот-помощник!\n"
    "Чтобы посмотреть список моих команд, введите символ /"
)


async def cmd_start(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer(
        text=START_MESSAGE,
        reply_markup=types.ReplyKeyboardRemove(),
        disable_notification=True,
    )


@message_or_call
async def cmd_cancel(
    message: types.Message | types.CallbackQuery, state: FSMContext
):
    await state.finish()
    await message.answer(
        "Команда отменена",
        reply_markup=types.ReplyKeyboardRemove(),
        disable_notification=True,
    )
