from aiogram import Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, ReplyKeyboardRemove

from src.telegram.messages.common import Commands

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        text=Commands.START,
        reply_markup=ReplyKeyboardRemove(),
        disable_notification=True,
    )


@router.callback_query(Command(commands=["cancel"]))
async def cmd_cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer(
        Commands.CANCEL, reply_markup=ReplyKeyboardRemove(), disable_notification=True
    )
