import logging
from logging.config import fileConfig

from aiogram import Bot, Router, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from task_schedule.task_scheduler import (
    add_chat_to_birthday_mailing_list,
    remove_chat_from_birthday_mailing_list,
)

from src import settings
from src.state.state_manager import BirthdayDataManager
from src.telegram.handlers.common import cmd_cancel
from src.telegram.messages.birthday import (
    AddBirthday,
    AddToBirthdayMailingList,
    RemoveFromBirthdayMailingList,
)
from src.telegram.states import NewBirthday
from src.utils import days_grid_reply_kb, days_in_month, months_grid_reply_kb

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)
router = Router()


router = Router()


async def send_birthday_feed_to_chat(
    bot: Bot,
    chat_id: int,
    birthday_sync_manager: BirthdayDataManager,
):
    await birthday_sync_manager.sync_state()


@router.message(Command(commands=["send_birthday_feed"], ignore_case=True))
async def send_birthday_feed(message: Message, state_manager: BirthdayDataManager):
    """Command for requesting birthday info."""
    await send_birthday_feed_to_chat(
        message.bot,
        message.chat.id,
        state_manager,
    )
    await message.delete  # ??


@router.message(Command(commands=["get_birthday"], ignore_case=True))
async def get_birthday(message: Message, state_manger: BirthdayDataManager):
    birthday = await state_manger.get_birthday(message.text)
    # format birthday
    await message.answer(birthday)


@router.message(Command(commands=["add_birthday"]))
async def add_birthday(message: Message, state: FSMContext):
    await message.answer(
        AddBirthday.GET_MONTH,
        reply_markup=months_grid_reply_kb(),
        disable_notification=True,
    )
    await state.set_state(NewBirthday.month)


@router.message(NewBirthday.month)
async def add_birthday_set_month(message: Message, state: FSMContext):
    month = message.text.lower()
    if month == "❌ отмена":
        await cmd_cancel(message, state)
        return
    elif month not in settings.MONTHS:
        await message.answer(AddBirthday.INVALID_MONTH, disable_notification=True)
        return
    await state.update_data(month=month)

    await state.set_state(NewBirthday.day)
    await message.answer(
        AddBirthday.GET_DAY,
        reply_markup=days_grid_reply_kb(month),
        disable_notification=True,
    )


@router.message(NewBirthday.day)
async def add_birthday_set_day(message: Message, state: FSMContext):
    user_data = await state.get_data()
    max_days = days_in_month(user_data.get("month"))
    day = message.text
    if day == "❌ отмена":
        await cmd_cancel(message, state)
        return
    elif day == "🔄 начать заново":
        await state.clear()
        await add_birthday(message, state)
        return
    elif not day.isnumeric() or int(day) < 1 or int(day) > max_days:
        await message.answer(AddBirthday.INVALID_DAY, disable_notification=True)
        return
    await state.update_data(day=day)

    await state.set_state(NewBirthday.name)
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=5)
    kb.row("❌ отмена", "🔄 начать заново")
    await message.answer(
        AddBirthday.GET_NAME,
        reply_markup=kb,
        disable_notification=True,
    )


@router.message(NewBirthday.name)
async def add_birthday_complete(message: Message, state: FSMContext):
    command = message.text
    if command == "❌ отмена":
        await cmd_cancel(message, state)
        return
    elif command == "🔄 начать заново":
        await state.clear()
        await add_birthday(message, state)
        return
    elif command == "💾 сохранить":
        user_data = await state.get_data()
        month, day, name = user_data.values()

        # This depends heavily on the structure of your excel file.
        # In our case we only add data to the first 4 columns,
        # where 3-rd column is not currently in use, that's why it's empty str.
        # The order of values in birtday_data list is CRUCIAL!
        birthday_data = [int(day), month, "", name]

        await message.answer(
            "⏳Обновляю файл на Яндекс Диске...",
            disable_notification=True,
            reply_markup=types.ReplyKeyboardRemove(),
        )
        if await add_birthday(message, birthday_data):
            await message.answer(
                AddBirthday.SUCCESS,  # remove this coma
                # f"{day} {decline_month(month)}, {name}",
                disable_notification=True,
            )

            # Refresh messages in database.
            # await Messages.load()
            logger.info("load birthday messages after remote file update by user")
        else:
            await message.answer(
                AddBirthday.CLOUD_SYNC_FAILURE,
                disable_notification=True,
            )
        await state.clear()
    else:
        await message.answer(
            "❗Для продолжения, введите команду, "
            "воспользовавшись интерактивной клавиатурой.",
            disable_notification=True,
        )
        return


@router.message(Command(commands=["add_chat"], ignore_case=True))
async def cmd_add_chat_to_birthday_mailing_list(
    message: types.Message,
    state_manager: BirthdayDataManager,
    task_scheduler: AsyncIOScheduler,
):
    """Command for adding chat-requester to birthday maling.
    Currently days and time are unavailable to choose.
    Need to implement tihs later.
    """
    # Need to define the allowed chat list and provide a check of new chats.
    chat_id = message.chat.id
    try:
        add_chat_to_birthday_mailing_list(
            task_scheduler, message.bot, chat_id, state_manager
        )
    except Exception as e:
        logger.error(f"Scheduler <add_chat_to_mailing> error: {e}")
        await message.answer(
            AddToBirthdayMailingList.FAILURE,
            disable_notification=True,
        )
    else:
        logger.info(f"Chat[{chat_id}] added to mailing list")
        await message.answer(
            AddToBirthdayMailingList.SUCCESS,
            disable_notification=True,
        )


@router.message(Command(commands=["remove_chat"], ignore_case=True))
async def cmd_remove_chat_from_birthday_mailing(
    message: types.Message, task_scheduler: AsyncIOScheduler
):
    "Command for removing chat-requester from birthday mailing list."
    chat_id = message.chat.id
    try:
        remove_chat_from_birthday_mailing_list(task_scheduler, chat_id)
    except Exception as e:
        logger.error(f"Scheduler <remove_chat_from_mailing> error: {e}")
        await message.answer(
            RemoveFromBirthdayMailingList.FAILURE,
            disable_notification=True,
        )
    else:
        logger.info(f"Chat[{chat_id}] removed from mailing list")
        await message.answer(
            RemoveFromBirthdayMailingList.SUCCESS,
            disable_notification=True,
        )
