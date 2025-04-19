import logging
from logging.config import fileConfig

import yadisk_async
from aiogram import types
from aiogram.dispatcher import FSMContext

from src import settings
from src.db.models import Birthday
from src.db.shared import get_session
from src.scheduler import Scheduler
from src.states import AddBirthday
from src.toolbox.birthdays import (
    Messages,
    add_birthday,
    dispatch_birthday_messages_to_chat,
)
from src.toolbox.birthdays.messageformat import decline_month
from src.utils import (
    days_grid_reply_kb,
    days_in_month,
    months_grid_reply_kb,
    set_inline_button,
    update_envar,
)

from .common import cmd_cancel

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


async def cmd_send_birthday_messages(message: types.Message):
    """Command for requesting birthday info."""
    if not Messages.is_fresh(minutes=30):
        await Messages.load()
        logger.info("load birthday messages via request from user")
    await dispatch_birthday_messages_to_chat(message.chat.id)


async def cmd_add_chat_to_birthday_mailing(message: types.Message):
    """Command for adding chat-requester to birthday maling.
    Currently days and time are unavailable to choose.
    Need to implement tihs later.
    """
    chat_id = message.chat.id
    try:
        Scheduler.add_chat_to_birthday_mailing(chat_id)
    except Exception as e:
        logger.error(f"Scheduler <add_chat_to_mailing> error: {e}")
        await message.answer(
            "Не удалось добавить чат в список рассылки.\n"
            "Попробуйте позднее.",
            disable_notification=True,
        )
    else:
        logger.info(f"Chat[{chat_id}] added to mailing list")
        await message.answer(
            "Ежедневная рассылка списка дней рождения партнеров "
            "для данного чата запланирована.\n"
            "Рассылка осуществляется каждый день в 09:00 МСК.",
            disable_notification=True,
        )


async def cmd_remove_chat_from_birthday_mailing(message: types.Message):
    "Command for removing chat-requester from birthday mailing list."
    chat_id = message.chat.id
    try:
        Scheduler.remove_job(str(chat_id))
    except Exception as e:
        logger.error(f"Scheduler <remove_chat_from_mailing> error: {e}")
        await message.answer(
            "Не удалось удалить чат из списка рассылки.\n"
            "Попробуйте позднее.",
            disable_notification=True,
        )
    else:
        logger.info(f"Chat[{chat_id}] removed from mailing list")
        await message.answer(
            "Чат исключен из списка ежедневной рассылки дней рождения партнеров. "
            "Вы можете возобновить расслыку с помощью команды /addchat.",
            disable_notification=True,
        )


async def cmd_verify_confirm_code(message: types.Message):
    """Command for Yandex.Disk confirmation code verification.
    If code valid, sets new token to yadisk_async.YaDisk instance.
    If code is invalid sends a callback message with button
    for generating new code.
    """
    confrm_code = message.get_args()
    if not confrm_code:
        await message.reply(
            "Вы не передали код. Попробуйте еще раз.\n"
            "Введите команду /code, добавьте пробел и напишите ваш код.",
            disable_notification=True,
        )
    else:
        disk = yadisk_async.YaDisk(
            id=settings.YANDEX_APP_ID, secret=settings.YANDEX_SECRET_CLIENT
        )
        try:
            resp = await disk.get_token(confrm_code)
        except yadisk_async.exceptions.BadRequestError:
            kbd = set_inline_button(
                text="Получить ссылку на новый код",
                callback_data="confirm_code",
            )
            await message.answer(
                "Вы ввели неверный код. Попробуйте получить новый код.",
                reply_markup=kbd,
                disable_notification=True,
            )
        else:
            new_token = resp.access_token
            disk.token = new_token
            if await disk.check_token():
                # update YADISK_TOKEN env var
                update_envar(
                    settings.BASE_DIR / ".env", "YADISK_TOKEN", new_token
                )
                await message.answer(
                    "Код прошел проверку. Получите информацию о днях рождениях "
                    "партнеров вызвав команду /sendbdays.",
                    disable_notification=True,
                )
            else:
                await message.answer(
                    "Что-то пошло не так. Обратитесь к разработчику.",
                    disable_notification=True,
                )


async def get_confirm_code(call: types.CallbackQuery):
    """Callback for sending 'obtain Yandex.Disk code' url to user."""
    disk = yadisk_async.YaDisk(
        id=settings.YANDEX_APP_ID, secret=settings.YANDEX_SECRET_CLIENT
    )
    await call.message.answer(disk.get_code_url(), disable_notification=True)
    await call.answer()


async def cmd_new_birthday(message: types.Message, state: FSMContext):
    await message.answer(
        "1/3 ➡️ Выберите месяц 📅, воспользовавшись интерактивной клавиатурой",
        reply_markup=months_grid_reply_kb(),
        disable_notification=True,
    )
    await state.set_state(AddBirthday.month.state)


async def new_birthday_month(message: types.Message, state: FSMContext):
    month = message.text.lower()
    if month == "❌ отмена":
        await cmd_cancel(message, state)
        return
    elif month not in settings.MONTHS:
        await message.answer(
            "❗Пожалуйста, выберите месяц, "
            "воспользовавшись интерактивной клавиатурой.",
            disable_notification=True,
        )
        return
    await state.update_data(month=month)

    await state.set_state(AddBirthday.day.state)
    await message.answer(
        "2/3 ➡️ Выберите день 🔢, воспользовавшись интерактивной клавиатурой.",
        reply_markup=days_grid_reply_kb(month),
        disable_notification=True,
    )


async def new_birthday_day(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    max_days = days_in_month(user_data.get("month"))
    day = message.text
    if day == "❌ отмена":
        await cmd_cancel(message, state)
        return
    elif day == "🔄 начать заново":
        await state.reset_state()
        await cmd_new_birthday(message, state)
        return
    elif not day.isnumeric() or int(day) < 1 or int(day) > max_days:
        await message.answer(
            "❗Пожалуйста, выберите день, "
            "воспользовавшись интерактивной клавиатурой.",
            disable_notification=True,
        )
        return
    await state.update_data(day=day)

    await state.set_state(AddBirthday.name.state)
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=5)
    kb.row("❌ отмена", "🔄 начать заново")
    await message.answer(
        "3/3 ➡️ Введите ФИО партнера, используя в том числе "
        "заглавные буквы и пробелы\n."
        "ФИО должно содержать не менее 5-ти символов и 1-го пробела.",
        reply_markup=kb,
        disable_notification=True,
    )


async def new_birthday_name(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    name = message.text.strip()
    if name == "❌ отмена":
        await cmd_cancel(message, state)
        return
    elif name == "🔄 начать заново":
        await state.reset_state()
        await cmd_new_birthday(message, state)
        return
    elif len(name) < 5 and " " not in name:
        await message.answer(
            "❗ФИО должно быть более 5 символов в длину "
            "и содержать хотя бы один пробел.",
            disable_notification=True,
        )
        return

    # Check if partner already exists.
    with get_session() as session:
        if Birthday.queries.get(session, name=name):
            await message.answer(
                f"❗Партнер с ФИО `{name}` уже существует."
                "\nУбедитесь, что вы добвляете ФИО нового партнера.\n"
                "При совпадении ФИО, добавьте через пробел атрибут отличия.\n"
                "Например: Иванов Иван Минобрнауки.",
                disable_notification=True,
            )
            return

    await state.update_data(name=name)
    await state.set_state(AddBirthday.complete.state)

    month = user_data.get("month")
    day = user_data.get("day")

    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("❌ отмена", "🔄 начать заново", "💾 сохранить")

    await message.answer(
        "Вы ввели день рождения партнера:\n"
        f"💎 {day} {decline_month(month)}, {name}."
        "\nВы можете сохранить 💾, отменить ❌ или ввести "
        "день рождения заново 🔄, воспользовавшись "
        "соответствующими кнопками на интерактивной клавиатуре.",
        reply_markup=kb,
        disable_notification=True,
    )


async def new_birthday_complete(message: types.Message, state: FSMContext):
    command = message.text
    if command == "❌ отмена":
        await cmd_cancel(message, state)
        return
    elif command == "🔄 начать заново":
        await state.reset_state()
        await cmd_new_birthday(message, state)
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
                "👍Вы добавили день рождения нового партнера:\n"
                f"{day} {decline_month(month)}, {name}",
                disable_notification=True,
            )

            # Refresh messages in database.
            await Messages.load()
            logger.info(
                "load birthday messages after remote file update by user"
            )
        else:
            await message.answer(
                "🔌Не удалось обновить файл на Яндекс Диске.\n"
                "Менеджер бота уже в курсе проблемы и работает над решением.\n"
                "Попробуйте повторить попытку позднее.",
                disable_notification=True,
            )
        await state.finish()
    else:
        await message.answer(
            "❗Для продолжения, введите команду, "
            "воспользовавшись интерактивной клавиатурой.",
            disable_notification=True,
        )
        return
