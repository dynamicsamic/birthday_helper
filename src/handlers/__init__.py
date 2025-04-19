from aiogram import Dispatcher

from src.states import AddBirthday

from .birthdays import (
    cmd_add_chat_to_birthday_mailing,
    cmd_new_birthday,
    cmd_remove_chat_from_birthday_mailing,
    cmd_send_birthday_messages,
    cmd_verify_confirm_code,
    get_confirm_code,
    new_birthday_complete,
    new_birthday_day,
    new_birthday_month,
    new_birthday_name,
)
from .common import cmd_cancel, cmd_start


def register_common_handlers(dp: Dispatcher):
    dp.register_message_handler(cmd_start, commands=["start"], state="*")
    dp.register_message_handler(cmd_cancel, commands=["cancel"], state="*")
    dp.register_callback_query_handler(
        cmd_cancel, text="cancel", state=AddBirthday
    )


def register_birthday_handlers(dp: Dispatcher):
    dp.register_message_handler(
        cmd_add_chat_to_birthday_mailing, commands=["addchat"]
    )
    dp.register_message_handler(
        cmd_remove_chat_from_birthday_mailing, commands=["removechat"]
    )

    dp.register_message_handler(
        cmd_send_birthday_messages, commands=["sendbdays"]
    )
    dp.register_message_handler(cmd_verify_confirm_code, commands=["code"])
    dp.register_callback_query_handler(get_confirm_code, text="confirm_code")

    dp.register_message_handler(
        cmd_new_birthday, commands=["newbirthday"], state="*"
    )
    dp.register_callback_query_handler(
        cmd_new_birthday, text="newbirthday", state="*"
    )
    dp.register_message_handler(new_birthday_month, state=AddBirthday.month)
    dp.register_message_handler(new_birthday_day, state=AddBirthday.day)
    dp.register_message_handler(new_birthday_name, state=AddBirthday.name)
    dp.register_message_handler(
        new_birthday_complete, state=AddBirthday.complete
    )
