from typing import Sequence

from src import settings
from src.db.models import Birthday

string_to_int_mapping = {
    month: i
    for month, i in zip(settings.MONTHS, range(1, len(settings.MONTHS) + 1))
}
int_to_string_mapping = {
    i: month
    for month, i in zip(settings.MONTHS, range(1, len(settings.MONTHS) + 1))
}


def convert_month(month: int | str) -> str | int | None:
    """
    Converts `int` month to `str` month (in russian) and vice versa.\n
    Performs type coercion if month provided as string digit (e.g. '1', '25').

    :param month: Either `str` or `int` month value.

    :returns: Either `str` or `int` month value.
    :returns: Default value (either `1` or `январь`)
        if there is no match in month mappings.
    :returns: `None` if provided month not of type `int` or `str`.
    """
    if isinstance(month, str):
        if not month.isdigit():
            return string_to_int_mapping.get(month, 1)
        month = int(month)
    if isinstance(month, int):
        return int_to_string_mapping.get(month, "январь")


def decline_month(month: str) -> str:
    """
    Returns month name in genitive declension in Russian.

    Because `convert_month` may provide a `None` result,
    this case is handled in first place.

    :param month: Name of a month in Russian.

    :returns: Name of a month in Russian with correct word ending.
        e.g. 'январь' -> 'января'; 'март' -> 'марта'.
    """
    if month is None:
        return "января"
    if month.endswith("т"):
        return month + "а"
    return month[:-1] + "я"


def birthday_to_message(birthday: Birthday) -> str:
    """
    Returns birthday attributes in a formatted string.

    :param birthday: An instance of `db.models.Birthday`.

    :returns: Birthday instance data in a formatted string.
        e.g. `20 мая, Иван Иванов`.
    """
    _, name, date = birthday.to_dict().values()
    day, month = date.day, date.month
    declined_month = decline_month(convert_month(month))
    return f"{day} {declined_month}, {name}"


def format_birthday_sequence(birthdays: Sequence[Birthday]) -> str:
    """Converts a sequence of `db.models.Birthday` instances into
    a long string of formatted birthday strings separated by new line char.

    :param birthdays: A sequnce of `db.models.Birthday` instances.

    :returns: A string that contains formatted strings for birthday instances.
        e.g. 20 мая, Иван Иванов\n
             15 сентября, Сергей Сергеев\n
             7 ноября, Екатерина Абрамова."""
    return "\n".join(birthday_to_message(birthday) for birthday in birthdays)


def get_formatted_messages(
    birthdays: Sequence[Birthday], today: bool = True
) -> str | None:
    """Creates a joined formatted string of `birthdays`.
    Adds an appropriate header to the formatted string.

    :param birthdays: Sequnce of `db.models.Birthday` instances.
    :param today: Switch to choose a header for today or future messages.

    :returns: Complete string to be send as a message to telegram chat.
        None, if birthdays is empty.
    """
    if not birthdays:
        return

    birthday_messages = format_birthday_sequence(birthdays)
    header = (
        "#деньрождения сегодня:\n"
        if today
        else "#деньрождения завтра и следующие два дня:\n"
    )
    return f"{header}{birthday_messages}"
