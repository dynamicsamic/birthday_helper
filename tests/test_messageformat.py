import datetime as dt

import pytest

from src.db.models import Birthday
from src.toolbox.birthdays.messageformat import (
    birthday_to_message,
    convert_month,
    decline_month,
    format_birthday_sequence,
    get_formatted_messages,
)

from .common import constants
from .fixtures.db import (
    create_birthday_range,
    create_tables,
    db_session,
    engine,
)


def test_convert_month_with_integer_returns_string_month():
    int_month = 1
    expected = "январь"
    assert convert_month(int_month) == expected


def test_convert_month_with_string_digit_returns_string_month():
    int_month = "1"
    expected = "январь"
    assert convert_month(int_month) == expected


def test_convert_month_with_string_returns_int():
    str_month = "январь"
    expected = 1
    assert convert_month(str_month) == expected


def test_convert_month_with_invalud_string_returns_default_value():
    default = 1
    assert convert_month("hello world") == default


def test_convert_month_with_invalud_integer_returns_default_value():
    default = "январь"
    assert convert_month(33) == default


def test_decline_month_with_valid_input_returns_declined_month():
    assert decline_month("март") == "марта"
    assert decline_month("апрель") == "апреля"


def test_decline_month_with_empty_input_returns_default():
    default = "января"
    assert decline_month(None) == default


def test_birthday_to_message_returns_formatted_string(db_session):
    date_ = {"year": 2023, "month": 1, "day": 15}
    payload = {"name": "valid_partner", "date": dt.date(**date_)}
    birthday = Birthday(**payload)
    db_session.add(birthday)
    db_session.commit()
    result = birthday_to_message(birthday)
    expected = f"{date_['day']} {decline_month(convert_month(date_['month']))}, {birthday.name}"
    assert isinstance(result, str)
    assert result == expected


def test_format_birthday_sequence_returns_formatted_string_of_all_birthdays(
    db_session, create_birthday_range
):
    birthdays = Birthday.queries.all(db_session)
    fmt_string = format_birthday_sequence(birthdays)

    # If we split the result string by line ends, size of resulting list
    # shoul be equal to number of all birthdays persisted in db table.
    assert len(fmt_string.splitlines()) == constants["TEST_SAMPLE_SIZE"]


def test_format_birthday_sequence_provides_specific_format(
    db_session, create_birthday_range
):
    birthdays = Birthday.queries.all(db_session)
    fmt_string = format_birthday_sequence(birthdays)

    for birthday, string in zip(birthdays, fmt_string.splitlines()):
        assert birthday_to_message(birthday) == string


def test_get_formatted_messages_adds_today_header(
    db_session, create_birthday_range
):
    birthdays = Birthday.queries.all(db_session)
    fmt_string = get_formatted_messages(birthdays)

    header = "#деньрождения сегодня:\n"

    assert fmt_string.startswith(header)


def test_get_formatted_messages_adds_future_header(
    db_session, create_birthday_range
):
    birthdays = Birthday.queries.all(db_session)
    fmt_string = get_formatted_messages(birthdays, today=False)

    header = "#деньрождения завтра и следующие два дня:\n"

    assert fmt_string.startswith(header)


def test_get_formatted_messages_return_None_for_empty_birthdays(
    db_session, create_birthday_range
):
    birthdays = []
    fmt_string = get_formatted_messages(birthdays)

    assert fmt_string is None
