import pytest

from src import settings
from src.db.models import Birthday
from src.toolbox.birthdays.messageloader import BirthdayMessageLoader
from src.utils import get_bot, today

from .common import constants
from .fixtures.db import (
    create_birthday_range,
    create_tables,
    db_session,
    engine,
)
from .fixtures.files import stored_excel_file, stored_excel_settings
from .fixtures.mocks import get_inmemory_session, yadisk_returns_true


@pytest.mark.asyncio
async def test_generate_mappings_with_invalid_download_kwargs_returns_empty_list(
    yadisk_returns_true, stored_excel_file
):
    yadisk_kwargs = {"token": "mock"}
    download_kwargs = {
        "remote_filepath": "mock/path",
        "local_filepath": "invalid/path",
    }
    bot = get_bot()
    msgloader = BirthdayMessageLoader(yadisk_kwargs, download_kwargs, bot)
    await msgloader._generate_mappings()
    assert msgloader.model_mappings == []


@pytest.mark.asyncio
async def test_generate_mappings_with_valid_download_kwargs_returns_list_of_mappings(
    yadisk_returns_true, stored_excel_file
):
    yadisk_kwargs = {"token": "mock"}
    local_file = constants["EXCEL_FILE"]
    download_kwargs = {
        "remote_filepath": "mock/path",
        "local_filepath": local_file.as_posix(),
    }
    bot = get_bot()
    msgloader = BirthdayMessageLoader(yadisk_kwargs, download_kwargs, bot)
    await msgloader._generate_mappings()

    # Rows with trailing whitespace in string columns are valid.
    expected_result_num = constants["TEST_SAMPLE_SIZE"] + len(
        stored_excel_settings["untrimmed_rows"]
    )
    assert len(msgloader.model_mappings) == expected_result_num


@pytest.mark.asyncio
async def test_load_formatted_messages_return_no_messages_notification_if_no_bdays(
    yadisk_returns_true, db_session, engine
):
    yadisk_kwargs = {"token": "mock"}
    local_file = constants["EXCEL_FILE"]
    download_kwargs = {
        "remote_filepath": "mock/path",
        "local_filepath": local_file.as_posix(),
    }
    bot = get_bot()
    msgloader = BirthdayMessageLoader(
        yadisk_kwargs, download_kwargs, bot, engine
    )
    await msgloader._load_formatted_messages()
    notification = (
        "Сегодня и ближайшие пару дней" " #деньрождения не предвидится."
    )

    assert msgloader.message_store.messages == [notification]


@pytest.mark.asyncio
async def test_load_formatted_messages_store_today_and_future_messages(
    yadisk_returns_true, db_session, engine
):
    from datetime import timedelta

    yadisk_kwargs = {"token": "mock"}
    local_file = constants["EXCEL_FILE"]
    download_kwargs = {
        "remote_filepath": "mock/path",
        "local_filepath": local_file.as_posix(),
    }
    bot = get_bot()
    msgloader = BirthdayMessageLoader(
        yadisk_kwargs, download_kwargs, bot, engine
    )

    birthdays = [
        Birthday(name="partner_001", date=today()),
        Birthday(
            name="partner_002",
            date=today() + timedelta(days=settings.FUTURE_SCOPE),
        ),
        Birthday(
            name="partner_003",
            date=today() + timedelta(days=settings.FUTURE_SCOPE + 2),
        ),
    ]
    db_session.add_all(birthdays)
    db_session.commit()

    await msgloader._load_formatted_messages()
    assert len(msgloader.message_store.messages) == 2
    assert "today" in msgloader.message_store
    assert "future" in msgloader.message_store


@pytest.mark.asyncio
async def test_load_store_warnign_message_with_empty_db(
    yadisk_returns_true, db_session, engine
):
    yadisk_kwargs = {"token": "mock"}
    local_file = constants["EXCEL_FILE"]
    download_kwargs = {
        "remote_filepath": "mock/path",
        "local_filepath": local_file.as_posix(),
    }
    bot = get_bot()
    msgloader = BirthdayMessageLoader(
        yadisk_kwargs, download_kwargs, bot, engine
    )
    await msgloader.load()
    assert "warning" in msgloader.message_store
