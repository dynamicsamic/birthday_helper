import datetime as dt

import pytest
from sqlalchemy import select

from src.db.models import Birthday

from .common import constants, today
from .fixtures.db import (
    create_birthday_range,
    create_tables,
    create_test_data,
    db_session,
    engine,
)


def test_birthday_get_method_with_valid_id_returns_selected_model_instance(
    db_session, create_birthday_range
):
    valid_id = 1
    birthday = Birthday.queries.get(db_session, valid_id)
    assert isinstance(birthday, Birthday)
    assert birthday.id == valid_id


def test_birthday_get_method_with_invalid_id_returns_None(
    db_session, create_birthday_range
):
    invalid_id = -1
    birthday = Birthday.queries.get(db_session, invalid_id)
    assert birthday is None


def test_birthday_get_method_without_id_returns_None(
    db_session, create_birthday_range
):
    birthday = Birthday.queries.get(db_session)
    assert birthday is None


def test_birthday_get_method_with_one_valid_kwarg_returns_selected_instance(
    db_session, create_birthday_range
):
    valid_kwarg = {"name": "name1"}
    birthday = Birthday.queries.get(db_session, **valid_kwarg)
    assert isinstance(birthday, Birthday)
    assert birthday.name == valid_kwarg["name"]


def test_birthday_get_method_with_several_valid_kwargs_returns_selected_instance(
    db_session, create_birthday_range
):
    valid_kwargs = {"name": "name1", "date": today()}
    birthday = Birthday.queries.get(db_session, **valid_kwargs)
    assert isinstance(birthday, Birthday)
    assert birthday.name == valid_kwargs["name"]
    assert birthday.date == today()


def test_birthday_get_method_with_one_invalid_kwarg_returns_None(
    db_session, create_birthday_range
):
    invalid_kwargs = {"name": "-1", "date": today()}
    birthday = Birthday.queries.get(db_session, **invalid_kwargs)
    assert birthday is None


def test_birthday_get_method_with_invalid_field_name_returns_None(
    db_session, create_birthday_range
):
    invalid_kwargs = {"invalid_field": "-1", "second_invalid_field": 11}
    birthday = Birthday.queries.get(db_session, **invalid_kwargs)
    assert birthday is None


def test_birthday_get_method_without_arguments_returns_None(
    db_session, create_birthday_range
):
    birthday = Birthday.queries.get(db_session)
    assert birthday is None


@pytest.mark.current
def test_birthday_get_method_with_valid_name_returns_model_instance(
    db_session, create_birthday_range
):
    valid_name = "name1"
    birthday = Birthday.queries.get(db_session, name=valid_name)
    assert isinstance(birthday, Birthday)


def test_birthday_get_method_with_invalid_name_returns_none(
    db_session, create_test_data
):
    invalid_name = "invalid"
    birthday = Birthday.queries.get(db_session, invalid_name)
    assert birthday is None


def test_birthday_all_method_returns_list_of_all_model_instances(
    db_session, create_test_data
):
    expected_birthdays = db_session.scalars(select(Birthday))
    birthdays = Birthday.queries.all(db_session)
    assert isinstance(birthdays, list)
    for expected_bday, bday in zip(expected_birthdays, birthdays):
        assert expected_bday is bday


def test_birthday_count_method_returns_number_of_all_model_instances(
    db_session, create_test_data
):
    expected_birthday_num = (
        constants["TODAY_BDAY_NUM"] + constants["FUTURE_BDAY_NUM"]
    )
    birthday_num = Birthday.queries.count(db_session)
    assert isinstance(birthday_num, int)
    assert birthday_num == expected_birthday_num


def test_birthday_last_method_returns_instance_with_latest_birth_date(
    db_session, create_test_data
):
    birthdays = Birthday.queries.all(db_session)
    latest_date = today()
    for birthday in birthdays:
        if birthday.date > latest_date:
            latest_date = birthday.date

    latest_birthday = Birthday.queries.last(db_session)
    assert isinstance(latest_birthday, Birthday)
    assert latest_birthday.date == latest_date


def test_birthday_first_method_returns_instance_with_earliest_birth_date(
    db_session, create_test_data
):
    birthdays = Birthday.queries.all(db_session)
    earliest_date = dt.date(year=2030, month=12, day=31)
    for birthday in birthdays:
        if birthday.date < earliest_date:
            earliest_date = birthday.date

    first_birthday = Birthday.queries.first(db_session)
    assert isinstance(first_birthday, Birthday)
    assert first_birthday.date == earliest_date


def test_birthday_between_method_with_valid_dates_returns_list_of_model_instances(
    db_session, create_test_data
):
    today_ = today()
    birthdays = Birthday.queries.between(db_session, today_, today_)
    assert isinstance(birthdays, list)
    assert len(birthdays) == constants["TODAY_BDAY_NUM"]


def test_birthday_between_method_with_valid_string_dates_returns_list_of_model_instances(
    db_session, create_test_data
):
    today_ = today()
    birthdays = Birthday.queries.between(
        db_session, today_.isoformat(), today_.isoformat()
    )
    assert isinstance(birthdays, list)
    assert len(birthdays) == constants["TODAY_BDAY_NUM"]


def test_birthday_between_method_with_invalid_string_dates_returns_list_of_current_year_model_instances(
    db_session, create_test_data
):
    # prepartion stage: add random last year birthday
    last_year_date = dt.date(year=today().year - 1, month=7, day=1)
    last_year_birthday = Birthday(name="valid", date=last_year_date)
    db_session.add(last_year_birthday)
    db_session.commit()

    invalid_start = "202-1-1"
    invalid_end = "1999-15-3"
    birthdays = Birthday.queries.between(
        db_session, invalid_start, invalid_end
    )
    birthday_num = Birthday.queries.count(db_session)
    assert len(birthdays) == birthday_num - 1
    assert all(birthday.date > last_year_date for birthday in birthdays)


def test_birthday_today_method_returns_list_of_instances_with_current_date(
    db_session, create_test_data
):
    today_birthdays = Birthday.queries.today(db_session)
    assert isinstance(today_birthdays, list)
    assert len(today_birthdays) == constants["TODAY_BDAY_NUM"]


def test_birthday_future_method_returns_list_of_instaces_with_date_between_tomorrow_and_delta(
    db_session, create_test_data
):
    # prepartion stage: add random next year birthday
    next_year_date = dt.date(year=today().year + 1, month=7, day=1)
    next_year_birthday = Birthday(name="valid", date=next_year_date)
    db_session.add(next_year_birthday)
    db_session.commit()

    future_birthdays = Birthday.queries.future(db_session)
    assert isinstance(future_birthdays, list)
    assert len(future_birthdays) == constants["FUTURE_BDAY_NUM"]


def test_birthday_future_method_returns_ordered_result(db_session):
    import random

    tommorow = today() + dt.timedelta(days=1)
    overmorrow = today() + dt.timedelta(days=2)
    dates = [today(), tommorow, overmorrow]
    mappings = [
        {"name": f"partner{i}", "date": random.choice(dates)}
        for i in range(10)
    ]
    Birthday.operations.refresh_table(mappings, db_session)

    future_birthdays = Birthday.queries.future(db_session)

    for i in range(len(future_birthdays) - 1):
        assert future_birthdays[i].date <= future_birthdays[i + 1].date


def test_birthday_future_all_method_returns_list_of_instaces_with_date_greater_than_today(
    db_session, create_test_data
):
    # prepartion stage: add random next year birthday
    next_year_date = dt.date(year=today().year + 1, month=7, day=1)
    next_year_birthday = Birthday(name="valid", date=next_year_date)
    db_session.add(next_year_birthday)
    db_session.commit()

    future_birthdays = Birthday.queries.future_all(db_session)
    assert isinstance(future_birthdays, list)
    assert len(future_birthdays) == constants["FUTURE_BDAY_NUM"] + 1


def test_birthday_refresh_table_method_deletes_all_rows_and_populates_db_again(
    db_session, create_birthday_range
):
    initial_birthday_num = Birthday.queries.count(db_session)
    assert (
        initial_birthday_num == constants["TEST_SAMPLE_SIZE"]
    )  # number of birthdays created

    num_inserted = Birthday.operations.refresh_table(
        [{"name": "valid", "date": today()}], db_session
    )

    assert num_inserted == 1
    assert Birthday.queries.count(db_session) == 1


def test_birthday_refresh_table_returns_zero_with_invalid_mappings(
    db_session, create_birthday_range
):
    num_inserted = Birthday.operations.refresh_table(
        [{"invalid": "invalid", "invalid_date": today()}], db_session
    )

    assert num_inserted == 0
    assert Birthday.queries.count(db_session) == 0


def test_birthday_refresh_table_returns_zero_with_empty_mappings(db_session):
    num_inserted = Birthday.operations.refresh_table([], db_session)

    assert num_inserted == 0
    assert Birthday.queries.count(db_session) == 0


@pytest.mark.skip
def test_birthday_bulk_save_objects_saves_new_instances_to_db(db_session):
    objects_num_to_be_created = 400
    names = [f"name{i}" for i in range(objects_num_to_be_created)]
    date = today()
    birthdays = [Birthday(name=name, date=date) for name in names]

    initial_birthday_num = Birthday.queries.count(db_session)

    db_session.bulk_save_objects(birthdays)
    db_session.commit()

    current_birthday_num = Birthday.queries.count(db_session)
    assert (
        current_birthday_num
        == initial_birthday_num + objects_num_to_be_created
    )


@pytest.mark.skip
def test_birthday_sqlite_upsert_method_with_valid_data_saves_instance_to_db(
    db_session,
):
    initial_birthday_num = Birthday.queries.count(db_session)
    Birthday.operations.sqlite_upsert(db_session, "valid_name", today())
    current_birthday_num = Birthday.queries.count(db_session)
    assert current_birthday_num == initial_birthday_num + 1
    inserted_obj = Birthday.queries.get(db_session, "valid_name")
    assert inserted_obj.date == today()


@pytest.mark.skip
def test_birthday_sqlite_upsert_method_updates_instance_instead_of_creating(
    db_session,
):
    Birthday.operations.sqlite_upsert(db_session, "valid_name", today())
    initial_birthday_num = Birthday.queries.count(db_session)

    Birthday.operations.sqlite_upsert(db_session, "valid_name", today())
    current_birthday_num = Birthday.queries.count(db_session)

    assert current_birthday_num == initial_birthday_num
