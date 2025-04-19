import datetime as dt
from time import sleep

import pytest

from src.utils import BirthdayStorage, is_fresh


def test_birthday_storage_skips_irrelevant_keys():
    store = BirthdayStorage()

    store["random"] = "random"
    store["invalid"] = "invalid"
    store["ts"] = "ts"

    assert len(store) == 0


def test_birthday_storage_sets_timestamp_only_for_message_keys_keys():
    store = BirthdayStorage()
    assert "ts" not in store

    store["random"] = "random"
    assert "ts" not in store

    tracked_keys = ("today", "future", "warning")

    for key in tracked_keys:
        store[key] = "hello world"
        assert "ts" in store
        store.clear()


def test_birthday_storage_is_empty_only_tracks_pure_birthday_keys():
    store = BirthdayStorage()
    assert store.is_empty() == True

    store["random"] = "random"
    assert store.is_empty() == True

    store["warning"] = "warning"
    assert store.is_empty() == True

    store["today"] = "hello world"
    assert store.is_empty() == False
    store.clear()

    store["future"] = "hello world"
    assert store.is_empty() == False


def test_birthday_storage_is_fresh_calculate_fresh_period():
    store = BirthdayStorage()
    assert store.is_fresh(seconds=1) == False

    store["today"] = "hello world"
    assert store.is_fresh(minutes=1) == True

    store["future"] = "hello world"
    sleep(5)
    assert store.is_fresh(seconds=4) == False


def test_birthday_storage_messages_return_values_for_specific_keys():
    store = BirthdayStorage()
    assert store.messages == []

    keys = ("today", "future", "warning", "random", "bar", "foo")

    for key in keys:
        store[key] = f"value_for{key}"

    assert len(store.messages) == 3

    expected = [f"value_for{key}" for key in ("warning", "today", "future")]
    assert store.messages == expected


def test_is_fresh_correctly_detect_fresh_data():
    now = dt.datetime.now().timestamp()
    assert is_fresh(now, {"minutes": 5}) == True

    sleep(5)
    assert is_fresh(now, {"seconds": 4}) == False


def test_is_fresh_return_false_for_invalid_args():
    now = dt.datetime.now().timestamp()

    assert is_fresh(now, {}) == False
    assert is_fresh(now, {"hello": "world"}) == False
    assert is_fresh(now, {"seconds": "20"}) == False
    assert is_fresh("now", {}) == False
