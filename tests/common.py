import datetime as dt

from src import settings

constants = {
    "TEST_SAMPLE_SIZE": 500,
    "TODAY_BDAY_NUM": 3,
    "FUTURE_BDAY_NUM": 5,
    "TEMP_FILE": settings.BASE_DIR / "test.xlsx",
    "EXCEL_FILE": settings.BASE_DIR / "tests.xlsx",
}
months = (
    "январь",
    "февраль",
    "март",
    "апрель",
    "май",
    "июнь",
    "июль",
    "август",
    "сентябрь",
    "октябрь",
    "ноябрь",
    "декабрь",
)


def today() -> dt.date:
    return dt.date.today()
