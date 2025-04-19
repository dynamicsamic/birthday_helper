import datetime as dt

import factory

from src.db import models

from .common import today

fake_future_dates = factory.Faker(
    "date_between",
    start_date=today() + dt.timedelta(days=1),
    end_date=today() + dt.timedelta(days=4),
)


class BirthdayFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = models.Birthday
        sqlalchemy_session = None

    name = factory.Sequence(lambda x: f"name{x}")
    date = factory.Faker(
        "date_between",
        start_date=today() - dt.timedelta(days=1),
        end_date=today() + dt.timedelta(days=4),
    )

    @classmethod
    def create_today_birthdays(cls, num: int) -> list[models.Birthday]:
        cls.create_batch(num, date=today())

    @classmethod
    def create_future_birthdays(cls, num: int) -> list[models.Birthday]:
        cls.create_batch(num, date=fake_future_dates)
