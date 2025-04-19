import datetime as dt
import logging
from logging.config import fileConfig
from typing import Any, Sequence, Type

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.utils import today as today_

from .shared import Base
from .shared import Session as session_

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class QueryManagerBase:
    """
    Class for performing data querying operations
    such as get_one, get_all etc.
    """

    def __init__(self, model: Type[Base]) -> None:
        self.model = model

    def get(
        self, session: Session, primary_key_value: int = None, **kwargs
    ) -> Type[Base]:
        """Fetch an instance of `self.model` with given attributes.\n
        Let's you specify two ways of fetching an instance:
        - `self.get(session, 1)` returns a model instance with primary_key == 1;
        - `self.get(session, name='name1')` returns a model instance
        with `self.model.name` == 'name1'
        """
        query = None
        if primary_key_value is not None and self.model.__mapper__.primary_key:
            query = session.get(self.model, primary_key_value)
        elif kwargs:
            try:
                query = session.scalar(select(self.model).filter_by(**kwargs))
            except SQLAlchemyError:
                pass
        return query

    def all(self, session: Session) -> list[Type[Base]]:
        """Fetch all instances of `model`."""
        return session.scalars(select(self.model)).all()

    def count(self, session: Session) -> int:
        """Count number of instances of `model` recorded in db."""
        return session.scalar(select(func.count(self.model.name)))


class DateQueryManager(QueryManagerBase):
    def last(self, session: Session) -> Type[Base]:
        """Fetch last added instance of `model`."""
        return session.scalar(
            select(self.model).order_by(self.model.date.desc()).limit(1)
        )

    def first(self, session: Session) -> Type[Base]:
        """Fetch first added instance of `model`."""
        return session.scalar(
            select(self.model).order_by(self.model.date).limit(1)
        )

    def between(
        self, session: Session, start: dt.date | str, end: dt.date | str
    ) -> list[Type[Base]]:
        """
        Fetch all instances of `model` which have
        `date` attribute between given date borders.

        Arguments for `start` and `end` may be passed as strings.
        In this case arguments must follow ISO format `yyyy-mm-dd'.
        If not, borders will be replaced with current year period.
        """
        if isinstance(start, str):
            try:
                start = dt.date.fromisoformat(start)
            except ValueError:
                start = dt.date.fromisoformat(f"{today_().year}-01-01")
        if isinstance(end, str):
            try:
                end = dt.date.fromisoformat(end)
            except ValueError:
                end = dt.date.fromisoformat(f"{today_().year}-12-31")

        return session.scalars(
            select(self.model)
            .filter(self.model.date.between(start, end))
            .order_by(self.model.date, self.model.name)
        ).all()

    def today(
        self, session: Session, today: dt.date = None
    ) -> list[Type[Base]]:
        """
        Fetch all instances of `model` which have
        `date` attribute equal to today.
        """
        today = today or today_()
        return self.between(session, today, today)

    def future(
        self, session: Session, today: dt.date = None, delta: int = 3
    ) -> list[Type[Base]]:
        """Fetch all instances of `model` from db
        which have `date` attribute between tomorrow and delta."""
        today = today or today_()
        start = today + dt.timedelta(days=1)
        end = today + dt.timedelta(days=delta)
        return self.between(session, start, end)

    def future_all(
        self, session: Session, today: dt.date = None
    ) -> list[Type[Base]]:
        """Fetch all instances of `model` from db
        which have `date` attribute greater than today."""
        today = today or today_()
        return session.scalars(
            select(self.model).filter(self.model.date > today)
        ).all()


class BirthdayManipulationManager:
    """
    Class for performing data manipulation operations
    such as create, update, delete.
    """

    def __init__(self, model: Type[Base]) -> None:
        self.model = model

    def refresh_table(
        self, mappings: Sequence[dict[str, Any]], session: Session = None
    ) -> int:
        """Insert model mappings into `self.model` table.
        Data existing in the table will be wiped out.

        This mehthod needs preliminary data validation.
        Use only pre-validated data for mappings.

        :param mappings: Sequence of `dict`s that implement
            mappings of values to model attributes.
        :param session: SQLAlchemy session to provide insert operations.

        :returns: Number of inserted table rows (0 if nothing was inserted)."""
        num_inserted = 0
        if session is None:
            session = session_
        session.query(self.model).delete()
        try:
            session.bulk_insert_mappings(self.model, mappings)
            session.commit()
            num_inserted = len(mappings)
        except SQLAlchemyError as e:
            logger.error(
                f"Refresh {self.model.__class__} table [FAILURE]!"
                f"Bulk refresh aborted with error: {e}"
            )
            session.rollback()
        finally:
            session.close()

        return num_inserted

    def bulk_save_objects(
        self, session: Session, birthdays: Sequence[Type[Base]]
    ) -> None:
        """Saves new 'model' instances to db."""
        session.bulk_save_objects(birthdays)

    def sqlite_upsert(
        self,
        session: Session,
        name: str,
        date: dt.date,
    ) -> None:
        """
        Insert new row into db. If such row already exists, update it.
        Uses `sqlite` specific syntax.
        WARNING: this method is too slow in bulk insert operations!
        """
        insert_stmt = sqlite_insert(self.model.__table__).values(
            name=name, date=date
        )
        on_duplicate_update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=("name", "date"),
            set_=dict(name=name, date=date),
        )
        session.execute(on_duplicate_update_stmt)

    def sqlite_insert_ignore_duplicate(
        self, session: Session, name: str, date: dt.date
    ) -> None:
        """
        Insert new row into db. If such row already exists, ignore it.
        Uses `sqlite` specific syntax.
        WARNING: this method is too slow in bulk insert operations!
        Although a bit faster than `upsert` method.
        """
        insert_stmt = sqlite_insert(self.model.__table__).values(
            name=name, date=date
        )
        do_nothing_stmt = insert_stmt.on_conflict_do_nothing(
            index_elements=("name", "date")
        )
        session.execute(do_nothing_stmt)
