import datetime as dt
from functools import cache
from typing import Any

from sqlalchemy import Date, String
from sqlalchemy.orm import Mapped, mapped_column

# from .managers import BirthdayManipulationManager, DateQueryManager
from .shared import Base


class Birthday(Base):
    __tablename__ = "birthday"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    date: Mapped[dt.date] = mapped_column(Date)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.id}, {self.name}, {self.date})"

    def to_dict(self) -> dict[str, Any]:
        """Returns dict of instance attributes."""
        return {"id": self.id, "name": self.name, "date": self.date}

    # @classmethod
    # @property
    # @cache
    # def queries(cls) -> DateQueryManager:
    #     """Setup query manager."""
    #     return DateQueryManager(cls)

    # @classmethod
    # @property
    # @cache
    # def operations(cls) -> BirthdayManipulationManager:
    #     """Setup data manipulation manager."""
    #     return BirthdayManipulationManager(cls)
