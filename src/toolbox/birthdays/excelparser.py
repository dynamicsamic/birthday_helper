import dataclasses
import datetime as dt
import logging
from logging.config import fileConfig
from typing import Any, Callable, Sequence

import numpy as np
import pandas as pd
from openpyxl import Workbook, load_workbook
from pandas import ExcelFile

from src import settings
from src.utils import is_fresh

from .messageformat import convert_month

fileConfig(fname="log_config.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ExcelParser:
    """
    Parse contents of excel file, convert contents into model mappings.
        `file_path` param is positional.
        All other params are `keyword` only.

    :param file_path: Any file-like entity that can be
        handled by `pandas.read_excel` function.
    :param columns: Excel columns to be inclueded in resulting dataframe.
        default: `None` - all columns existing in excel file will be included.
    :param unique_fields: Sequence of column names which should be
        checked for duplicate rows.
        default: 'None' - no checks will be performed.
    :param sort_by: List of column names that dataframe should be sorted by.
        default: `None` - no sorting will be performed.
    :param filter_set: Mappings `column_name` to `filter_clause`
    used to filter out undesirable results.
        default: `None` - no filtering will be performed.
            `Example`: {'columnA': ['>0', '<32']} means that `columnA` values
            must be greater that `0` and less than `32`.
    :param _model_mappings: Stores results of parsing excel file.
        initial state: empty list [].
    """

    file_path: str | bytes | ExcelFile | Workbook
    _: dataclasses.KW_ONLY
    columns: dict[str, type] = None
    unique_fields: Sequence[str] = None
    sort_by: list[str] = None
    filter_set: dict[str, Sequence[str]] = None
    _model_mappings: Sequence[dict[str, Any]] = None

    @property
    def model_mappings(self) -> list[dict[str, Any]]:
        if not self._model_mappings:
            self._model_mappings = []

        return self._model_mappings

    def run(
        self, model_mapper: Callable[[pd.Series], dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Glue togther all methods in a pipeline.

        :param model_mapper: Function that recieves one dataframe row
        and converts it into model mapping.

        :returns: List of model mappings or empty list if error occured.

        Attention: raises no exceptions.
        """
        # Clear previous mappings.
        self.model_mappings.clear()
        try:
            df = self.read_excel()
        except Exception as e:
            logger.error(f"read excel [FAILURE!]: {e}")
            return self.model_mappings
        try:
            (
                df.pipe(self.check_empty_columns)
                .pipe(self.cast_numeric)
                .pipe(self.check_unique)
                .pipe(self.filter)
                .pipe(self.sort)
                .pipe(self.to_model_mappings, model_mapper)
            )
        except Exception as e:
            logger.error(f"Exception occured during parsing excel file: {e}")
        return self.model_mappings

    def read_excel(self) -> pd.DataFrame:
        """Read excel file with pandas and handle errors.

        :returns: Instance of `pandas.DataFrame' for further processing.

        :raises: FileNotFoundError if could not locate the file.
        """
        # columns = self.columns if self.columns is None else self.columns.keys()
        if self.columns is None:
            columns = None
            converters = None
        else:
            columns = self.columns.keys()
            converters = {
                col: str.strip
                for col, type_ in self.columns.items()
                if type_.__name__ == "str"
            }

        try:
            df = pd.DataFrame(
                pd.read_excel(
                    self.file_path,
                    engine="openpyxl",
                    converters=converters,
                ),
                columns=columns,
            )
        except FileNotFoundError as e:
            logger.error(f"ExcelParser <read_excel> no such file: {e}")
            raise
        except Exception as e:
            logger.error(f"ExcelParser <read_excel> [FAILURE!]: {e}")
            raise
        logger.info("ExcelParser <read_excel> [SUCESS!]")
        return df

    def check_empty_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect columns with all null values.

        :returns: Instance of `pandas.DataFrame' for further processing.

        :raises: `pandas.errors.EmptyDataError`.
        """
        for column in self.columns:
            if getattr(df, column).isnull().all():
                logger.error(
                    "ExcelParser <check_empty_columns> [FAILURE!]: "
                    f"`{column}` column empty."
                )
                raise pd.errors.EmptyDataError(
                    f"Column `{column}` empty! Empty columns are not allowed!"
                )
        return df

    def cast_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all columns defined in `self.columns` which have numeric
        data types coerced to specified type and invalid values dropped.
        """
        for column, type_ in self.columns.items():
            if type_.__name__ in ("int", "float"):
                try:
                    df[column].replace(
                        "[^0-9]", np.nan, regex=True, inplace=True
                    )
                    df.dropna(inplace=True)
                    df[column] = df[column].astype(type_, copy=True)
                except Exception as e:
                    logger.error(f"ExcelParser <cast_numeric> [FAILURE!]: {e}")
                    raise
        return df

    def check_unique(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop duplicates for specified columns."""
        if self.unique_fields:
            for field in self.unique_fields:
                if hasattr(df, field):
                    df = df.drop_duplicates((field,))
        return df

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter values with specifed constaints."""
        if self.filter_set:
            for field, filter_clause in self.filter_set.items():
                expr = self._build_query_expression(field, filter_clause)
                try:
                    df = df.query(expr)
                except Exception as e:
                    logger.error(
                        f"ExcelParser <filter> [FAILURE] for field: {field}: {e}"
                    )
                    raise
        return df

    def sort(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Sort reulst in specified order."""
        if self.sort_by:
            try:
                return df.sort_values(self.sort_by, **kwargs)
            except Exception as e:
                logger.error(f"ExcelParser <sort> [FAILURE!]: {e}")
                raise
        return df

    def to_model_mappings(
        self,
        df: pd.DataFrame,
        model_mapper: Callable[[pd.Series], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Convert dataframe row into a sequence of model mappings.

        :param model_mapper: Function that recieves one dataframe row
        and converts it into model mapping (dict with str keys).

        :returns: List of model mappings.
        """
        mappings = self.model_mappings
        for i in df.index:
            try:
                model_mapping = model_mapper(df.loc[i])
            except Exception as e:
                logger.warning(
                    f"ExcelParser <convert_to model_mappings> [FAILURE]: {e}. "
                    f"Skipped row No: {i}"
                )
                continue
            mappings.append(model_mapping)
        return mappings

    @staticmethod
    def _build_query_expression(
        field_name: str, filter_clause: Sequence[str]
    ) -> str:
        """Compose string constraints to be passed
        to 'pandas.DataFrame.query` method.

        :param filter_clause: Sequence of short expressions, like
            `>10', `<50`, `in [1,2,3,] etc.

        :returns: Composed filter clause.
        """
        expression_parts = [
            f"{field_name} {value}"  # Last part of expression don't need `and`
            if i == len(filter_clause) - 1
            else f"{field_name} {value} and "
            for i, value in enumerate(filter_clause)
        ]

        return "".join(expression_parts)


def df_row_to_birthday_mapping(row: pd.Series) -> dict[str, Any]:
    """Convert dataframe row into models.Birthday mapping.
    A type of `mapper` to be used in `ExcelParser.to_model_mappings`.

    :param row: An instance of `pandas.Series` mainly.
        Though any iterable will work.

    :returns: A dictionary with data for creating a `models.Birthday` instance.
    """
    day, month, name = row
    month = month.lower().strip()
    name = name.strip()
    birth_date = dt.date(dt.date.today().year, convert_month(month), day)
    return {"name": name, "date": birth_date}


def backup_excel_workbook(
    wb: Workbook, backup_dirname: str = "excel_backup", **fresh_period
) -> None:
    """Backup excel workbook before updating.

    :param wb: Instance of `openpyxl.Workbook`
    :param backup_dirname: Name of backup directory
    :param fresh_period: Keyword argument valid for
        creating an instance of `datetime.timedelta`

    :returns: None.
    """
    now = dt.datetime.now().astimezone(settings.TIME_ZONE)
    backup_filename = f"birthdays_{now:%Y_%m_%d_%H_%M_%S}.xlsx"
    backup_dir = settings.BASE_DIR / backup_dirname
    backup_filepath = backup_dir / backup_filename
    wb.save(backup_filepath.as_posix())

    # Delete old files from backup folder.
    for file in backup_dir.iterdir():
        if not is_fresh(file.lstat().st_ctime, fresh_period):
            file.unlink()


def append_excel(filename: str | bytes, row: list[str | int]) -> bool:
    """Add new data to excel workbook.

    :param filename: Path to local excel file or file-like object
    :param row: List of data to be appended to the workbook.

    :returns: Boolean result of append operation:
        `True` if no exception raised, `False` otherwise.
    """
    try:
        wb = load_workbook(filename)
    except Exception as e:
        logger.error(f"<append_excel> create workbook [FAILURE!]: {e}")
        return False

    # Create a backup copy.
    try:
        backup_excel_workbook(wb, hours=1)
    except Exception as e:
        logger.error(f"<append excel> backup workbook [FAILURE!]: {e}")

    ws = wb.active
    try:
        ws.append(row)
    except Exception as e:
        logger.error(f"<append_excel> update worksheet [FAILURE!]: {e}")
        return False

    wb.save(filename)
    wb.close()
    return True
