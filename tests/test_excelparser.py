import datetime as dt
import random
from io import BytesIO

import pandas as pd
import pytest
from numpy import int32, int64

from src import settings
from src.toolbox.birthdays.excelparser import (
    ExcelParser,
    df_row_to_birthday_mapping,
)

from .common import constants, months

# sample_size = constants["TEST_SAMPLE_SIZE"]


# valid_data = {
#     "Дата": [random.randint(1, 28) for _ in range(sample_size)],
#     "месяц": [random.choice(months) for _ in range(sample_size)],
#     "ФИО": [fake.name() for _ in range(sample_size)],
# }

sample_size = 12
dates_ = [i for i in range(1, sample_size + 1)]
months_ = [settings.MONTHS[i] for i in range(sample_size)]
partners_ = [f"partner{i}" for i in range(1, sample_size + 1)]

valid_data = {
    "Дата": dates_,
    "месяц": months_,
    "ФИО": partners_,
}

extra_column_data = valid_data.copy()
extra_column_data["возраст"] = [
    random.randint(18, 80) for _ in range(sample_size)
]
not_enough_column_data = valid_data.copy()
not_enough_column_data.pop("ФИО")

string_instead_of_numeric_data = valid_data.copy()
string_instead_of_numeric_data["Дата"] = [
    f"-{i}" for i in range(1, sample_size + 1)
]


valid_dataframe = pd.DataFrame(valid_data)
extra_column_dataframe = pd.DataFrame(extra_column_data)
not_enough_column_dataframe = pd.DataFrame(not_enough_column_data)
string_instead_of_numeric_dataframe = pd.DataFrame(
    string_instead_of_numeric_data
)


def create_inmemory_excel_file(df: pd.DataFrame) -> BytesIO:
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine="openpyxl")
    df.to_excel(writer, sheet_name="Sheet1", index=False)
    writer.close()
    output.seek(0)
    return output


@pytest.fixture
def valid_excel_file():
    return create_inmemory_excel_file(valid_dataframe)


@pytest.fixture
def extra_column_excel_file():
    return create_inmemory_excel_file(extra_column_dataframe)


@pytest.fixture
def not_enough_column_excel_file():
    return create_inmemory_excel_file(not_enough_column_dataframe)


@pytest.fixture
def string_instead_of_numeric_data_excel_file():
    return create_inmemory_excel_file(string_instead_of_numeric_dataframe)


def test_df_row_to_birthday_mapping_returns_valid_mapping_for_valid_row():
    valid_row = pd.Series(
        {"Дата": int64(1), "месяц": "июнь", "ФИО": "Александр Иванов"}
    )
    date = dt.date(dt.date.today().year, 6, 1)
    expected = {"name": "Александр Иванов", "date": date}
    result = df_row_to_birthday_mapping(valid_row)

    assert result == expected


def test_excel_parser_read_excel_with_valid_data_returns_expected_dataframe(
    valid_excel_file,
):
    result_df = ExcelParser(
        valid_excel_file, columns=settings.COLUMNS
    ).read_excel()
    pd.testing.assert_frame_equal(result_df, valid_dataframe)


def test_excel_parser_read_excel_ignores_extra_rows(
    extra_column_excel_file,
):
    result_df = ExcelParser(
        extra_column_excel_file, columns=settings.COLUMNS
    ).read_excel()
    pd.testing.assert_frame_equal(result_df, valid_dataframe)


def test_excel_parser_read_excel_includes_only_selected_columns(
    extra_column_excel_file,
):
    result_df = ExcelParser(
        extra_column_excel_file, columns=settings.COLUMNS
    ).read_excel()
    pd.testing.assert_frame_equal(result_df, valid_dataframe)


def test_excel_parser_read_excel_includes_all_columns_if_none_selected(
    extra_column_excel_file,
):
    result_df = ExcelParser(extra_column_excel_file).read_excel()
    pd.testing.assert_frame_equal(result_df, extra_column_dataframe)


def test_excel_parser_read_excel_inserts_empty_columns(valid_excel_file):
    invalid_column_name = "invalid"
    result_df = ExcelParser(
        valid_excel_file, columns={invalid_column_name: str}
    ).read_excel()
    assert result_df[invalid_column_name].isnull().all()


@pytest.mark.xfail(raises=FileNotFoundError, strict=True)
def test_excel_parser_read_excel_with_invalid_path_raises_error():
    ExcelParser("invalid_path").read_excel()


@pytest.mark.xfail(strict=True)
def test_excel_parser_read_excel_with_wrong_file_type_raises_error():
    invalid_file_mock = BytesIO()
    ExcelParser(invalid_file_mock).read_excel()


@pytest.mark.xfail(raises=pd.errors.EmptyDataError, strict=True)
def test_excel_parser_check_empty_columns_raises_error(valid_excel_file):
    columns = settings.COLUMNS | {"new_empty_col": str}
    parser = ExcelParser(valid_excel_file, columns=columns)
    parser.check_empty_columns(parser.read_excel())


def test_excel_parser_cast_numeric_converts_str_column_to_int(
    string_instead_of_numeric_data_excel_file,
):
    parser = ExcelParser(
        string_instead_of_numeric_data_excel_file, columns=settings.COLUMNS
    )
    result_df = parser.cast_numeric(parser.read_excel())
    column_type = result_df["Дата"].dtype.type
    assert issubclass(column_type, (int64, int32))


def test_excel_parser_check_duplicates_drops_duplicate_row(valid_excel_file):
    parser = ExcelParser(
        valid_excel_file, columns=settings.COLUMNS, unique_fields=(("ФИО",))
    )
    df = parser.read_excel()
    duplicate_row = {"Дата": 1, "месяц": settings.MONTHS[0], "ФИО": "partner1"}

    df = df.append(duplicate_row, ignore_index=True)
    assert len(df.index) == sample_size + 1

    deduped_df = parser.check_unique(df)
    assert len(deduped_df.index) == sample_size


def test_excel_parser_filter_drops_specified_rows(valid_excel_file):
    parser = ExcelParser(
        valid_excel_file, columns=settings.COLUMNS, filter_set={"Дата": [">1"]}
    )
    df = parser.filter(parser.read_excel())
    assert len(df.index) == sample_size - 1


def test_excel_parser_filter_works_with_several_constraints(valid_excel_file):
    parser = ExcelParser(
        valid_excel_file,
        columns=settings.COLUMNS,
        filter_set={"Дата": [">1", "<10"], "ФИО": ["!='partner5'"]},
    )
    num_filtered = 5  # Does not include partner1, 5, 10, 11, 12
    df = parser.filter(parser.read_excel())
    assert len(df.index) == sample_size - num_filtered


@pytest.mark.xfail(strict=True)
def test_excel_parser_filter_with_invalid_clause_raises_error(
    valid_excel_file,
):
    parser = ExcelParser(
        valid_excel_file,
        columns=settings.COLUMNS,
        filter_set={"Дата": "invalid", "ФИО": "invalid"},
    )
    parser.filter(parser.read_excel())


def test_excel_parser_sort_provides_dataframe_numeric_sorting(
    valid_excel_file,
):
    parser = ExcelParser(
        valid_excel_file,
        columns=settings.COLUMNS,
        sort_by=[
            "Дата",
        ],
    )
    sorted_df = parser.sort(parser.read_excel())
    assert sorted_df["Дата"].to_list() == sorted(dates_)

    reversed_df = parser.sort(parser.read_excel(), ascending=False)
    assert reversed_df["Дата"].to_list() == sorted(dates_, reverse=True)


def test_excel_parser_sort_provides_dataframe_lexical_sorting(
    valid_excel_file,
):
    parser = ExcelParser(
        valid_excel_file,
        columns=settings.COLUMNS,
        sort_by=[
            "месяц",
        ],
    )

    sorted_df = parser.sort(parser.read_excel())
    assert sorted_df["месяц"].to_list() == sorted(months_)

    reversed_df = parser.sort(parser.read_excel(), ascending=False)
    assert reversed_df["месяц"].to_list() == sorted(months_, reverse=True)


@pytest.mark.xfail(strict=True)
def test_excel_parser_sort_with_invalid_args_raises_error(valid_excel_file):
    parser = ExcelParser(
        valid_excel_file,
        columns=settings.COLUMNS,
        sort_by=[
            "invalid_column",
        ],
    )
    parser.sort(parser.read_excel(), ascending=False)


def test_excel_parser_to_model_mappings_with_valid_df_returns_list_of_mappings(
    valid_excel_file,
):
    expected = [
        df_row_to_birthday_mapping((date, month, partner))
        for date, month, partner in zip(*valid_data.values())
    ]
    parser = ExcelParser(valid_excel_file)
    result_mappings = parser.to_model_mappings(
        parser.read_excel(), df_row_to_birthday_mapping
    )

    assert result_mappings == expected


def test_excel_parser_to_model_mappings_skips_invalid_row(valid_excel_file):
    invalid_row = ["invalid", "invalid", "True"]
    parser = ExcelParser(valid_excel_file)
    df = parser.read_excel()

    df.loc[sample_size] = invalid_row
    df.loc[sample_size + 1] = invalid_row
    result_mappings = parser.to_model_mappings(df, df_row_to_birthday_mapping)
    assert len(result_mappings) == sample_size


def test_excel_parser_to_model_mappings_for_all_invalid_rows_returns_empty_list(
    valid_excel_file,
):
    invalid_row = ["invalid", "invalid", "True"]
    parser = ExcelParser(valid_excel_file)
    df = parser.read_excel()

    df.loc[:] = invalid_row
    result_mappings = parser.to_model_mappings(df, df_row_to_birthday_mapping)
    assert result_mappings == []


# @pytest.mark.current
def test_excel_parser_run_returns_mappings(valid_excel_file):
    output_file = settings.BASE_DIR / settings.OUTPUT_FILE_NAME
    parser = ExcelParser(
        output_file,
        columns=settings.COLUMNS,
        unique_fields=("ФИО",),
        sort_by=["месяц", "Дата"],
        filter_set={
            "Дата": ["> 0", "< 32"],
            "месяц": [f"in {settings.MONTHS}"],
        },
    )
    df = parser.read_excel()

    res = parser.run(df_row_to_birthday_mapping)
    print(len(res))
    for i in res:
        if i["name"] == "Баженов Дмитрий":
            print("ok")


import re
