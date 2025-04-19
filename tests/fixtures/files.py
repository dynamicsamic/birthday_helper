import os
from itertools import cycle

import pytest
from openpyxl import Workbook

from src import settings
from tests.common import constants

sample_size = constants["TEST_SAMPLE_SIZE"]

days = cycle(range(1, 29))
months = cycle(settings.MONTHS)
stored_excel_settings = {
    "payload": [
        [next(days), next(months), f"partner{i}"] for i in range(sample_size)
    ],
    "duplicate_rows": [
        [1, settings.MONTHS[0], "partner0"],
        [2, settings.MONTHS[1], "partner1"],
    ],
    "unfiltered_rows": [
        [150, settings.MONTHS[0], "new_partner"],
        [2, "any_month", "second_partner"],
        ["?", settings.MONTHS[0], "third_partner"],
    ],
    "untrimmed_rows": [
        [1, settings.MONTHS[0] + " ", "fourth_partner"],
        [2, " " + settings.MONTHS[1], "fifth_partner"],
    ],
}


@pytest.fixture
def stored_excel_file():
    wb = Workbook()
    ws = wb.active
    headers = list(settings.COLUMNS.keys())
    data = [
        sublist
        for list_ in stored_excel_settings.values()
        for sublist in list_
    ]
    for row in [headers] + data:
        ws.append(row)
    wb.save(constants["EXCEL_FILE"])

    yield wb

    os.remove(constants["EXCEL_FILE"])


@pytest.fixture
def temp_file():

    fpath = constants["TEMP_FILE"]
    with open(fpath, "w") as f:
        pass
    yield f
    os.remove(fpath)
