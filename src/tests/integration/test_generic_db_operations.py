import shutil
from matplotlib import pyplot as plt
import pytest
import warnings
from sql.telemetry import telemetry
from unittest.mock import ANY, Mock
import math

ALL_DATABASES = [
    "ip_with_postgreSQL",
    "ip_with_mySQL",
    "ip_with_mariaDB",
    "ip_with_SQLite",
    "ip_with_duckDB",
    "ip_with_MSSQL",
    "ip_with_Snowflake",
]


@pytest.fixture(autouse=True)
def run_around_tests(tmpdir_factory):
    # Create tmp folder
    my_tmpdir = tmpdir_factory.mktemp("tmp")
    yield my_tmpdir
    # Destory tmp folder
    shutil.rmtree(str(my_tmpdir))


@pytest.fixture
def mock_log_api(monkeypatch):
    mock_log_api = Mock()
    monkeypatch.setattr(telemetry, "log_api", mock_log_api)
    yield mock_log_api


# Query
@pytest.mark.parametrize(
    "ip_with_dynamic_db, expected",
    [
        ("ip_with_postgreSQL", 3),
        ("ip_with_mySQL", 3),
        ("ip_with_mariaDB", 3),
        ("ip_with_SQLite", 3),
        ("ip_with_duckDB", 3),
        ("ip_with_Snowflake", 3),
    ],
)
def test_query_count(ip_with_dynamic_db, expected, request, test_table_name_dict):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    out = ip_with_dynamic_db.run_line_magic(
        "sql", f"SELECT * FROM {test_table_name_dict['taxi']} LIMIT 3"
    )

    # Test query with --with & --save
    ip_with_dynamic_db.run_cell(
        f"%sql --save taxi_subset --no-execute SELECT * FROM\
          {test_table_name_dict['taxi']} LIMIT 3"
    )
    out_query_with_save_arg = ip_with_dynamic_db.run_cell(
        "%sql --with taxi_subset SELECT * FROM taxi_subset"
    )

    assert len(out) == expected
    assert len(out_query_with_save_arg.result) == expected
