import sys
from unittest.mock import ANY, Mock, patch
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import sql.connection
from sql.connection import Connection, CustomConnection
from IPython.core.error import UsageError
import sqlglot
import sqlalchemy
import sqlite3


@pytest.fixture
def cleanup():
    yield
    Connection.connections = {}


@pytest.fixture
def mock_database(monkeypatch, cleanup):
    monkeypatch.setitem(sys.modules, "some_driver", Mock())
    monkeypatch.setattr(Engine, "connect", Mock())
    monkeypatch.setattr(sqlalchemy, "create_engine", Mock())


@pytest.fixture
def mock_postgres(monkeypatch, cleanup):
    monkeypatch.setitem(sys.modules, "psycopg2", Mock())
    monkeypatch.setattr(Engine, "connect", Mock())


def test_password_isnt_displayed(mock_postgres):
    Connection.from_connect_str("postgresql://user:topsecret@somedomain.com/db")

    assert "topsecret" not in Connection.connection_list()


def test_connection_name(mock_postgres):
    conn = Connection.from_connect_str("postgresql://user:topsecret@somedomain.com/db")

    assert conn.name == "user@db"


def test_alias(cleanup):
    Connection.from_connect_str("sqlite://", alias="some-alias")

    assert list(Connection.connections) == ["some-alias"]


def test_get_curr_sqlalchemy_connection_info():
    engine = create_engine("sqlite://")
    conn = Connection(engine=engine)

    assert conn._get_curr_sqlalchemy_connection_info() == {
        "dialect": "sqlite",
        "driver": "pysqlite",
        "server_version_info": ANY,
    }


def test_get_curr_sqlglot_dialect_no_curr_connection(mock_database, monkeypatch):
    conn = Connection(engine=sqlalchemy.create_engine("someurl://"))
    monkeypatch.setattr(conn, "_get_curr_sqlalchemy_connection_info", lambda: None)
    assert conn._get_curr_sqlglot_dialect() is None


@pytest.mark.parametrize(
    "sqlalchemy_connection_info, expected_sqlglot_dialect",
    [
        (
            {
                "dialect": "duckdb",
                "driver": "duckdb_engine",
                "server_version_info": [8, 0],
            },
            "duckdb",
        ),
        (
            {
                "dialect": "mysql",
                "driver": "pymysql",
                "server_version_info": [10, 10, 3, 10, 3],
            },
            "mysql",
        ),
        # sqlalchemy and sqlglot have different dialect name, test the mapping dict
        (
            {
                "dialect": "sqlalchemy_mock_dialect_name",
                "driver": "sqlalchemy_mock_driver_name",
                "server_version_info": [0],
            },
            "sqlglot_mock_dialect",
        ),
        (
            {
                "dialect": "only_support_in_sqlalchemy_dialect",
                "driver": "sqlalchemy_mock_driver_name",
                "server_version_info": [0],
            },
            "only_support_in_sqlalchemy_dialect",
        ),
    ],
)
def test_get_curr_sqlglot_dialect(
    monkeypatch, sqlalchemy_connection_info, expected_sqlglot_dialect, mock_database
):
    """To test if we can get the dialect name in sqlglot package scope

    Args:
        monkeypatch (fixture): A convenient fixture for monkey-patching
        sqlalchemy_connection_info (dict): The metadata about the current dialect
        expected_sqlglot_dialect (str): Expected sqlglot dialect name
    """
    conn = Connection(engine=sqlalchemy.create_engine("someurl://"))

    monkeypatch.setattr(
        conn,
        "_get_curr_sqlalchemy_connection_info",
        lambda: sqlalchemy_connection_info,
    )
    monkeypatch.setattr(
        sql.connection,
        "DIALECT_NAME_SQLALCHEMY_TO_SQLGLOT_MAPPING",
        {"sqlalchemy_mock_dialect_name": "sqlglot_mock_dialect"},
    )
    assert conn._get_curr_sqlglot_dialect() == expected_sqlglot_dialect


@pytest.mark.parametrize(
    "cur_dialect, expected_support_backtick",
    [
        ("mysql", True),
        ("sqlite", True),
        ("postgres", False),
    ],
)
def test_is_use_backtick_template(
    mock_database, cur_dialect, expected_support_backtick, monkeypatch
):
    """To test if we can get the backtick supportive information from different dialects

    Args:
        monkeypatch (fixture): A convenient fixture for monkey-patching
        cur_dialect (bool): Patched dialect name
        expected_support_backtick (bool): Excepted boolean value to indicate
        if the dialect supports backtick identifier
    """
    # conn = Connection(engine=create_engine(sqlalchemy_url))
    conn = Connection(engine=sqlalchemy.create_engine("someurl://"))
    monkeypatch.setattr(conn, "_get_curr_sqlglot_dialect", lambda: cur_dialect)
    assert conn.is_use_backtick_template() == expected_support_backtick


def test_is_use_backtick_template_sqlglot_missing_dialect_ValueError(
    mock_database, monkeypatch
):
    """Since accessing missing dialect will raise ValueError from sqlglot, we assume
    that's not support case
    """
    conn = Connection(engine=create_engine("sqlite://"))

    monkeypatch.setattr(
        conn, "_get_curr_sqlglot_dialect", lambda: "something_weird_dialect"
    )
    assert conn.is_use_backtick_template() is False


def test_is_use_backtick_template_sqlglot_missing_tokenizer_AttributeError(
    mock_database, monkeypatch
):
    """Since accessing the dialect without Tokenizer Class will raise AttributeError
    from sqlglot, we assume that's not support case
    """
    conn = Connection(engine=create_engine("sqlite://"))

    monkeypatch.setattr(conn, "_get_curr_sqlglot_dialect", lambda: "mysql")
    monkeypatch.setattr(sqlglot.Dialect.get_or_raise("mysql"), "Tokenizer", None)

    assert conn.is_use_backtick_template() is False


def test_is_use_backtick_template_sqlglot_missing_identifiers_TypeError(
    mock_database, monkeypatch
):
    """Since accessing the IDENTIFIERS list of the dialect's Tokenizer Class
    will raise TypeError from sqlglot, we assume that's not support case
    """
    conn = Connection(engine=create_engine("sqlite://"))

    monkeypatch.setattr(conn, "_get_curr_sqlglot_dialect", lambda: "mysql")
    monkeypatch.setattr(
        sqlglot.Dialect.get_or_raise("mysql").Tokenizer, "IDENTIFIERS", None
    )
    assert conn.is_use_backtick_template() is False


def test_is_use_backtick_template_sqlglot_empty_identifiers(mock_database, monkeypatch):
    """Since looking up the "`" symbol in IDENTIFIERS list of the dialect's
    Tokenizer Class will raise TypeError from sqlglot, we assume that's not support case
    """
    conn = Connection(engine=create_engine("sqlite://"))

    monkeypatch.setattr(conn, "_get_curr_sqlglot_dialect", lambda: "mysql")
    monkeypatch.setattr(
        sqlglot.Dialect.get_or_raise("mysql").Tokenizer, "IDENTIFIERS", []
    )
    assert conn.is_use_backtick_template() is False


# Mock the missing package
# Ref: https://stackoverflow.com/a/28361013
def test_missing_duckdb_dependencies(cleanup, monkeypatch):
    with patch.dict(sys.modules):
        sys.modules["duckdb"] = None
        sys.modules["duckdb-engine"] = None

        with pytest.raises(UsageError) as excinfo:
            Connection.from_connect_str("duckdb://")

        assert excinfo.value.error_type == "MissingPackageError"
        assert "try to install package: duckdb-engine" + str(excinfo.value)


@pytest.mark.parametrize(
    "missing_pkg, except_missing_pkg_suggestion, connect_str",
    [
        # MySQL + MariaDB
        ["pymysql", "pymysql", "mysql+pymysql://"],
        ["mysqlclient", "mysqlclient", "mysql+mysqldb://"],
        ["mariadb", "mariadb", "mariadb+mariadbconnector://"],
        ["mysql-connector-python", "mysql-connector-python", "mysql+mysqlconnector://"],
        ["asyncmy", "asyncmy", "mysql+asyncmy://"],
        ["aiomysql", "aiomysql", "mysql+aiomysql://"],
        ["cymysql", "cymysql", "mysql+cymysql://"],
        ["pyodbc", "pyodbc", "mysql+pyodbc://"],
        # PostgreSQL
        ["psycopg2", "psycopg2", "postgresql+psycopg2://"],
        ["psycopg", "psycopg", "postgresql+psycopg://"],
        ["pg8000", "pg8000", "postgresql+pg8000://"],
        ["asyncpg", "asyncpg", "postgresql+asyncpg://"],
        ["psycopg2cffi", "psycopg2cffi", "postgresql+psycopg2cffi://"],
        # Oracle
        ["cx_oracle", "cx_oracle", "oracle+cx_oracle://"],
        ["oracledb", "oracledb", "oracle+oracledb://"],
        # MSSQL
        ["pyodbc", "pyodbc", "mssql+pyodbc://"],
        ["pymssql", "pymssql", "mssql+pymssql://"],
    ],
)
def test_missing_driver(
    missing_pkg, except_missing_pkg_suggestion, connect_str, monkeypatch
):
    with patch.dict(sys.modules):
        sys.modules[missing_pkg] = None
        with pytest.raises(UsageError) as excinfo:
            Connection.from_connect_str(connect_str)

        assert excinfo.value.error_type == "MissingPackageError"
        assert "try to install package: " + missing_pkg in str(excinfo.value)


def test_no_current_connection_and_get_info(monkeypatch, mock_database):
    engine = create_engine("sqlite://")
    conn = Connection(engine=engine)

    monkeypatch.setattr(conn, "session", None)
    assert conn._get_curr_sqlalchemy_connection_info() is None


class dummy_connection:
    def __init__(self):
        self.engine_name = "dummy_engine"

    def close(self):
        pass


@pytest.mark.parametrize(
    "conn, expected",
    [
        [sqlite3.connect(""), True],
        [
            CustomConnection(engine=sqlalchemy.create_engine("sqlite://")),
            True,
        ],
        [
            Connection(engine=sqlalchemy.create_engine("sqlite://")),
            False,
        ],
        [dummy_connection(), False],
        ["not_a_valid_connection", False],
        [0, False],
    ],
    ids=[
        "sqlite3_connection",
        "custom_connection",
        "normal_connection",
        "dummy_connection",
        "str",
        "int",
    ],
)
def test_custom_connection(conn, expected):
    is_custom = Connection.is_custom_connection(conn)
    assert is_custom == expected
