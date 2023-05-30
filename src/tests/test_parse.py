import os
from pathlib import Path


import pytest

from sql.parse import (
    connection_from_dsn_section,
    parse,
    without_sql_comment,
    magic_args,
)

try:
    from traitlets.config.configurable import Configurable
except ImportError:
    from IPython.config.configurable import Configurable

empty_config = Configurable()
default_connect_args = {"options": "-csearch_path=test"}


def test_parse_no_sql():
    assert parse("will:longliveliz@localhost/shakes", empty_config) == {
        "connection": "will:longliveliz@localhost/shakes",
        "sql": "",
        "result_var": None,
        "return_result_var": False,
    }


def test_parse_with_sql():
    assert parse(
        "postgresql://will:longliveliz@localhost/shakes SELECT * FROM work",
        empty_config,
    ) == {
        "connection": "postgresql://will:longliveliz@localhost/shakes",
        "sql": "SELECT * FROM work",
        "result_var": None,
        "return_result_var": False,
    }


def test_parse_sql_only():
    assert parse("SELECT * FROM work", empty_config) == {
        "connection": "",
        "sql": "SELECT * FROM work",
        "result_var": None,
        "return_result_var": False,
    }


def test_parse_postgresql_socket_connection():
    assert parse("postgresql:///shakes SELECT * FROM work", empty_config) == {
        "connection": "postgresql:///shakes",
        "sql": "SELECT * FROM work",
        "result_var": None,
        "return_result_var": False,
    }


def test_expand_environment_variables_in_connection():
    os.environ["DATABASE_URL"] = "postgresql:///shakes"
    assert parse("$DATABASE_URL SELECT * FROM work", empty_config) == {
        "connection": "postgresql:///shakes",
        "sql": "SELECT * FROM work",
        "result_var": None,
        "return_result_var": False,
    }


def test_parse_shovel_operator():
    assert parse("dest << SELECT * FROM work", empty_config) == {
        "connection": "",
        "sql": "SELECT * FROM work",
        "result_var": "dest",
        "return_result_var": False,
    }


def test_parse_return_shovel_operator():
    assert parse("dest= << SELECT * FROM work", empty_config) == {
        "connection": "",
        "sql": "SELECT * FROM work",
        "result_var": "dest",
        "return_result_var": True,
    }


def test_parse_connect_plus_shovel():
    assert parse("sqlite:// dest << SELECT * FROM work", empty_config) == {
        "connection": "sqlite://",
        "sql": "SELECT * FROM work",
        "result_var": "dest",
        "return_result_var": False,
    }


def test_parse_early_newlines():
    assert parse("--comment\nSELECT *\n--comment\nFROM work", empty_config) == {
        "connection": "",
        "sql": "--comment\nSELECT *\n--comment\nFROM work",
        "result_var": None,
        "return_result_var": False,
    }


def test_parse_connect_shovel_over_newlines():
    assert parse("\nsqlite://\ndest\n<<\nSELECT *\nFROM work", empty_config) == {
        "connection": "sqlite://",
        "sql": "SELECT *\nFROM work",
        "result_var": "dest",
        "return_result_var": False,
    }


class DummyConfig:
    dsn_filename = Path("src/tests/test_dsn_config.ini")


def test_connection_from_dsn_section():
    result = connection_from_dsn_section(section="DB_CONFIG_1", config=DummyConfig())
    assert result == "postgres://goesto11:seentheelephant@my.remote.host:5432/pgmain"
    result = connection_from_dsn_section(section="DB_CONFIG_2", config=DummyConfig())
    assert result == "mysql://thefin:fishputsfishonthetable@127.0.0.1/dolfin"


class Bunch:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class ParserStub:
    opstrs = [
        [],
        ["-l", "--connections"],
        ["-x", "--close"],
        ["-c", "--creator"],
        ["-s", "--section"],
        ["-p", "--persist"],
        ["--append"],
        ["-a", "--connection_arguments"],
        ["-f", "--file"],
    ]
    _actions = [Bunch(option_strings=o) for o in opstrs]


parser_stub = ParserStub()


def test_without_sql_comment_plain():
    line = "SELECT * FROM author"
    assert without_sql_comment(parser=parser_stub, line=line) == line


def test_without_sql_comment_with_arg():
    line = "--file moo.txt --persist SELECT * FROM author"
    assert without_sql_comment(parser=parser_stub, line=line) == line


def test_without_sql_comment_with_comment():
    line = "SELECT * FROM author -- uff da"
    expected = "SELECT * FROM author"
    assert without_sql_comment(parser=parser_stub, line=line) == expected


def test_without_sql_comment_with_arg_and_comment():
    line = "--file moo.txt --persist SELECT * FROM author -- uff da"
    expected = "--file moo.txt --persist SELECT * FROM author"
    assert without_sql_comment(parser=parser_stub, line=line) == expected


def test_without_sql_comment_unspaced_comment():
    line = "SELECT * FROM author --uff da"
    expected = "SELECT * FROM author"
    assert without_sql_comment(parser=parser_stub, line=line) == expected


def test_without_sql_comment_dashes_in_string():
    line = "SELECT '--very --confusing' FROM author -- uff da"
    expected = "SELECT '--very --confusing' FROM author"
    assert without_sql_comment(parser=parser_stub, line=line) == expected


def test_without_sql_comment_with_arg_and_leading_comment():
    line = "--file moo.txt --persist --comment, not arg"
    expected = "--file moo.txt --persist"
    assert without_sql_comment(parser=parser_stub, line=line) == expected


def test_without_sql_persist():
    line = "--persist my_table --uff da"
    expected = "--persist my_table"
    assert without_sql_comment(parser=parser_stub, line=line) == expected


def complete_with_defaults(mapping):
    defaults = {
        "alias": None,
        "line": ["some-argument"],
        "connections": False,
        "close": None,
        "creator": None,
        "section": None,
        "persist": False,
        "persist_replace": False,
        "no_index": False,
        "append": False,
        "connection_arguments": None,
        "file": None,
        "interact": None,
        "save": None,
        "with_": None,
        "no_execute": False,
    }

    return {**defaults, **mapping}


@pytest.mark.parametrize(
    "line, expected",
    [
        (
            "some-argument",
            {"line": ["some-argument"]},
        ),
        (
            "a b c",
            {"line": ["a", "b", "c"]},
        ),
        (
            "a b c --file query.sql",
            {"line": ["a", "b", "c"], "file": "query.sql"},
        ),
    ],
)
def test_magic_args(ip, line, expected):
    sql_line = ip.magics_manager.lsmagic()["line"]["sql"]

    args = magic_args(sql_line, line)

    assert args.__dict__ == complete_with_defaults(expected)
