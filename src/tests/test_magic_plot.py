import sys
from pathlib import Path
import pytest
from IPython.core.error import UsageError
import matplotlib.pyplot as plt


@pytest.mark.parametrize(
    "cell, error_type, error_message",
    [
        [
            "%sqlplot someplot -t a -c b",
            UsageError,
            "unknown plot 'someplot'. must be any of: ",
        ],
        [
            "%sqlplot -t a -c b",
            UsageError,
            "missing the first argument, must be any of: ",
        ],
    ],
)
def test_validate_plot_name(tmp_empty, ip, cell, error_type, error_message):
    out = ip.run_cell(cell)

    assert isinstance(out.error_in_exec, error_type)
    assert str(error_message).lower() in str(out.error_in_exec).lower()


@pytest.mark.parametrize(
    "cell, error_type, error_message",
    [
        [
            "%sqlplot histogram --column a",
            UsageError,
            "the following arguments are required: -t/--table",
        ],
        [
            "%sqlplot histogram --table a",
            UsageError,
            "the following arguments are required: -c/--column",
        ],
    ],
)
def test_validate_arguments(tmp_empty, ip, cell, error_type, error_message):
    out = ip.run_cell(cell)

    assert isinstance(out.error_in_exec, error_type)
    assert str(out.error_in_exec) == (error_message)


@pytest.mark.parametrize(
    "cell",
    [
        "%sqlplot histogram --table data.csv --column x",
        "%sqlplot hist --table data.csv --column x",
        "%sqlplot histogram --table data.csv --column x --bins 10",
        pytest.param(
            "%sqlplot histogram --table nas.csv --column x",
            marks=pytest.mark.xfail(reason="Not implemented yet"),
        ),
        "%sqlplot boxplot --table data.csv --column x",
        "%sqlplot box --table data.csv --column x",
        "%sqlplot boxplot --table data.csv --column x --orient h",
        "%sqlplot boxplot --table subset --column x --with subset",
        "%sqlplot boxplot -t subset -c x -w subset -o h",
        "%sqlplot boxplot --table nas.csv --column x",
        "%sqlplot bar -t data.csv -c x",
        "%sqlplot bar -t data.csv -c x -o h",
        "%sqlplot bar -t data.csv -c x y",
        "%sqlplot pie -t data.csv -c x",
        "%sqlplot pie -t data.csv -c x y",
        pytest.param(
            "%sqlplot boxplot --table spaces.csv --column 'some column'",
            marks=pytest.mark.xfail(
                sys.platform == "win32",
                reason="problem in IPython.core.magic_arguments.parse_argstring",
            ),
        ),
        pytest.param(
            "%sqlplot histogram --table spaces.csv --column 'some column'",
            marks=pytest.mark.xfail(
                sys.platform == "win32",
                reason="problem in IPython.core.magic_arguments.parse_argstring",
            ),
        ),
        pytest.param(
            "%sqlplot bar --table spaces.csv --column 'some column'",
            marks=pytest.mark.xfail(
                sys.platform == "win32",
                reason="problem in IPython.core.magic_arguments.parse_argstring",
            ),
        ),
        pytest.param(
            "%sqlplot pie --table spaces.csv --column 'some column'",
            marks=pytest.mark.xfail(
                sys.platform == "win32",
                reason="problem in IPython.core.magic_arguments.parse_argstring",
            ),
        ),
        pytest.param(
            "%sqlplot boxplot --table 'file with spaces.csv' --column x",
            marks=pytest.mark.xfail(
                sys.platform == "win32",
                reason="problem in IPython.core.magic_arguments.parse_argstring",
            ),
        ),
        pytest.param(
            "%sqlplot histogram --table 'file with spaces.csv' --column x",
            marks=pytest.mark.xfail(
                sys.platform == "win32",
                reason="problem in IPython.core.magic_arguments.parse_argstring",
            ),
        ),
        pytest.param(
            "%sqlplot bar --table 'file with spaces.csv' --column x",
            marks=pytest.mark.xfail(
                sys.platform == "win32",
                reason="problem in IPython.core.magic_arguments.parse_argstring",
            ),
        ),
        pytest.param(
            "%sqlplot pie --table 'file with spaces.csv' --column x",
            marks=pytest.mark.xfail(
                sys.platform == "win32",
                reason="problem in IPython.core.magic_arguments.parse_argstring",
            ),
        ),
    ],
    ids=[
        "histogram",
        "hist",
        "histogram-bins",
        "histogram-nas",
        "boxplot",
        "box",
        "boxplot-horizontal",
        "boxplot-with",
        "boxplot-shortcuts",
        "boxplot-nas",
        "bar-1-col",
        "bar-1-col-horizontal",
        "bar-2-col",
        "pie-1-col",
        "pie-2-col",
        "boxplot-column-name-with-spaces",
        "histogram-column-name-with-spaces",
        "bar-column-name-with-spaces",
        "pie-column-name-with-spaces",
        "boxplot-table-name-with-spaces",
        "histogram-table-name-with-spaces",
        "bar-table-name-with-spaces",
        "pie-table-name-with-spaces",
    ],
)
def test_sqlplot(tmp_empty, ip, cell):
    # clean current Axes
    plt.cla()

    Path("spaces.csv").write_text(
        """\
"some column", y
0, 0
1, 1
2, 2
"""
    )

    Path("data.csv").write_text(
        """\
x, y
0, 0
1, 1
2, 2
"""
    )

    Path("nas.csv").write_text(
        """\
x, y
, 0
1, 1
2, 2
"""
    )

    Path("file with spaces.csv").write_text(
        """\
x, y
0, 0
1, 1
2, 2
"""
    )
    ip.run_cell("%sql duckdb://")

    ip.run_cell(
        """%%sql --save subset --no-execute
SELECT *
FROM data.csv
WHERE x > -1
"""
    )

    out = ip.run_cell(cell)

    # maptlotlib >= 3.7 has Axes but earlier Python
    # versions are not compatible
    assert type(out.result).__name__ in {"Axes", "AxesSubplot"}
