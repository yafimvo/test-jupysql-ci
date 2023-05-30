import codecs
import csv
import operator
import os.path
import re
from functools import reduce
from io import StringIO
import html

import prettytable
import sqlalchemy
import sqlparse
from sql.connection import Connection
from sql import exceptions
from .column_guesser import ColumnGuesserMixin

try:
    from pgspecial.main import PGSpecial
except ModuleNotFoundError:
    PGSpecial = None
from sqlalchemy.orm import Session

from sql.telemetry import telemetry
import logging
import warnings
from collections.abc import Iterable

DEFAULT_DISPLAYLIMIT_VALUE = 10


def unduplicate_field_names(field_names):
    """Append a number to duplicate field names to make them unique."""
    res = []
    for k in field_names:
        if k in res:
            i = 1
            while k + "_" + str(i) in res:
                i += 1
            k += "_" + str(i)
        res.append(k)
    return res


class UnicodeWriter(object):
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        _row = row
        self.writer.writerow(_row)
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)
        self.queue.seek(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class CsvResultDescriptor(object):
    """
    Provides IPython Notebook-friendly output for the
    feedback after a ``.csv`` called.
    """

    def __init__(self, file_path):
        self.file_path = file_path

    def __repr__(self):
        return "CSV results at %s" % os.path.join(os.path.abspath("."), self.file_path)

    def _repr_html_(self):
        return '<a href="%s">CSV results</a>' % os.path.join(
            ".", "files", self.file_path
        )


def _nonbreaking_spaces(match_obj):
    """
    Make spaces visible in HTML by replacing all `` `` with ``&nbsp;``

    Call with a ``re`` match object.  Retain group 1, replace group 2
    with nonbreaking spaces.
    """
    spaces = "&nbsp;" * len(match_obj.group(2))
    return "%s%s" % (match_obj.group(1), spaces)


_cell_with_spaces_pattern = re.compile(r"(<td>)( {2,})")


class ResultSet(ColumnGuesserMixin):
    """
    Results of a SQL query.

    Can access rows listwise, or by string value of leftmost column.
    """

    def __init__(self, sqlaproxy, config):
        self.config = config
        self.keys = {}
        self._results = []

        # https://peps.python.org/pep-0249/#description
        is_dbapi_results = hasattr(sqlaproxy, "description")

        self.pretty = None

        if is_dbapi_results:
            should_try_fetch_results = True
        else:
            should_try_fetch_results = sqlaproxy.returns_rows

        if should_try_fetch_results:
            # sql alchemy results
            if not is_dbapi_results:
                self.keys = sqlaproxy.keys()
            elif isinstance(sqlaproxy.description, Iterable):
                self.keys = [i[0] for i in sqlaproxy.description]
            else:
                self.keys = []

            if len(self.keys) > 0:
                if isinstance(config.autolimit, int) and config.autolimit > 0:
                    self._results = sqlaproxy.fetchmany(size=config.autolimit)
                else:
                    self._results = sqlaproxy.fetchall()

                self.field_names = unduplicate_field_names(self.keys)

                _style = None

                if isinstance(config.style, str):
                    _style = prettytable.__dict__[config.style.upper()]

                self.pretty = PrettyTable(self.field_names, style=_style)

    def _repr_html_(self):
        _cell_with_spaces_pattern = re.compile(r"(<td>)( {2,})")
        if self.pretty:
            self.pretty.add_rows(self)
            result = self.pretty.get_html_string()
            # to create clickable links
            result = html.unescape(result)
            result = _cell_with_spaces_pattern.sub(_nonbreaking_spaces, result)
            if len(self) > self.pretty.row_count:
                HTML = (
                    '%s\n<span style="font-style:italic;text-align:center;">'
                    "%d rows, truncated to displaylimit of %d</span>"
                    "<br>"
                    '<span style="font-style:italic;text-align:center;">'
                    "If you want to see more, please visit "
                    '<a href="https://jupysql.ploomber.io/en/latest/api/configuration.html#displaylimit">displaylimit</a>'  # noqa: E501
                    " configuration</span>"
                )
                result = HTML % (result, len(self), self.pretty.row_count)
            return result
        else:
            return None

    def __len__(self):
        return len(self._results)

    def __iter__(self):
        for result in self._results:
            yield result

    def __str__(self, *arg, **kwarg):
        if self.pretty:
            self.pretty.add_rows(self)
        return str(self.pretty or "")

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, another: object) -> bool:
        return self._results == another

    def __getitem__(self, key):
        """
        Access by integer (row position within result set)
        or by string (value of leftmost column)
        """
        try:
            return self._results[key]
        except TypeError:
            result = [row for row in self if row[0] == key]
            if not result:
                raise KeyError(key)
            if len(result) > 1:
                raise KeyError('%d results for "%s"' % (len(result), key))
            return result[0]

    def dict(self):
        """Returns a single dict built from the result set

        Keys are column names; values are a tuple"""
        return dict(zip(self.keys, zip(*self)))

    def dicts(self):
        "Iterator yielding a dict for each row"
        for row in self:
            yield dict(zip(self.keys, row))

    @telemetry.log_call("data-frame", payload=True)
    def DataFrame(self, payload):
        "Returns a Pandas DataFrame instance built from the result set."
        import pandas as pd

        frame = pd.DataFrame(self, columns=(self and self.keys) or [])
        payload[
            "connection_info"
        ] = Connection.current._get_curr_sqlalchemy_connection_info()
        return frame

    @telemetry.log_call("polars-data-frame")
    def PolarsDataFrame(self, **polars_dataframe_kwargs):
        "Returns a Polars DataFrame instance built from the result set."
        import polars as pl

        frame = pl.DataFrame(
            (tuple(row) for row in self), schema=self.keys, **polars_dataframe_kwargs
        )
        return frame

    @telemetry.log_call("pie")
    def pie(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab pie chart from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        Values (pie slice sizes) are taken from the
        rightmost column (numerical values required).
        All other columns are used to label the pie slices.

        Parameters
        ----------
        key_word_sep: string used to separate column values
                      from each other in pie labels
        title: Plot title, defaults to name of value column

        Any additional keyword arguments will be passed
        through to ``matplotlib.pylab.pie``.
        """
        self.guess_pie_columns(xlabel_sep=key_word_sep)
        import matplotlib.pylab as plt

        ax = plt.gca()

        ax.pie(self.ys[0], labels=self.xlabels, **kwargs)
        ax.set_title(title or self.ys[0].name)
        return ax

    @telemetry.log_call("plot")
    def plot(self, title=None, **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        The first and last columns are taken as the X and Y
        values.  Any columns between are ignored.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passed
        through to ``matplotlib.pylab.plot``.
        """
        import matplotlib.pylab as plt

        self.guess_plot_columns()
        self.x = self.x or range(len(self.ys[0]))

        ax = plt.gca()

        coords = reduce(operator.add, [(self.x, y) for y in self.ys])
        ax.plot(*coords, **kwargs)

        if hasattr(self.x, "name"):
            ax.set_xlabel(self.x.name)

        ylabel = ", ".join(y.name for y in self.ys)

        ax.set_title(title or ylabel)
        ax.set_ylabel(ylabel)

        return ax

    @telemetry.log_call("bar")
    def bar(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab bar plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        The last quantitative column is taken as the Y values;
        all other columns are combined to label the X axis.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns
        key_word_sep: string used to separate column values
                      from each other in labels

        Any additional keyword arguments will be passed
        through to ``matplotlib.pylab.bar``.
        """
        import matplotlib.pylab as plt

        ax = plt.gca()

        self.guess_pie_columns(xlabel_sep=key_word_sep)
        ax.bar(range(len(self.ys[0])), self.ys[0], **kwargs)

        if self.xlabels:
            ax.set_xticks(range(len(self.xlabels)), self.xlabels, rotation=45)

        ax.set_xlabel(self.xlabel)
        ax.set_ylabel(self.ys[0].name)
        return ax

    @telemetry.log_call("generate-csv")
    def csv(self, filename=None, **format_params):
        """Generate results in comma-separated form.  Write to ``filename`` if given.
        Any other parameters will be passed on to csv.writer."""
        if not self.pretty:
            return None  # no results
        self.pretty.add_rows(self)
        if filename:
            encoding = format_params.get("encoding", "utf-8")
            outfile = open(filename, "w", newline="", encoding=encoding)
        else:
            outfile = StringIO()

        writer = UnicodeWriter(outfile, **format_params)
        writer.writerow(self.field_names)
        for row in self:
            writer.writerow(row)
        if filename:
            outfile.close()
            return CsvResultDescriptor(filename)
        else:
            return outfile.getvalue()


def interpret_rowcount(rowcount):
    if rowcount < 0:
        result = "Done."
    else:
        result = "%d rows affected." % rowcount
    return result


class FakeResultProxy(object):
    """A fake class that pretends to behave like the ResultProxy from
    SqlAlchemy.
    """

    def __init__(self, cursor, headers):
        if cursor is None:
            cursor = []
            headers = []
        if isinstance(cursor, list):
            self.from_list(source_list=cursor)
        else:
            self.fetchall = cursor.fetchall
            self.fetchmany = cursor.fetchmany
            self.rowcount = cursor.rowcount
        self.keys = lambda: headers
        self.returns_rows = True

    def from_list(self, source_list):
        "Simulates SQLA ResultProxy from a list."

        self.fetchall = lambda: source_list
        self.rowcount = len(source_list)

        def fetchmany(size):
            pos = 0
            while pos < len(source_list):
                yield source_list[pos : pos + size]
                pos += size

        self.fetchmany = fetchmany


# some dialects have autocommit
# specific dialects break when commit is used:

_COMMIT_BLACKLIST_DIALECTS = (
    "athena",
    "bigquery",
    "clickhouse",
    "ingres",
    "mssql",
    "teradata",
    "vertica",
)


def _commit(conn, config, manual_commit):
    """Issues a commit, if appropriate for current config and dialect"""

    _should_commit = (
        config.autocommit
        and all(
            dialect not in str(conn.dialect) for dialect in _COMMIT_BLACKLIST_DIALECTS
        )
        and manual_commit
    )

    if _should_commit:
        try:
            with Session(conn.session) as session:
                session.commit()
        except sqlalchemy.exc.OperationalError:
            print("The database does not support the COMMIT command")


def is_postgres_or_redshift(dialect):
    """Checks if dialect is postgres or redshift"""
    return "postgres" in str(dialect) or "redshift" in str(dialect)


def is_pytds(dialect):
    """Checks if driver is pytds"""
    return "pytds" in str(dialect)


def handle_postgres_special(conn, statement):
    """Execute a PostgreSQL special statement using PGSpecial module."""
    if not PGSpecial:
        raise exceptions.MissingPackageError("pgspecial not installed")

    pgspecial = PGSpecial()
    _, cur, headers, _ = pgspecial.execute(conn.session.connection.cursor(), statement)[
        0
    ]
    return FakeResultProxy(cur, headers)


def set_autocommit(conn, config):
    """Sets the autocommit setting for a database connection."""
    if is_pytds(conn.dialect):
        warnings.warn(
            "Autocommit is not supported for pytds, thus is automatically disabled"
        )
        return False
    if config.autocommit:
        try:
            conn.session.execution_options(isolation_level="AUTOCOMMIT")
        except Exception as e:
            logging.debug(
                f"The database driver doesn't support such "
                f"AUTOCOMMIT execution option"
                f"\nPerhaps you can try running a manual COMMIT command"
                f"\nMessage from the database driver\n\t"
                f"Exception:  {e}\n",  # noqa: F841
            )
            return True
    return False


def select_df_type(resultset, config):
    """
    Converts the input resultset to either a Pandas DataFrame
    or Polars DataFrame based on the config settings.
    """
    if config.autopandas:
        return resultset.DataFrame()
    elif config.autopolars:
        return resultset.PolarsDataFrame(**config.polars_dataframe_kwargs)
    else:
        return resultset
    # returning only last result, intentionally


def run(conn, sql, config):
    """Run a SQL query with the given connection

    Parameters
    ----------
    conn : sql.connection.Connection
        The connection to use

    sql : str
        SQL query to execution

    config
        Configuration object
    """
    info = conn._get_curr_sqlalchemy_connection_info()

    duckdb_autopandas = info and info.get("dialect") == "duckdb" and config.autopandas

    if not sql.strip():
        # returning only when sql is empty string
        return "Connected: %s" % conn.name

    for statement in sqlparse.split(sql):
        first_word = sql.strip().split()[0].lower()
        manual_commit = False

        # attempting to run a transaction
        if first_word == "begin":
            raise exceptions.RuntimeError("JupySQL does not support transactions")

        # postgres metacommand
        if first_word.startswith("\\") and is_postgres_or_redshift(conn.dialect):
            result = handle_postgres_special(conn, statement)

        # regular query
        else:
            manual_commit = set_autocommit(conn, config)
            is_custom_connection = Connection.is_custom_connection(conn)

            # if regular sqlalchemy, pass a text object
            if not is_custom_connection:
                statement = sqlalchemy.sql.text(statement)

            if duckdb_autopandas:
                conn = conn.engine.raw_connection()
                cursor = conn.cursor()
                cursor.execute(str(statement))

            else:
                result = conn.session.execute(statement)
                _commit(conn=conn, config=config, manual_commit=manual_commit)

                if result and config.feedback:
                    if hasattr(result, "rowcount"):
                        print(interpret_rowcount(result.rowcount))

    # bypass ResultSet and use duckdb's native method to return a pandas data frame
    if duckdb_autopandas:
        df = cursor.df()
        conn.close()
        return df
    else:
        resultset = ResultSet(result, config)
        return select_df_type(resultset, config)


def raw_run(conn, sql):
    return conn.session.execute(sqlalchemy.sql.text(sql))


class PrettyTable(prettytable.PrettyTable):
    def __init__(self, *args, **kwargs):
        self.row_count = 0
        self.displaylimit = DEFAULT_DISPLAYLIMIT_VALUE
        return super(PrettyTable, self).__init__(*args, **kwargs)

    def add_rows(self, data):
        if self.row_count and (data.config.displaylimit == self.displaylimit):
            return  # correct number of rows already present
        self.clear_rows()
        self.displaylimit = data.config.displaylimit
        if self.displaylimit == 0:
            self.displaylimit = None
        if self.displaylimit in (None, 0):
            self.row_count = len(data)
        else:
            self.row_count = min(len(data), self.displaylimit)
        for row in data[: self.displaylimit]:
            formatted_row = []
            for cell in row:
                if isinstance(cell, str) and cell.startswith("http"):
                    formatted_row.append("<a href={}>{}</a>".format(cell, cell))
                else:
                    formatted_row.append(cell)
            self.add_row(formatted_row)
