from typing import Iterator, Iterable
from collections.abc import MutableMapping
from jinja2 import Template
from ploomber_core.exceptions import modify_exceptions
import sql.connection
import difflib

from sql import exceptions


class SQLStore(MutableMapping):
    """Stores SQL scripts to render large queries with CTEs

    Notes
    -----
    .. versionadded:: 0.4.3

    Examples
    --------
    >>> from sql.store import SQLStore
    >>> sqlstore = SQLStore()
    >>> sqlstore.store("writers_fav",
    ...                "SELECT * FROM writers WHERE genre = 'non-fiction'")
    >>> sqlstore.store("writers_fav_modern",
    ...                "SELECT * FROM writers_fav WHERE born >= 1970",
    ...                with_=["writers_fav"])
    >>> query = sqlstore.render("SELECT * FROM writers_fav_modern LIMIT 10",
    ...                         with_=["writers_fav_modern"])
    >>> print(query)
    WITH "writers_fav" AS (
        SELECT * FROM writers WHERE genre = 'non-fiction'
    ), "writers_fav_modern" AS (
        SELECT * FROM writers_fav WHERE born >= 1970
    )
    SELECT * FROM writers_fav_modern LIMIT 10
    """

    def __init__(self):
        self._data = dict()

    def __setitem__(self, key: str, value: str) -> None:
        self._data[key] = value

    def __getitem__(self, key) -> str:
        if not self._data:
            raise exceptions.UsageError("No saved SQL")
        if key not in self._data:
            matches = difflib.get_close_matches(key, self._data)
            error = f'"{key}" is not a valid snippet identifier.'
            if matches:
                raise exceptions.UsageError(error + f' Did you mean "{matches[0]}"?')
            else:
                valid = ", ".join(f'"{key}"' for key in self._data.keys())
                raise exceptions.UsageError(error + f" Valid identifiers are {valid}.")
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        for key in self._data:
            yield key

    def __len__(self) -> int:
        return len(self._data)

    def __delitem__(self, key: str) -> None:
        del self._data[key]

    def render(self, query, with_=None):
        # TODO: if with is false, WITH should not appear
        return SQLQuery(self, query, with_)

    @modify_exceptions
    def store(self, key, query, with_=None):
        if "-" in key:
            raise exceptions.UsageError(
                "Using hyphens (-) in save argument isn't allowed."
                " Please use underscores (_) instead"
            )
        if with_ and key in with_:
            raise exceptions.UsageError(
                f"Script name ({key!r}) cannot appear in with_ argument"
            )

        self._data[key] = SQLQuery(self, query, with_)


class SQLQuery:
    """Holds queries and renders them"""

    def __init__(self, store: SQLStore, query: str, with_: Iterable = None):
        self._store = store
        self._query = query
        self._with_ = with_ or []

        if any("-" in x for x in self._with_):
            raise exceptions.UsageError(
                "Using hyphens is not allowed. "
                "Please use "
                + ", ".join(self._with_).replace("-", "_")
                + " instead for the with argument.",
            )

    def __str__(self) -> str:
        """
        We use the ' (backtick symbol) to wrap the CTE alias if the dialect supports
        ` (backtick)
        """
        with_clause_template = Template(
            """WITH{% for name in with_ %} {{name}} AS ({{saved[name]._query}})\
{{ "," if not loop.last }}{% endfor %}{{query}}"""
        )
        with_clause_template_backtick = Template(
            """WITH{% for name in with_ %} `{{name}}` AS ({{saved[name]._query}})\
{{ "," if not loop.last }}{% endfor %}{{query}}"""
        )
        is_use_backtick = sql.connection.Connection.current.is_use_backtick_template()
        with_all = _get_dependencies(self._store, self._with_)
        template = (
            with_clause_template_backtick if is_use_backtick else with_clause_template
        )
        return template.render(
            query=self._query, saved=self._store._data, with_=with_all
        )


def _get_dependencies(store, keys):
    """Get a list of all dependencies to reconstruct the CTEs in keys"""
    # get the dependencies for each key
    deps = _flatten([_get_dependencies_for_key(store, key) for key in keys])
    # remove duplicates but preserve order
    return list(dict.fromkeys(deps + keys))


def _get_dependencies_for_key(store, key):
    """Retrieve dependencies for a single key"""
    deps = store[key]._with_
    deps_of_deps = _flatten([_get_dependencies_for_key(store, dep) for dep in deps])
    return deps_of_deps + deps


def _flatten(elements):
    """Flatten a list of lists"""
    return [element for sub in elements for element in sub]


# session-wide store
store = SQLStore()
