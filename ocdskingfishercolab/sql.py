"""SQL utilities."""

import contextlib
from urllib.parse import urljoin

import requests
import sql
from IPython import get_ipython
from jupyter_server import serverapp
from sqlalchemy.exc import ResourceClosedError

# Patch ipython-sql to add a comment to all SQL queries.
old_run = sql.run.run


def _run(conn, _sql, *args, **kwargs):
    try:
        comment = f"/* https://colab.research.google.com/drive/{_notebook_id()} */"
    except KeyError:
        comment = "/* run from a notebook, but no colab id */"
    return old_run(conn, comment + _sql, *args, **kwargs)


sql.run.run = _run


def _notebook_id():
    server = next(serverapp.list_running_servers())
    response = requests.get(urljoin(server["url"], "api/sessions"), timeout=10)
    response.raise_for_status()
    return response.json()[0]["path"][7:]  # fileId=


def _pluck(sql, **kwargs):
    return [row[0] for row in get_ipython_sql_resultset_from_query(sql, **kwargs)]


def set_search_path(schema_name):
    """
    Set the `search_path <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-SEARCH-PATH>`__
    to the given schema, followed by the ``public`` schema.

    :param str schema_name: a schema name
    """
    # https://github.com/catherinedevlin/ipython-sql/issues/191
    with contextlib.suppress(ResourceClosedError):
        get_ipython().run_line_magic("sql", f"SET search_path = {schema_name}, public")


# We need to add the local variables from its callers, so that `run_line_magic` finds them among locals. This module's
# local variables are prefixed with "_", to avoid shadowing local variables in the notebook's cells.
def get_ipython_sql_resultset_from_query(sql, _collection_id=None, _ocid=None):
    """
    Execute a SQL statement and return a ResultSet.

    Parameters are taken from the scope this function is called from (same behaviour as ipython-sql's ``%sql`` magic).

    :param str sql: a SQL statement
    :returns: the results as a :ipython-sql:`ResultSet<src/sql/run.py#L99>`
    :rtype: sql.run.ResultSet
    """
    ipython = get_ipython()
    autopandas = ipython.run_line_magic("config", "SqlMagic.autopandas")
    if autopandas:
        ipython.run_line_magic("config", "SqlMagic.autopandas = False")
    results = ipython.run_line_magic("sql", sql)
    if autopandas:
        ipython.run_line_magic("config", "SqlMagic.autopandas = True")
    return results
