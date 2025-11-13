import contextlib
import json
import os
import textwrap
import warnings
from urllib.parse import urljoin

import flattentool
import google.auth
import gspread
import httplib2
import matplotlib.ticker
import requests
import seaborn as sns
import sql
from babel.numbers import format_decimal
from flattentool.exceptions import FlattenToolWarning
from gspread_dataframe import set_with_dataframe
from IPython import get_ipython
from IPython.display import HTML
from jupyter_server import serverapp
from oauth2client.client import GoogleCredentials
from oauth2client.contrib.gce import AppAssertionCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from sqlalchemy.exc import ResourceClosedError

try:
    from google.colab import auth, files
except ImportError:
    # Assume we are in a testing environment.
    from unittest.mock import Mock

    files = Mock()
    files.download.return_value = None

# Patch ipython-sql to add a comment to all SQL queries.
old_run = sql.run.run
# Patch PyDrive2 like at: https://github.com/googlecolab/colabtools/blob/main/google/colab/_import_hooks/_pydrive.py
old_local_webserver_auth = GoogleAuth.LocalWebserverAuth


def _run(conn, _sql, *args, **kwargs):
    try:
        comment = f"/* https://colab.research.google.com/drive/{_notebook_id()} */"
    except KeyError:
        comment = "/* run from a notebook, but no colab id */"
    return old_run(conn, comment + _sql, *args, **kwargs)


def _local_web_server_auth(self, *args, **kwargs):
    if isinstance(self.credentials, AppAssertionCredentials):
        self.credentials.refresh(httplib2.Http())
        return None
    return old_local_webserver_auth(self, *args, **kwargs)


sql.run.run = _run
GoogleAuth.LocalWebserverAuth = _local_web_server_auth

# Use the same placeholder values as OCDS Kit.
package_metadata = {
    "uri": "placeholder:",
    "publisher": {
        "name": "",
    },
    "publishedDate": "9999-01-01T00:00:00Z",
    "version": "1.1",
}


def authenticate_gspread():
    """
    Authenticate the current user and give the notebook permission to connect to Google Spreadsheets.

    :returns: a `Google Sheets Client <https://gspread.readthedocs.io/en/latest/api.html#client>`__ instance
    :rtype: gspread.Client
    """
    auth.authenticate_user()
    credentials, _ = google.auth.default()
    return gspread.authorize(credentials)


def authenticate_pydrive():
    """
    Authenticate the current user and give the notebook permission to connect to Google Drive.

    :returns: a `GoogleDrive <https://gsuitedevs.github.io/PyDrive/docs/build/html/pydrive.html#module-pydrive.drive>`__ instance
    :rtype: pydrive.drive.GoogleDrive
    """  # noqa: E501
    auth.authenticate_user()
    gauth = GoogleAuth()
    gauth.credentials = GoogleCredentials.get_application_default()
    return GoogleDrive(gauth)


def list_source_ids(pattern=""):
    """
    Return, as a ResultSet or DataFrame, a list of source IDs matching the given pattern.

    :param str pattern: a substring, like "paraguay"
    :returns: the results as a pandas DataFrame or an ipython-sql :ipython-sql:`ResultSet<src/sql/run.py#L99>`,
              depending on whether ``%config SqlMagic.autopandas`` is ``True`` or ``False`` respectively. This is the
              same behaviour as ipython-sql's ``%sql`` magic.
    :rtype: pandas.DataFrame or sql.run.ResultSet
    """
    sql = """
    SELECT source_id
    FROM collection
    WHERE source_id ILIKE :pattern
    GROUP BY source_id
    ORDER BY source_id
    """

    pattern = f"%{pattern}%"

    # This inspects locals to find `pattern`.
    return get_ipython().run_line_magic("sql", sql)


def list_collections(source_id=None):
    """
    Return, as a ResultSet or DataFrame, a list of collections with the given source ID.

    :param str source_id: a source ID
    :returns: the results as a pandas DataFrame or an ipython-sql :ipython-sql:`ResultSet<src/sql/run.py#L99>`,
              depending on whether ``%config SqlMagic.autopandas`` is ``True`` or ``False`` respectively. This is the
              same behaviour as ipython-sql's ``%sql`` magic.
    :rtype: pandas.DataFrame or sql.run.ResultSet
    """
    sql = ["SELECT * FROM collection"]
    if source_id:
        sql.append("WHERE source_id = :source_id")
    sql.append("ORDER BY id DESC")

    # This inspects locals to find `source_id`.
    return get_ipython().run_line_magic("sql", " ".join(sql))


def set_search_path(schema_name):
    """
    Set the `search_path <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-SEARCH-PATH>`__
    to the given schema, followed by the ``public`` schema.

    :param str schema_name: a schema name
    """
    # https://github.com/catherinedevlin/ipython-sql/issues/191
    with contextlib.suppress(ResourceClosedError):
        get_ipython().run_line_magic("sql", f"SET search_path = {schema_name}, public")


def save_dataframe_to_sheet(spreadsheet_name, dataframe, sheetname, *, prompt=True):
    """
    Save a data frame to a worksheet in Google Sheets, after asking the user for confirmation.

    :param str spreadsheet_name: the name of the spreadsheet
    :param pandas.DataFrame dataframe: a data frame
    :param str sheetname: the name of the sheet to add
    :param bool prompt: whether to prompt the user
    """
    if dataframe.empty:
        print("Data frame is empty.")  # noqa: T201
        return

    if not prompt or input("Save to Google Sheets? (y/N)") == "y":
        gc = authenticate_gspread()
        try:
            sheet = gc.open(spreadsheet_name)
        except gspread.SpreadsheetNotFound:
            sheet = gc.create(spreadsheet_name)

        try:
            worksheet = sheet.add_worksheet(sheetname, dataframe.shape[0], dataframe.shape[1])
        except gspread.exceptions.APIError:
            newsheetname = input(f"{sheetname} already exists, enter a new name:")
            worksheet = sheet.add_worksheet(newsheetname, dataframe.shape[0], dataframe.shape[1])

        set_with_dataframe(worksheet, dataframe)


def save_dataframe_to_spreadsheet(dataframe, name):
    """
    Dump the ``release_package`` column of a data frame to a JSON file, convert the JSON file to an Excel file,
    and upload the Excel file to Google Drive.

    :param pandas.DataFrame dataframe: a data frame
    :param str name: the basename of the Excel file to write
    """
    if dataframe.empty:
        print("Data frame is empty.")  # noqa: T201
        return

    write_data_as_json(dataframe["release_package"][0], "release_package.json")

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FlattenToolWarning)

        flattentool.flatten(
            "release_package.json",
            main_sheet_name="releases",
            root_list_path="releases",
            root_id="ocid",
            schema="https://standard.open-contracting.org/1.1/en/release-schema.json",
            disable_local_refs=True,
            remove_empty_schema_columns=True,
            root_is_list=False,
            output_format="xlsx",
        )

    drive_file = _save_file_to_drive({"title": f"{name}.xlsx"}, "flattened.xlsx")
    print(f"Uploaded file with ID {drive_file['id']!r}")  # noqa: T201


def download_dataframe_as_csv(dataframe, filename):
    """
    Convert the data frame to a CSV file, and invoke a browser download of the CSV file to your local computer.

    :param pandas.DataFrame dataframe: a data frame
    :param str filename: a file name
    """
    dataframe.to_csv(filename)
    files.download(filename)


def download_data_as_json(data, filename):
    """
    Dump the data to a JSON file, and invoke a browser download of the CSV file to your local computer.

    :param data: JSON-serializable data
    :param str filename: a file name
    """
    write_data_as_json(data, filename)
    files.download(filename)


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


def download_package_from_query(sql, package_type=None):
    """
    Execute a SQL statement that SELECTs only the ``data`` column of the ``data`` table, and invoke a browser
    download of the packaged data to your local computer.

    :param str sql: a SQL statement
    :param str package_type: "release" or "record"
    :raises UnknownPackageTypeError: when the provided package type is unknown
    """
    if package_type not in {"release", "record"}:
        raise UnknownPackageTypeError("package_type argument must be either 'release' or 'record'")

    data = _pluck(sql)

    if package_type == "record":
        package = {"records": data}
    elif package_type == "release":
        package = {"releases": data}

    package.update(package_metadata)
    download_data_as_json(package, f"{package_type}_package.json")


def download_package_from_ocid(collection_id, ocid, package_type):
    """
    Select all releases with the given ocid from the given collection, and invoke a browser download of the packaged
    releases to your local computer.

    :param int collection_id: a collection's ID
    :param str ocid: an OCID
    :param str package_type: "release" or "record"
    :raises UnknownPackageTypeError: when the provided package type is unknown
    """
    if package_type not in {"release", "record"}:
        raise UnknownPackageTypeError("package_type argument must be either 'release' or 'record'")

    sql = """
    SELECT
        data
    FROM
        release
        JOIN data ON data.id = data_id
    WHERE
        collection_id = :_collection_id
        AND ocid = :_ocid
    ORDER BY
        release_date DESC
    """

    data = _pluck(sql, _collection_id=collection_id, _ocid=ocid)

    if package_type == "record":
        package = {"records": [{"ocid": ocid, "releases": data}]}
    elif package_type == "release":
        package = {"releases": data}

    package.update(package_metadata)
    download_data_as_json(package, f"{ocid}_{package_type}_package.json")


def write_data_as_json(data, filename):
    """
    Dump the data to a JSON file.

    :param data: JSON-serializable data
    :param str filename: a file name
    """
    with open(filename.replace(os.sep, "_"), "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _notebook_id():
    server = next(serverapp.list_running_servers())
    response = requests.get(urljoin(server["url"], "api/sessions"), timeout=10)
    response.raise_for_status()
    return response.json()[0]["path"][7:]  # fileId=


def _save_file_to_drive(metadata, filename):
    drive = authenticate_pydrive()
    drive_file = drive.CreateFile(metadata)
    drive_file.SetContentFile(filename)
    drive_file.Upload()
    return drive_file


def _pluck(sql, **kwargs):
    return [row[0] for row in get_ipython_sql_resultset_from_query(sql, **kwargs)]


def _all_tables():
    tables = set()
    for column, table in (("viewname", "pg_views"), ("tablename", "pg_tables")):
        tables.update(
            _pluck(
                f"SELECT {column} FROM pg_catalog.{table} "  # noqa: S608 # false positive
                "WHERE schemaname = ANY(CURRENT_SCHEMAS(false))"
            )
        )
    return tables


def render_json(json_string):
    """
    Render JSON into collapsible HTML.

    :param json_string: JSON-deserializable string
    """
    if not isinstance(json_string, str):
        json_string = json.dumps(json_string)
    return HTML(f"""
        <script
        src="https://cdn.jsdelivr.net/gh/caldwell/renderjson@master/renderjson.js">
        </script>
        <script>
        renderjson.set_show_to_level(1)
        document.body.appendChild(renderjson({json_string}))
        new ResizeObserver(google.colab.output.resizeIframeToContent).observe(document.body)
        </script>
        """)


def calculate_coverage(fields, scope=None, *, print_sql=True, return_sql=False):
    """
    Calculate the coverage of one or more fields using the summary tables produced by Kingfisher Summarize's
    ``--field-lists`` option. Return the coverage of each field and the co-occurrence coverage of all fields.

    ``scope`` is the Kingfisher Summarize table to measure coverage against, e.g. ``"awards_summary"``.
    Coverage is calculated using the number of rows in this table as the denominator.

    If ``scope`` is not set, it defaults to the parent table of the first field.

    ``fields`` is a list of fields to measure the coverage of, specified using JSON Pointer.

    If a field isn't a child of the ``scope`` table, use an absolute pointer:

    .. code-block:: python

       calculate_coverage(["tender/procurementMethod"], "awards_summary")

    If a field is a child of the ``scope`` table, use either an absolute pointer:

    .. code-block:: python

       calculate_coverage(["awards/value/amount"], "awards_summary")

    Or a relative pointer (prepend with ``":"``):

    .. code-block:: python

       calculate_coverage([":value/amount"], "awards_summary")

    If a field is within an array, it counts if it appears in **any** object in the array.

    .. code-block:: python

       calculate_coverage([":items/description"], "awards_summary")

    To require a field to appear in **all** objects in the array, prepend with ``"ALL "``:

    .. code-block:: python

       calculate_coverage(["ALL :items/description"], "awards_summary")

    .. note::

       Nested arrays, like the ``"awards/items/description"`` field with a ``"release_summary"`` scope, will yield
       inaccurate results, unless the initial arrays are present and one-to-one with the scope table (i.e. there is
       always exactly one award for each release).

    If ``scope`` is ``"awards_summary"``, you can specify fields on related contracts by prepending ``":contracts/"``:

    .. code-block:: python

       calculate_coverage([":value/amount", ":contracts/period"], "awards_summary")

    If ``scope`` is ``"contracts_summary"``, you can specify fields on related awards by prepending ``":awards/"``:

    .. code-block:: python

       calculate_coverage([":value/amount", ":awards/date"], "contracts_summary")

    :param list fields: the fields to measure coverage of
    :param str scope: the table to measure coverage against
    :param bool print_sql: print the SQL query
    :param bool return_sql: return the SQL query instead of executing the SQL query and returning the results

    :returns: the results as a pandas DataFrame or an ipython-sql :ipython-sql:`ResultSet<src/sql/run.py#L99>`,
              depending on whether ``%config SqlMagic.autopandas`` is ``True`` or ``False`` respectively. This is the
              same behaviour as ipython-sql's ``%sql`` magic.
    :rtype: pandas.DataFrame or sql.run.ResultSet
    """
    head_replacements = {
        "awards": "award",
        "contracts": "contract",
    }

    def get_table_and_pointer(tables, pointer):
        parts = pointer.split("/")
        table = "release_summary"

        # Abbreviate absolute pointers to relative pointers if the pointer is on the scope table.
        # For example: "awards/date" to "date" if the scope is "awards_summary."
        for i in range(len(parts), 0, -1):
            head = parts[0]
            # Kingfisher Summarize uses the singular prefixes "award_" and "contract_".
            if i > 1:
                head = head_replacements.get(head, head)
            # Kingfisher Summarize tables are lowercase.
            candidate = f"{'_'.join([head, *parts[1:i]])}_summary".lower()
            if candidate in tables:
                parts = parts[i:]
                table = candidate
                break

        return table, "/".join(parts)

    # https://www.postgresql.org/docs/current/functions-json.html
    def get_condition(table, pointer, mode):
        # Test for the presence of the field in any object.
        if mode == "any":
            return f"{table}.field_list ? '{pointer}'"

        # The logic from here is for mode == "all".
        parts = pointer.split("/")

        # It would be more robust to analyze the release schema. That said, as of OCDS 1.1.5, all arrays of objects
        # end in "s", and only one object ends in "s" ("address").
        array_indices = [i for i, part in enumerate(parts[:-1]) if part.endswith("s") and part != "address"]

        # If the field is not within an array, simplify the logic from ALL to ANY.
        if not array_indices:
            return f"{table}.field_list ? '{pointer}'"

        # If arrays are nested, then the condition below can be satisfied for, e.g., awards/items/description, if there
        # are 2 awards, only one of which sets items/description.
        if len(array_indices) > 1:
            print(  # noqa: T201
                "WARNING: Results might be inaccurate due to nested arrays. Check that there is exactly one "
                f"`{'/'.join(parts[: array_indices[-2] + 1])}` path per {table} row."
            )

        # Test whether the number of occurrences of the path and its closest enclosing array are equal.
        return (
            f"coalesce({table}.field_list->>'{pointer}' =\n"
            f"                  {table}.field_list->>'{'/'.join(parts[: array_indices[-1] + 1])}', false)"
        )

    if not fields:
        raise MissingFieldsError("You must provide a list of fields as the first argument to `calculate_coverage`.")

    # Default to the parent table of the first field.
    if not scope:
        scope, _ = get_table_and_pointer(_all_tables(), fields[0].split()[-1])

    columns = {}
    conditions = []
    join = ""
    for field in fields:
        split = field.split()
        pointer = split[-1]

        # If the first token isn't "ALL" or if there are more than 2, behave as if only the last token was provided.
        mode = "all" if len(split) == 2 and split[0].lower() == "all" else "any"

        # Handle relative pointers. This includes `:awards` and `:contracts` (see Kingfisher Summarize).
        if pointer.startswith(":"):
            table, pointer = scope, pointer[1:]
        # Handle absolute pointers.
        else:
            table, pointer = get_table_and_pointer({scope}, pointer)

        condition = get_condition(table, pointer, mode)

        # Add a JOIN clause for the release_summary table, unless it is already in the FROM clause.
        if table == "release_summary" and scope != "release_summary":
            join = f"JOIN\n            release_summary ON release_summary.id = {scope}.id"

        # Add the field coverage.
        alias = pointer.replace("/", "_").lower()
        if mode == "all":
            alias = f"all_{alias}"
        columns[alias] = condition

        # Collect the conditions for co-occurrence coverage.
        conditions.append(condition)

    # Add the co-occurrence coverage.
    columns["total"] = " AND\n                ".join(conditions)

    select = ",\n            ".join(
        f"ROUND(SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS {alias}_percentage"
        for alias, condition in columns.items()
    )
    sql = textwrap.dedent(f"""\
        SELECT
            count(*) AS total_{scope},
            {select}
        FROM {scope}
        {join}
    """)  # noqa: S608

    if print_sql:
        print(sql)  # noqa: T201

    if return_sql:
        return sql

    return get_ipython().run_cell_magic("sql", "", sql)


def set_dark_mode():
    """Set the Seaborn theme to match Google Colab's dark mode."""
    sns.set_style(
        "dark",
        {
            "figure.facecolor": "#383838",
            "axes.edgecolor": "#d5d5d5",
            "axes.facecolor": "#383838",
            "axes.labelcolor": "#d5d5d5",
            "text.color": "#d5d5d5",
            "xtick.color": "#d5d5d5",
            "ytick.color": "#d5d5d5",
        },
    )


def set_light_mode():
    """Set the Seaborn theme to light mode, for exporting plots."""
    sns.set_theme()


def format_thousands(axis, locale="en_US"):
    """Set the thousands separator on the given axis for the given locale, e.g. ``en_US``."""
    axis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, _: format_decimal(x, format="#", locale=locale))
    )


class OCDSKingfisherColabError(Exception):
    """Base class for exceptions from within this package."""


class UnknownPackageTypeError(OCDSKingfisherColabError, ValueError):
    """Raised when the provided package type is unknown."""


class MissingFieldsError(OCDSKingfisherColabError):
    """Raised when no fields are provided to a function."""
