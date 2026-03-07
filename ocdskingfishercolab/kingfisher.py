"""Kingfisher database integration."""

import textwrap

from IPython import get_ipython

from ocdskingfishercolab.exceptions import MissingFieldsError
from ocdskingfishercolab.sql import _pluck


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
