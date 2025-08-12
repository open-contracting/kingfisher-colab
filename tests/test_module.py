# Manual tests can be performed against regularly used Colabotatory notebooks:
# https://colab.research.google.com/drive/1lpWoGnOb6KcjHDEhSBjWZgA8aBLCfDp0

import contextlib
import json
import math
import os
import textwrap
from unittest.mock import patch
from zipfile import ZipFile

import pandas as pd
import pytest
from IPython import get_ipython

from ocdskingfishercolab import (
    UnknownPackageTypeError,
    calculate_coverage,
    download_dataframe_as_csv,
    download_package_from_ocid,
    download_package_from_query,
    get_ipython_sql_resultset_from_query,
    list_collections,
    list_source_ids,
    save_dataframe_to_spreadsheet,
    set_search_path,
)


def _notebook_id():
    return "1lpWoGnOb6KcjHDEhSBjWZgA8aBLCfDp0"


def _all_tables():
    return {
        "field_counts",
        "release_summary",
        "release_summary_no_data",
        "parties_summary",
        "buyer_summary",
        "procuringEntity_summary",
        "tenderers_summary",
        "planning_summary",
        "planning_documents_summary",
        "planning_milestones_summary",
        "tender_summary",
        "tender_summary_no_data",
        "tender_items_summary",
        "tender_documents_summary",
        "tender_milestones_summary",
        "awards_summary",
        "award_suppliers_summary",
        "award_items_summary",
        "award_documents_summary",
        "contracts_summary",
        "contract_items_summary",
        "contract_documents_summary",
        "contract_milestones_summary",
        "contract_implementation_transactions_summary",
        "contract_implementation_documents_summary",
        "contract_implementation_milestones_summary",
        "relatedprocesses_summary",
    }


@contextlib.contextmanager
def chdir(path):
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


@patch("ocdskingfishercolab._notebook_id", _notebook_id)
def test_set_search_path(db):
    set_search_path("test")

    assert get_ipython().run_line_magic("sql", "show search_path")["search_path"][0] == "test, public"


@patch("ocdskingfishercolab.files.download")
def test_download_dataframe_as_csv(download, tmpdir):
    df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    with chdir(tmpdir):
        download_dataframe_as_csv(df, "file.csv")

        with open("file.csv") as f:
            data = f.read()

        assert data == ",col1,col2\n0,1,3\n1,2,4\n"


@patch("ocdskingfishercolab.files.download")
@patch("ocdskingfishercolab._notebook_id", _notebook_id)
def test_download_package_from_ocid_release(download, db, tmpdir):
    with chdir(tmpdir):
        download_package_from_ocid(1, "ocds-213czf-1", "release")

        with open("ocds-213czf-1_release_package.json") as f:
            data = json.load(f)

        assert data == {
            "uri": "placeholder:",
            "publisher": {"name": ""},
            "publishedDate": "9999-01-01T00:00:00Z",
            "version": "1.1",
            "releases": [
                {"ocid": "ocds-213czf-1", "date": "2001"},
                {"ocid": "ocds-213czf-1", "date": "2000"},
            ],
        }

        download.assert_called_once_with("ocds-213czf-1_release_package.json")


@patch("ocdskingfishercolab.files.download")
@patch("ocdskingfishercolab._notebook_id", _notebook_id)
def test_download_package_from_ocid_record(download, db, tmpdir):
    with chdir(tmpdir):
        download_package_from_ocid(1, "ocds-213czf-1", "record")

        with open("ocds-213czf-1_record_package.json") as f:
            data = json.load(f)

        assert data == {
            "uri": "placeholder:",
            "publisher": {"name": ""},
            "publishedDate": "9999-01-01T00:00:00Z",
            "version": "1.1",
            "records": [
                {
                    "ocid": "ocds-213czf-1",
                    "releases": [
                        {"ocid": "ocds-213czf-1", "date": "2001"},
                        {"ocid": "ocds-213czf-1", "date": "2000"},
                    ],
                }
            ],
        }

        download.assert_called_once_with("ocds-213czf-1_record_package.json")


@patch("ocdskingfishercolab.files.download")
@patch("ocdskingfishercolab._notebook_id", _notebook_id)
def test_download_package_from_ocid_path_separator(download, db, tmpdir):
    with chdir(tmpdir):
        download_package_from_ocid(1, "ocds-213czf-1/a", "release")

        with open("ocds-213czf-1_a_release_package.json") as f:
            data = json.load(f)

        assert data == {
            "uri": "placeholder:",
            "publisher": {"name": ""},
            "publishedDate": "9999-01-01T00:00:00Z",
            "version": "1.1",
            "releases": [{"ocid": "ocds-213czf-1/a"}],
        }

        download.assert_called_once_with("ocds-213czf-1/a_release_package.json")


def test_download_package_from_ocid_other():
    with pytest.raises(UnknownPackageTypeError) as excinfo:
        download_package_from_ocid(1, "ocds-213czf-1", "other")

    assert str(excinfo.value) == "package_type argument must be either 'release' or 'record'"


@patch("ocdskingfishercolab.files.download")
@patch("ocdskingfishercolab._notebook_id", _notebook_id)
def test_download_package_from_query_release(download, db, tmpdir):
    with chdir(tmpdir):
        get_ipython().run_cell(
            textwrap.dedent("""
            sql = '''
                SELECT data FROM data JOIN release ON data.id = release.data_id
                WHERE collection_id = :collection_id AND ocid = :ocid
            '''
            from ocdskingfishercolab import download_package_from_query
            collection_id = 1
            ocid = 'ocds-213czf-1/a'
            download_package_from_query(sql, 'release')
        """)
        )

        with open("release_package.json") as f:
            data = json.load(f)

        assert data == {
            "uri": "placeholder:",
            "publisher": {"name": ""},
            "publishedDate": "9999-01-01T00:00:00Z",
            "version": "1.1",
            "releases": [{"ocid": "ocds-213czf-1/a"}],
        }

        download.assert_called_once_with("release_package.json")


@patch("ocdskingfishercolab.files.download")
@patch("ocdskingfishercolab._notebook_id", _notebook_id)
def test_download_package_from_query_record(download, db, tmpdir):
    with chdir(tmpdir):
        get_ipython().run_cell(
            textwrap.dedent("""
            sql = '''
                SELECT data FROM data JOIN record ON data.id = record.data_id
                WHERE collection_id = :collection_id AND ocid = :ocid
            '''
            from ocdskingfishercolab import download_package_from_query
            collection_id = 1
            ocid = 'ocds-213czf-2'
            download_package_from_query(sql, 'record')
        """)
        )

        with open("record_package.json") as f:
            data = json.load(f)

        assert data == {
            "uri": "placeholder:",
            "publisher": {"name": ""},
            "publishedDate": "9999-01-01T00:00:00Z",
            "version": "1.1",
            "records": [
                {
                    "ocid": "ocds-213czf-2",
                    "releases": [{"ocid": "ocds-213czf-2"}],
                }
            ],
        }

        download.assert_called_once_with("record_package.json")


def test_download_package_from_query_other():
    with pytest.raises(UnknownPackageTypeError) as excinfo:
        download_package_from_query("SELECT 1", package_type="other")

    assert str(excinfo.value) == "package_type argument must be either 'release' or 'record'"


@patch("ocdskingfishercolab._notebook_id", _notebook_id)
def test_get_list_from_query(db):
    result = get_ipython_sql_resultset_from_query("SELECT * FROM record")

    assert result == [(1, 1, "ocds-213czf-2", 4)]


@patch("ocdskingfishercolab._notebook_id", _notebook_id)
def test_get_ipython_sql_resultset_from_query(db):
    result = get_ipython_sql_resultset_from_query("SELECT * FROM record")

    assert result.DataFrame().to_dict() == {
        "collection_id": {0: 1},
        "data_id": {0: 4},
        "id": {0: 1},
        "ocid": {0: "ocds-213czf-2"},
    }


@patch("ocdskingfishercolab._notebook_id", _notebook_id)
def test_get_ipython_sql_resultset_from_query_error(db, capsys):
    get_ipython().run_line_magic("sql", "invalid")

    assert (
        '(psycopg2.errors.SyntaxError) syntax error at or near "invalid"\n'
        "LINE 1: ...google.com/drive/1lpWoGnOb6KcjHDEhSBjWZgA8aBLCfDp0 */invalid\n"
        "                                                                ^\n\n"
        "[SQL: /* https://colab.research.google.com/drive/1lpWoGnOb6KcjHDEhSBjWZgA8aBLCfDp0 */invalid]\n"
        "(Background on this error at: https://sqlalche.me/e/" in capsys.readouterr().out
    )


@patch("ocdskingfishercolab._notebook_id", _notebook_id)
def test_list_source_ids(db):
    dataframe = list_source_ids("paraguay")

    assert dataframe.to_dict() == {
        "source_id": {0: "paraguay_dncp_records", 1: "paraguay_dncp_releases"},
    }


@patch("ocdskingfishercolab._notebook_id", _notebook_id)
def test_list_source_ids_default(db):
    dataframe = list_source_ids()

    assert dataframe.to_dict() == {
        "source_id": {0: "paraguay_dncp_records", 1: "paraguay_dncp_releases", 2: "scotland"},
    }


@patch("ocdskingfishercolab._notebook_id", _notebook_id)
def test_list_collections(db):
    dataframe = list_collections("paraguay_dncp_releases")

    actual = dataframe.to_dict()

    assert len(actual) == 3
    assert actual["id"] == {
        0: 5,
        1: 4,
        2: 3,
    }
    assert actual["source_id"] == {
        0: "paraguay_dncp_releases",
        1: "paraguay_dncp_releases",
        2: "paraguay_dncp_releases",
    }
    assert actual["transform_from_collection_id"][0] == 4.0
    assert actual["transform_from_collection_id"][1] == 3.0
    assert math.isnan(actual["transform_from_collection_id"][2])


@patch("ocdskingfishercolab._save_file_to_drive")
def test_save_dataframe_to_spreadsheet(save, capsys, tmpdir):
    save.return_value = {"id": "test"}

    df = pd.DataFrame(data={"release_package": [{"releases": [{"ocid": "ocds-213czf-1"}]}]})

    with chdir(tmpdir):
        save_dataframe_to_spreadsheet(df, "yet_another_excel_file")

        with open("release_package.json") as f:
            assert json.load(f) == {"releases": [{"ocid": "ocds-213czf-1"}]}

        with ZipFile("flattened.xlsx") as zipfile:
            sheet_count = len([name for name in zipfile.namelist() if name.startswith("xl/worksheets")])
            sheet_content = (
                b'<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetPr><outlinePr '
                b'summaryBelow="1" summaryRight="1"/><pageSetUpPr/></sheetPr><sheetViews><sheetView '
                b'workbookViewId="0"><selection activeCell="A1" sqref="A1"/></sheetView></sheetViews><sheetFormatPr '
                b'baseColWidth="8" defaultRowHeight="15"/><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>ocid'
                b'</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>ocds-213czf-1</t></is></c></row>'
                b'</sheetData><pageMargins left="0.75" right="0.75" top="1" bottom="1" header="0.5" footer="0.5"/>'
                b"</worksheet>"
            )

            assert sheet_count == 1
            assert zipfile.read("xl/worksheets/sheet1.xml") == sheet_content

        assert capsys.readouterr().out == "Uploaded file with ID 'test'\n"

        save.assert_called_once_with({"title": "yet_another_excel_file.xlsx"}, "flattened.xlsx")


@patch("ocdskingfishercolab._save_file_to_drive")
def test_save_dataframe_to_spreadsheet_empty(save, capsys, tmpdir):
    df = pd.DataFrame()

    with chdir(tmpdir):
        save_dataframe_to_spreadsheet(df, "yet_another_excel_file")

        for filename in ("release_package.json", "flattened.xlsx"):
            assert not os.path.exists(filename)

        assert capsys.readouterr().out == "Data frame is empty.\n"

        save.assert_not_called()


@pytest.mark.parametrize(
    ("field", "pointer", "alias", "scope"),
    [
        # Absolute pointer.
        ("ocid", "ocid", "ocid", "release_summary"),
        ("ALL tender/id", "tender/id", "all_tender_id", "release_summary"),
        # Relative pointer.
        (":quantity", "quantity", "quantity", "award_items_summary"),
        ("ALL :date", "date", "all_date", "awards_summary"),
    ],
)
def test_calculate_coverage_any(field, pointer, alias, scope, db, tmpdir):
    sql = calculate_coverage([field], scope=scope, return_sql=True)

    assert sql == textwrap.dedent(f"""\
        SELECT
            count(*) AS total_{scope},
            ROUND(SUM(CASE WHEN {scope}.field_list ? '{pointer}' THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS {alias}_percentage,
            ROUND(SUM(CASE WHEN {scope}.field_list ? '{pointer}' THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS total_percentage
        FROM {scope}

    """)  # noqa: E501


@pytest.mark.parametrize(
    ("field", "parent", "warning", "scope"),
    [
        # Relative pointer.
        (":items/description", "items", None, "awards_summary"),
        # One nested array.
        ("awards/items/description", "awards/items", "awards", "release_summary"),
        # Two nested arrays.
        (
            "awards/items/additionalClassifications/scheme",
            "awards/items/additionalClassifications",
            "awards/items",
            "release_summary",
        ),
        # The "address" field should not be treated as an array.
        ("parties/address/region", "parties", None, "release_summary"),
        # Non-array ancestors should be retained.
        ("a/bs/c/ds/e/fs/g", "a/bs/c/ds/e/fs", "a/bs/c/ds", "release_summary"),
    ],
)
def test_calculate_coverage_all(field, parent, warning, scope, db, capsys, tmpdir):
    sql = calculate_coverage([f"ALL {field}"], scope=scope, print_sql=False, return_sql=True)

    pointer = field.removeprefix(":")
    alias = pointer.replace("/", "_").lower()

    assert sql == textwrap.dedent(f"""\
        SELECT
            count(*) AS total_{scope},
            ROUND(SUM(CASE WHEN coalesce({scope}.field_list->>'{pointer}' =
                  {scope}.field_list->>'{parent}', false) THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS all_{alias}_percentage,
            ROUND(SUM(CASE WHEN coalesce({scope}.field_list->>'{pointer}' =
                  {scope}.field_list->>'{parent}', false) THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS total_percentage
        FROM {scope}

    """)  # noqa: E501

    if warning:
        expected = f"WARNING: Results might be inaccurate due to nested arrays. Check that there is exactly one `{warning}` path per {scope} row.\n"  # noqa: E501
    else:
        expected = ""

    assert capsys.readouterr().out == expected


def test_calculate_coverage_all_mixed(db, capsys, tmpdir):
    fields = ["ALL :items/description", ":items/description"]
    sql = calculate_coverage(fields, scope="awards_summary", print_sql=False, return_sql=True)

    assert sql == textwrap.dedent("""\
        SELECT
            count(*) AS total_awards_summary,
            ROUND(SUM(CASE WHEN coalesce(awards_summary.field_list->>'items/description' =
                  awards_summary.field_list->>'items', false) THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS all_items_description_percentage,
            ROUND(SUM(CASE WHEN awards_summary.field_list ? 'items/description' THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS items_description_percentage,
            ROUND(SUM(CASE WHEN coalesce(awards_summary.field_list->>'items/description' =
                  awards_summary.field_list->>'items', false) AND
                awards_summary.field_list ? 'items/description' THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS total_percentage
        FROM awards_summary

    """)  # noqa: E501

    assert capsys.readouterr().out == ""


def test_calculate_coverage_join_release_summary(db, tmpdir):
    sql = calculate_coverage(["awards/title", "contracts/title"], scope="contracts_summary", return_sql=True)

    assert sql == textwrap.dedent("""\
        SELECT
            count(*) AS total_contracts_summary,
            ROUND(SUM(CASE WHEN release_summary.field_list ? 'awards/title' THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS awards_title_percentage,
            ROUND(SUM(CASE WHEN contracts_summary.field_list ? 'title' THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS title_percentage,
            ROUND(SUM(CASE WHEN release_summary.field_list ? 'awards/title' AND
                contracts_summary.field_list ? 'title' THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS total_percentage
        FROM contracts_summary
        JOIN
            release_summary ON release_summary.id = contracts_summary.id
    """)  # noqa: E501


@patch("ocdskingfishercolab._all_tables", _all_tables)
@pytest.mark.parametrize(
    ("field", "pointer", "table"),
    [
        ("awards/date", "date", "awards_summary"),
        ("tender/documents/format", "format", "tender_documents_summary"),
        ("relatedProcesses/relationship", "relationship", "relatedprocesses_summary"),
        ("awards/items/quantity", "quantity", "award_items_summary"),
        ("contracts/implementation/documents/format", "format", "contract_implementation_documents_summary"),
    ],
)
def test_calculate_coverage_default_scope(field, pointer, table, db, tmpdir):
    sql = calculate_coverage([field], return_sql=True)

    assert sql == textwrap.dedent(f"""\
        SELECT
            count(*) AS total_{table},
            ROUND(SUM(CASE WHEN {table}.field_list ? '{pointer}' THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS {pointer.replace("/", "_").lower()}_percentage,
            ROUND(SUM(CASE WHEN {table}.field_list ? '{pointer}' THEN 1 ELSE 0 END) * 100.0 / count(*), 2) AS total_percentage
        FROM {table}

    """)  # noqa: E501
