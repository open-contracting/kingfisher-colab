# Manual tests can be performed against regularly used Colabotatory notebooks:
# https://colab.research.google.com/drive/1lpWoGnOb6KcjHDEhSBjWZgA8aBLCfDp0

import contextlib
import json
import math
import os
import textwrap
from unittest.mock import patch
from zipfile import ZipFile

import pandas
import pytest
from IPython import get_ipython

from ocdskingfishercolab import (UnknownPackageTypeError, calculate_coverage, download_dataframe_as_csv,
                                 download_package_from_ocid, download_package_from_query,
                                 get_ipython_sql_resultset_from_query, list_collections, list_source_ids,
                                 save_dataframe_to_spreadsheet, set_search_path)


def path(filename):
    return os.path.join('tests', 'fixtures', filename)


def _notebook_id():
    return '1lpWoGnOb6KcjHDEhSBjWZgA8aBLCfDp0'


def _worksheets_length(zipfile):
    return len([name for name in zipfile.namelist() if name.startswith('xl/worksheets')])


@contextlib.contextmanager
def chdir(path):
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_set_search_path(db):
    set_search_path('test')

    get_ipython().magic('sql show search_path')['search_path'][0] == 'test, public'


@patch('ocdskingfishercolab.files.download')
def test_download_dataframe_as_csv(download, tmpdir):
    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pandas.DataFrame(data=d)

    with chdir(tmpdir):
        download_dataframe_as_csv(df, 'file.csv')

        with open('file.csv') as f:
            data = f.read()

        assert data == ',col1,col2\n0,1,3\n1,2,4\n'


@patch('ocdskingfishercolab.files.download')
@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_download_package_from_ocid_release(download, db, tmpdir):
    with chdir(tmpdir):
        download_package_from_ocid(1, 'ocds-213czf-1', 'release')

        with open('ocds-213czf-1_release_package.json') as f:
            data = json.load(f)

        assert data == {
            'uri': 'placeholder:',
            'publisher': {'name': ''},
            'publishedDate': '9999-01-01T00:00:00Z',
            'version': '1.1',
            'releases': [
                {'ocid': 'ocds-213czf-1', 'date': '2001'},
                {'ocid': 'ocds-213czf-1', 'date': '2000'},
            ],
        }

        download.assert_called_once_with('ocds-213czf-1_release_package.json')


@patch('ocdskingfishercolab.files.download')
@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_download_package_from_ocid_record(download, db, tmpdir):
    with chdir(tmpdir):
        download_package_from_ocid(1, 'ocds-213czf-1', 'record')

        with open('ocds-213czf-1_record_package.json') as f:
            data = json.load(f)

        assert data == {
            'uri': 'placeholder:',
            'publisher': {'name': ''},
            'publishedDate': '9999-01-01T00:00:00Z',
            'version': '1.1',
            'records': [{
                'ocid': 'ocds-213czf-1',
                'releases': [
                    {'ocid': 'ocds-213czf-1', 'date': '2001'},
                    {'ocid': 'ocds-213czf-1', 'date': '2000'},
                ],
            }],
        }

        download.assert_called_once_with('ocds-213czf-1_record_package.json')


@patch('ocdskingfishercolab.files.download')
@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_download_package_from_ocid_path_separator(download, db, tmpdir):
    with chdir(tmpdir):
        download_package_from_ocid(1, 'ocds-213czf-1/a', 'release')

        with open('ocds-213czf-1_a_release_package.json') as f:
            data = json.load(f)

        assert data == {
            'uri': 'placeholder:',
            'publisher': {'name': ''},
            'publishedDate': '9999-01-01T00:00:00Z',
            'version': '1.1',
            'releases': [{'ocid': 'ocds-213czf-1/a'}],
        }

        download.assert_called_once_with('ocds-213czf-1/a_release_package.json')


def test_download_package_from_ocid_other():
    with pytest.raises(UnknownPackageTypeError) as excinfo:
        download_package_from_ocid(1, 'ocds-213czf-1', 'other')

    assert str(excinfo.value) == "package_type argument must be either 'release' or 'record'"


@patch('ocdskingfishercolab.files.download')
@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_download_package_from_query_release(download, db, tmpdir):

    with chdir(tmpdir):
        get_ipython().run_cell(textwrap.dedent("""
            sql = '''
                SELECT data FROM data JOIN release ON data.id = release.data_id
                WHERE collection_id = :collection_id AND ocid = :ocid
            '''
            from ocdskingfishercolab import download_package_from_query
            collection_id = 1
            ocid = 'ocds-213czf-1/a'
            download_package_from_query(sql, 'release')
        """))

        with open('release_package.json') as f:
            data = json.load(f)

        assert data == {
            'uri': 'placeholder:',
            'publisher': {'name': ''},
            'publishedDate': '9999-01-01T00:00:00Z',
            'version': '1.1',
            'releases': [{'ocid': 'ocds-213czf-1/a'}],
        }

        download.assert_called_once_with('release_package.json')


@patch('ocdskingfishercolab.files.download')
@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_download_package_from_query_record(download, db, tmpdir):

    with chdir(tmpdir):
        get_ipython().run_cell(textwrap.dedent("""
            sql = '''
                SELECT data FROM data JOIN record ON data.id = record.data_id
                WHERE collection_id = :collection_id AND ocid = :ocid
            '''
            from ocdskingfishercolab import download_package_from_query
            collection_id = 1
            ocid = 'ocds-213czf-2'
            download_package_from_query(sql, 'record')
        """))

        with open('record_package.json') as f:
            data = json.load(f)

        assert data == {
            'uri': 'placeholder:',
            'publisher': {'name': ''},
            'publishedDate': '9999-01-01T00:00:00Z',
            'version': '1.1',
            'records': [{
                'ocid': 'ocds-213czf-2',
                'releases': [{'ocid': 'ocds-213czf-2'}],
            }],
        }

        download.assert_called_once_with('record_package.json')


def test_download_package_from_query_other():
    with pytest.raises(UnknownPackageTypeError) as excinfo:
        download_package_from_query('SELECT 1', package_type='other')

    assert str(excinfo.value) == "package_type argument must be either 'release' or 'record'"


@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_get_list_from_query(db):
    result = get_ipython_sql_resultset_from_query('SELECT * FROM record')

    assert result == [(1, 1, 'ocds-213czf-2', 4)]


@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_get_ipython_sql_resultset_from_query(db):
    result = get_ipython_sql_resultset_from_query('SELECT * FROM record')

    assert result.DataFrame().to_dict() == {
        'collection_id': {0: 1},
        'data_id': {0: 4},
        'id': {0: 1},
        'ocid': {0: 'ocds-213czf-2'},
    }


@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_get_ipython_sql_resultset_from_query_error(db, capsys):
    get_ipython().magic('sql invalid')
    captured = capsys.readouterr()

    assert '(psycopg2.errors.SyntaxError) syntax error at or near "invalid"\n' \
           'LINE 1: ...google.com/drive/1lpWoGnOb6KcjHDEhSBjWZgA8aBLCfDp0 */invalid\n' \
           '                                                                ^\n\n' \
           '[SQL: /* https://colab.research.google.com/drive/1lpWoGnOb6KcjHDEhSBjWZgA8aBLCfDp0' \
           ' */invalid]\n' \
           '(Background on this error at: http://sqlalche.me/e/13/f405)\n' in captured.out


@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_list_source_ids(db):
    dataframe = list_source_ids('paraguay')

    assert dataframe.to_dict() == {
        'source_id': {0: 'paraguay_dncp_records', 1: 'paraguay_dncp_releases'},
    }


@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_list_source_ids_default(db):
    dataframe = list_source_ids()

    assert dataframe.to_dict() == {
        'source_id': {0: 'paraguay_dncp_records', 1: 'paraguay_dncp_releases', 2: 'scotland'},
    }


@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_list_collections(db):
    dataframe = list_collections('paraguay_dncp_releases')

    actual = dataframe.to_dict()

    assert len(actual) == 3
    assert actual['id'] == {
        0: 5,
        1: 4,
        2: 3,
    }
    assert actual['source_id'] == {
        0: 'paraguay_dncp_releases',
        1: 'paraguay_dncp_releases',
        2: 'paraguay_dncp_releases',
    }
    assert actual['transform_from_collection_id'][0] == 4.0
    assert actual['transform_from_collection_id'][1] == 3.0
    assert math.isnan(actual['transform_from_collection_id'][2])


@patch('ocdskingfishercolab._save_file_to_drive')
def test_save_dataframe_to_spreadsheet(save, capsys, tmpdir):
    save.return_value = {'id': 'test'}

    d = {'release_package': [{'releases': [{'ocid': 'ocds-213czf-1'}]}]}
    df = pandas.DataFrame(data=d)

    fixture = os.path.realpath(path('flattened.xlsx'))

    with chdir(tmpdir):
        save_dataframe_to_spreadsheet(df, 'yet_another_excel_file')

        with open('release_package.json') as f:
            data = json.load(f)

        assert data == {'releases': [{'ocid': 'ocds-213czf-1'}]}

        with ZipFile('flattened.xlsx') as actual, ZipFile(fixture) as expected:
            assert _worksheets_length(actual) == _worksheets_length(expected) == 1
            assert actual.read('xl/worksheets/sheet1.xml') == expected.read('xl/worksheets/sheet1.xml')

        assert capsys.readouterr().out == "Uploaded file with ID 'test'\n"

        save.assert_called_once_with({'title': 'yet_another_excel_file.xlsx'}, 'flattened.xlsx')


@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_calculate_coverage(db, tmpdir):

    sql = calculate_coverage(["ocid"], scope="release_summary", sql_only=True)

    # only seperated to reduce line length
    case_statement = "CASE WHEN release_summary.field_list ? 'ocid' THEN 1 ELSE 0 END"

    assert sql.strip() == textwrap.dedent(f'''
    SELECT
        count(*) AS total_release_summary,
        ROUND(SUM({case_statement}) * 100.0 / count(*), 2) AS ocid_percentage,
        ROUND(SUM({case_statement}) * 100.0 / count(*), 2) AS total_percentage
    FROM
    release_summary''').strip()
