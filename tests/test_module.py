# Manual tests can be performed against regularly used Colabotatory notebooks:
# https://colab.research.google.com/drive/1lpWoGnOb6KcjHDEhSBjWZgA8aBLCfDp0

import contextlib
import json
import math
import os
from io import StringIO
from unittest.mock import patch
from zipfile import ZipFile

import pandas
import psycopg2
import pytest
from psycopg2.sql import SQL, Identifier

from ocdskingfishercolab import (UnknownPackageTypeError, download_dataframe_as_csv, download_package_from_ocid,
                                 download_package_from_query, execute_statement, get_dataframe_from_query,
                                 get_list_from_query, list_collections, list_source_ids, save_dataframe_to_spreadsheet,
                                 set_search_path)


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

    db.execute('show search_path')
    assert db.fetchone()[0] == 'test, public'


@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_execute_statement_composable(db):
    execute_statement(db, SQL('SELECT id FROM {}').format(Identifier('record')))

    assert [row for row in db] == [(1,)]


@patch('google.colab.files.download')
def test_download_dataframe_as_csv(download, tmpdir):
    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pandas.DataFrame(data=d)

    with chdir(tmpdir):
        download_dataframe_as_csv(df, 'file.csv')

        with open('file.csv') as f:
            data = f.read()

        assert data == ',col1,col2\n0,1,3\n1,2,4\n'


@patch('google.colab.files.download')
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
            'releases': [{'ocid': 'ocds-213czf-1'}],
        }

        download.assert_called_once_with('ocds-213czf-1_release_package.json')


@patch('google.colab.files.download')
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
                'releases': [{'ocid': 'ocds-213czf-1'}],
            }],
        }

        download.assert_called_once_with('ocds-213czf-1_record_package.json')


def test_download_package_from_ocid_other():
    with pytest.raises(UnknownPackageTypeError) as excinfo:
        download_package_from_ocid(1, 'ocds-213czf-1', 'other')

    assert str(excinfo.value) == "package_type argument must be either 'release' or 'record'"


@patch('google.colab.files.download')
@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_download_package_from_query_release(download, db, tmpdir):
    sql = """
    SELECT data FROM data JOIN release ON data.id = release.data_id
    WHERE collection_id = %(collection_id)s AND ocid = %(ocid)s
    """

    with chdir(tmpdir):
        download_package_from_query(sql, {'collection_id': 1, 'ocid': 'ocds-213czf-1'}, 'release')

        with open('release_package.json') as f:
            data = json.load(f)

        assert data == {
            'uri': 'placeholder:',
            'publisher': {'name': ''},
            'publishedDate': '9999-01-01T00:00:00Z',
            'version': '1.1',
            'releases': [{'ocid': 'ocds-213czf-1'}],
        }

        download.assert_called_once_with('release_package.json')


@patch('google.colab.files.download')
@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_download_package_from_query_record(download, db, tmpdir):
    sql = """
    SELECT data FROM data JOIN record ON data.id = record.data_id
    WHERE collection_id = %(collection_id)s AND ocid = %(ocid)s
    """

    with chdir(tmpdir):
        download_package_from_query(sql, {'collection_id': 1, 'ocid': 'ocds-213czf-2'}, 'record')

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
    result = get_list_from_query('SELECT * FROM release')

    assert result == [(1, 1, 'ocds-213czf-1', 1)]


@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_get_dataframe_from_query(db):
    dataframe = get_dataframe_from_query('SELECT * FROM release')

    assert dataframe.to_dict() == {
        'collection_id': {0: 1},
        'data_id': {0: 1},
        'id': {0: 1},
        'ocid': {0: 'ocds-213czf-1'},
    }


@patch('ocdskingfishercolab._notebook_id', _notebook_id)
def test_get_dataframe_from_query_error(db):
    with pytest.raises(psycopg2.errors.SyntaxError) as excinfo:
        get_dataframe_from_query('invalid')

    assert str(excinfo.value) == 'syntax error at or near "invalid"\n' \
                                 'LINE 1: ...google.com/drive/1lpWoGnOb6KcjHDEhSBjWZgA8aBLCfDp0 */invalid\n' \
                                 '                                                                ^\n'


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


@patch('sys.stdout', new_callable=StringIO)
@patch('ocdskingfishercolab._save_file_to_drive')
def test_save_dataframe_to_spreadsheet(save, stdout, tmpdir):
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

        assert stdout.getvalue() == "Uploaded file with ID 'test'\n"

        save.assert_called_once_with({'title': 'yet_another_excel_file.xlsx'}, 'flattened.xlsx')
