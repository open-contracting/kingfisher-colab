import contextlib
import json
import os
from io import StringIO
from unittest.mock import patch

import pandas
import psycopg2
import pytest

from ocdskingfishercolab import (UnknownPackageTypeError, download_dataframe_as_csv, download_package_from_ocid,
                                 download_package_from_query, get_dataframe_from_query)


@contextlib.contextmanager
def chdir(path):
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


@patch('google.colab.files.download')
def test_download_dataframe_as_csv(mocked, tmpdir):
    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pandas.DataFrame(data=d)

    with chdir(tmpdir):
        download_dataframe_as_csv(df, 'file.csv')

        with open('file.csv') as f:
            data = f.read()

            assert data == ',col1,col2\n0,1,3\n1,2,4\n'


@patch('google.colab.files.download')
def test_download_package_from_ocid_release(mocked, db, tmpdir):
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

            mocked.assert_called_once_with('ocds-213czf-1_release_package.json')


@patch('google.colab.files.download')
def test_download_package_from_ocid_record(mocked, db, tmpdir):
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

            mocked.assert_called_once_with('ocds-213czf-1_record_package.json')


@patch('sys.stdout', new_callable=StringIO)
def test_download_package_from_ocid_other(stdout):
    with pytest.raises(UnknownPackageTypeError) as excinfo:
        download_package_from_ocid(1, 'ocds-213czf-1', 'other')

    assert str(excinfo.value) == "package_type argument must be either 'release' or 'record'"


@patch('google.colab.files.download')
def test_download_package_from_query_release(mocked, db, tmpdir):
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

            mocked.assert_called_once_with('release_package.json')


@patch('google.colab.files.download')
def test_download_package_from_query_record(mocked, db, tmpdir):
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

            mocked.assert_called_once_with('record_package.json')


@patch('sys.stdout', new_callable=StringIO)
def test_download_package_from_query_other(stdout):
    with pytest.raises(UnknownPackageTypeError) as excinfo:
        download_package_from_query('SELECT 1', package_type='other')

    assert str(excinfo.value) == "package_type argument must be either 'release' or 'record'"


def test_get_dataframe_from_query(db):
    dataframe = get_dataframe_from_query('SELECT * FROM release')

    assert dataframe.to_dict() == {
        'collection_id': {0: 1},
        'data_id': {0: 1},
        'id': {0: 1},
        'ocid': {0: 'ocds-213czf-1'},
    }


def test_get_dataframe_from_query_error(db):
    with pytest.raises(psycopg2.errors.SyntaxError) as excinfo:
        get_dataframe_from_query('invalid')

    assert str(excinfo.value) == 'syntax error at or near "invalid"\nLINE 1: invalid\n        ^\n'
