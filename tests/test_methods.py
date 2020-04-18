import contextlib
import getpass
import json
import os
from io import StringIO
from unittest.mock import patch

import psycopg2
import pytest

from ocdskingfishercolab import create_connection, downloadReleases


@pytest.fixture
def db():
    connection = psycopg2.connect(dbname='postgres')
    cursor = connection.cursor()

    # Avoid "CREATE DATABASE cannot run inside a transaction block" error
    connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        cursor.execute('CREATE DATABASE ocdskingfishercolab_test')

        conn = create_connection('ocdskingfishercolab_test', getpass.getuser())
        cur = conn.cursor()

        try:
            cur.execute("CREATE TABLE release (id int, collection_id int, ocid text, data_id int)")
            cur.execute("CREATE TABLE data (id int, data jsonb)")
            cur.execute("INSERT INTO release VALUES (1, 1, 'ocds-213czf-1', 1)")
            cur.execute("""INSERT INTO data VALUES (1, '{"ocid":"ocds-213czf-1"}'::jsonb)""")
            conn.commit()

            yield
        finally:
            cur.close()
            conn.close()
    finally:
        cursor.execute('DROP DATABASE ocdskingfishercolab_test')

        cursor.close()
        connection.close()



@contextlib.contextmanager
def chdir(path):
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


@patch('google.colab.files.download')
def test_downloadReleases_release(mocked, db, tmpdir):
    with chdir(tmpdir):
        downloadReleases(1, 'ocds-213czf-1', 'release')

        with open('ocds-213czf-1_release_package.json') as f:
            data = json.load(f)

            assert data == {
                'releases': [
                    {
                        'ocid': 'ocds-213czf-1',
                    },
                ],
            }
            mocked.assert_called_once_with('ocds-213czf-1_release_package.json')


@patch('google.colab.files.download')
def test_downloadReleases_record(mocked, db, tmpdir):
    with chdir(tmpdir):
        downloadReleases(1, 'ocds-213czf-1', 'record')

        with open('ocds-213czf-1_record_package.json') as f:
            data = json.load(f)

            assert data == {
                'ocid': 'ocds-213czf-1',
                'records': [
                    {
                        'releases': [
                            {
                                'ocid': 'ocds-213czf-1',
                            },
                        ],
                    },
                ],
            }

            mocked.assert_called_once_with('ocds-213czf-1_record_package.json')


@patch('sys.stdout', new_callable=StringIO)
def test_downloadReleases_other(stdout):
    downloadReleases(1, 'ocds-213czf-1', 'other')

    assert stdout.getvalue() == "package_type parameter must be either 'release' or 'record'\n"
