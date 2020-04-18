import contextlib
import getpass
import json
import os
from unittest.mock import patch

import psycopg2

from ocdskingfishercolab import create_connection, downloadReleases


@contextlib.contextmanager
def chdir(path):
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


@patch('google.colab.files.download')
def test_downloadReleases_release(mocked, tmpdir):
    with psycopg2.connect(dbname='postgres') as connection:
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        with connection.cursor() as cursor:
            cursor.execute('DROP DATABASE ocdskingfishercolab_test')
            cursor.execute('CREATE DATABASE ocdskingfishercolab_test')

            conn = create_connection('ocdskingfishercolab_test', getpass.getuser())
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE release_with_collection (id int, collection_id int, ocid text, data_id int)")
                cur.execute("CREATE TABLE data (id int, data jsonb)")
                cur.execute("INSERT INTO release_with_collection VALUES (1, 1, 'ocds-213czf-1', 1)")
                cur.execute("""INSERT INTO data VALUES (1, '{"ocid":"ocds-213czf-1"}'::jsonb)""")

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
            conn.close()


@patch('google.colab.files.download')
def test_downloadReleases_record(mocked, tmpdir):
    with psycopg2.connect(dbname='postgres') as connection:
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        with connection.cursor() as cursor:
            cursor.execute('DROP DATABASE ocdskingfishercolab_test')
            cursor.execute('CREATE DATABASE ocdskingfishercolab_test')

            conn = create_connection('ocdskingfishercolab_test', getpass.getuser())
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE release_with_collection (id int, collection_id int, ocid text, data_id int)")
                cur.execute("CREATE TABLE data (id int, data jsonb)")
                cur.execute("INSERT INTO release_with_collection VALUES (1, 1, 'ocds-213czf-1', 1)")
                cur.execute("""INSERT INTO data VALUES (1, '{"ocid":"ocds-213czf-1"}'::jsonb)""")

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
            conn.close()
