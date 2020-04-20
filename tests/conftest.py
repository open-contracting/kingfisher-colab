import getpass
import os
from urllib.parse import urlparse

import psycopg2
import pytest

from ocdskingfishercolab import create_connection


@pytest.fixture
def db():
    database_url = os.getenv('DATABASE_URL', 'postgresql://{}:@localhost:5432/postgres'.format(getpass.getuser()))
    parts = urlparse(database_url)
    kwargs = {
        'user': parts.username,
        'password': parts.password,
        'host': parts.hostname,
        'port': parts.port,
    }

    connection = psycopg2.connect(dbname=parts.path[1:], **kwargs)
    cursor = connection.cursor()

    # Avoid "CREATE DATABASE cannot run inside a transaction block" error
    connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        cursor.execute('CREATE DATABASE ocdskingfishercolab_test')

        conn = create_connection(database='ocdskingfishercolab_test', **kwargs)
        cur = conn.cursor()

        try:
            cur.execute("CREATE TABLE release (id int, collection_id int, ocid text, data_id int)")
            cur.execute("CREATE TABLE record (id int, collection_id int, ocid text, data_id int)")
            cur.execute("CREATE TABLE data (id int, data jsonb)")
            cur.execute("INSERT INTO release VALUES (1, 1, 'ocds-213czf-1', 1)")
            cur.execute("INSERT INTO record VALUES (1, 1, 'ocds-213czf-2', 2)")
            cur.execute("""INSERT INTO data VALUES (1, '{"ocid":"ocds-213czf-1"}'::jsonb)""")
            cur.execute("""INSERT INTO data VALUES (2, '{"ocid":"ocds-213czf-2","""
                        """"releases":[{"ocid":"ocds-213czf-2"}]}'::jsonb)""")
            conn.commit()

            yield
        finally:
            cur.close()
            conn.close()
    finally:
        cursor.execute('DROP DATABASE ocdskingfishercolab_test')

        cursor.close()
        connection.close()