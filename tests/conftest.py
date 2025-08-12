import getpass
import os
from urllib.parse import urlsplit

import psycopg2
import pytest
import sql
from IPython import get_ipython


# If this fixture becomes too slow, we can setup the database once, and run each test in a transaction.
@pytest.fixture
def db():
    # This can't be named DATABASE_URL, because ipython-sql will try and use it.
    database_url = os.getenv("TEST_DATABASE_URL", f"postgresql://{getpass.getuser()}:@localhost:5432/postgres")
    parsed = urlsplit(database_url)
    created_database_url = parsed._replace(path="/ocdskingfishercolab_test").geturl()
    kwargs = {
        "user": parsed.username,
        "password": parsed.password,
        "host": parsed.hostname,
        "port": parsed.port,
    }

    connection = psycopg2.connect(dbname=parsed.path[1:], **kwargs)
    cursor = connection.cursor()

    # Avoid "CREATE DATABASE cannot run inside a transaction block" error
    connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        cursor.execute("CREATE DATABASE ocdskingfishercolab_test")

        conn = psycopg2.connect(dbname="ocdskingfishercolab_test", **kwargs)
        cur = conn.cursor()

        try:
            cur.execute("CREATE TABLE collection (id int, source_id text, transform_from_collection_id int)")
            cur.execute("CREATE TABLE release (id int, collection_id int, ocid text, data_id int, release_date text)")
            cur.execute("CREATE TABLE record (id int, collection_id int, ocid text, data_id int)")
            cur.execute("CREATE TABLE data (id int, data jsonb)")

            cur.execute("INSERT INTO collection VALUES (1, 'scotland', NULL)")
            cur.execute("INSERT INTO collection VALUES (2, 'paraguay_dncp_records', NULL)")
            cur.execute("INSERT INTO collection VALUES (3, 'paraguay_dncp_releases', NULL)")
            cur.execute("INSERT INTO collection VALUES (4, 'paraguay_dncp_releases', 3)")
            cur.execute("INSERT INTO collection VALUES (5, 'paraguay_dncp_releases', 4)")

            cur.execute("INSERT INTO release VALUES (1, 1, 'ocds-213czf-1', 1, '2000')")
            cur.execute("""INSERT INTO data VALUES (1, '{"ocid":"ocds-213czf-1","date":"2000"}'::jsonb)""")

            cur.execute("INSERT INTO release VALUES (2, 1, 'ocds-213czf-1', 2, '2001')")
            cur.execute("""INSERT INTO data VALUES (2, '{"ocid":"ocds-213czf-1","date":"2001"}'::jsonb)""")

            cur.execute("INSERT INTO release VALUES (3, 1, 'ocds-213czf-1/a', 3, '')")
            cur.execute("""INSERT INTO data VALUES (3, '{"ocid":"ocds-213czf-1/a"}'::jsonb)""")

            cur.execute("INSERT INTO record VALUES (1, 1, 'ocds-213czf-2', 4)")
            cur.execute(
                """INSERT INTO data VALUES (4, '{"ocid":"ocds-213czf-2","""
                """"releases":[{"ocid":"ocds-213czf-2"}]}'::jsonb)"""
            )

            conn.commit()

            ipython = get_ipython()
            ipython.run_line_magic("reload_ext", "sql")
            ipython.run_line_magic("sql", created_database_url)
            # Avoid "KeyError: 'DEFAULT'" in some test environments.
            # https://github.com/catherinedevlin/ipython-sql/issues/129
            ipython.run_line_magic("config", 'SqlMagic.style = "NONE"')
            ipython.run_line_magic("config", "SqlMagic.autopandas = True")

            yield cur
        finally:
            cur.close()
            conn.close()
    finally:
        # Close ipython-sql's open connections, to be able to drop the database.
        # ipython-sql's own connection closing logic is broken.
        # https://github.com/catherinedevlin/ipython-sql/issues/170
        for ipython_sql_connection in sql.connection.Connection.connections.values():
            ipython_sql_connection.internal_connection.close()
            ipython_sql_connection.internal_connection.engine.dispose()
        sql.connection.Connection.connections = {}

        cursor.execute("DROP DATABASE ocdskingfishercolab_test")

        cursor.close()
        connection.close()
