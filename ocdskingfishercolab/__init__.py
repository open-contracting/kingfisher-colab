"""
To import all functions:

.. code-block:: python

   from ocdskingfishercolab import (create_connection, reset_connection, authenticate_gspread, authenticate_pydrive,
                                    set_spreadsheet_name, save_dataframe_to_sheet, download_dataframe_as_csv,
                                    download_package_from_ocid, download_package_from_query, get_dataframe_from_query,
                                    get_dataframe_from_cursor)
"""
import json

import gspread
import pandas
import psycopg2
from google.colab import auth, files
from gspread_dataframe import set_with_dataframe
from oauth2client.client import GoogleCredentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

spreadsheet_name = None
conn = None

# Use the same placeholder values as OCDS Kit.
package_metadata = {
    'uri': 'placeholder:',
    'publisher': {
        'name': '',
    },
    'publishedDate': '9999-01-01T00:00:00Z',
    'version': '1.1',
}


class OCDSKingfisherColabError(Exception):
    """Base class for exceptions from within this package"""


class UnknownPackageTypeError(OCDSKingfisherColabError, ValueError):
    """Raised when the provided package type is unknown"""


def create_connection(database, user, password='', host='localhost', port='5432', sslmode=None):
    """
    Creates a connection to the database.

    :returns: a database connection
    :rtype: psycopg2.extensions.connection
    """
    global conn
    if conn and conn.closed:
        reset_connection()
    if not conn:
        conn = psycopg2.connect(dbname=database, user=user, password=password, host=host, port=port, sslmode=sslmode)
    return conn


def reset_connection():
    """
    Closes and resets the connection to the database.

    This does not re-open the connection again.
    """
    global conn
    if conn and not conn.closed:
        try:
            conn.cancel()
            conn.reset()
        except Exception:
            pass

    conn = None


def authenticate_gspread():
    """
    Authenticates the current user and gives the notebook permission to connect to Google Spreadsheets.

    :returns: a `Google Sheets Client <https://gspread.readthedocs.io/en/latest/api.html#client>`__ instance
    :rtype: gspread.Client
    """
    auth.authenticate_user()
    return gspread.authorize(GoogleCredentials.get_application_default())


def authenticate_pydrive():
    """
    Authenticates the current user and gives the notebook permission to connect to Google Drive.

    :returns: a `GoogleDrive <https://gsuitedevs.github.io/PyDrive/docs/build/html/pydrive.html#module-pydrive.drive>`__ instance
    :rtype: pydrive.drive.GoogleDrive
    """  # noqa: E501
    auth.authenticate_user()
    gauth = GoogleAuth()
    gauth.credentials = GoogleCredentials.get_application_default()
    return GoogleDrive(gauth)


def set_spreadsheet_name(name):
    """
    Sets the name of the spreadsheet to which to save.

    Used by :meth:`ocdskingfishercolab.save_dataframe_to_sheet`.

    :param str name: a spreadsheet name
    """
    global spreadsheet_name
    spreadsheet_name = name


def save_dataframe_to_sheet(dataframe, sheetname, prompt=True):
    """
    Saves a data frame to a sheet in Google Sheets, after asking the user for confirmation.

    :param pandas.DataFrame dataframe: a data frame
    :param str sheetname: a sheet name
    :param bool prompt: whether to prompt the user
    """
    if prompt is False or input('Save to Google Sheets? (y/N)') == 'y':
        gc = authenticate_gspread()
        try:
            sheet = gc.open(spreadsheet_name)
        except gspread.SpreadsheetNotFound:
            sheet = gc.create(spreadsheet_name)

        try:
            worksheet = sheet.add_worksheet(sheetname, dataframe.shape[0], dataframe.shape[1])
        except gspread.exceptions.APIError:
            newsheetname = input('{} already exists, enter a new name:'.format(sheetname))
            worksheet = sheet.add_worksheet(newsheetname, dataframe.shape[0], dataframe.shape[1])

        set_with_dataframe(worksheet, dataframe)


def download_dataframe_as_csv(dataframe, filename):
    """
    Converts the data frame to a CSV file, and invokes a browser download of the CSV file to your local computer.

    :param pandas.DataFrame dataframe: a data frame
    :param str filename: a file name
    """
    dataframe.to_csv(filename)
    files.download(filename)


def download_data_as_json(data, filename):
    """
    Dumps the data to a JSON file, and invokes a browser download of the CSV file to your local computer.

    :param data: JSON-serializable data
    :param str filename: a file name
    """
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    files.download(filename)


def download_package_from_query(sql, params=None, package_type=None):
    """
    Executes a SQL statement that SELECTs only the ``data`` column of the ``data`` table, and invokes a browser
    download of the packaged data to your local computer.

    :param str sql: a SQL statement
    :param params: the parameters to pass to the SQL statement
    :param str package_type: "record" or "release"
    :raises UnknownPackageTypeError: when the provided package type is unknown
    """
    if package_type not in ('record', 'release'):
        raise UnknownPackageTypeError("package_type argument must be either 'release' or 'record'")

    with conn, conn.cursor() as cur:
        _execute_statement(cur, sql, params)

        data = [row[0] for row in cur]
        if package_type == 'record':
            package = {'records': data}
        elif package_type == 'release':
            package = {'releases': data}

        package.update(package_metadata)
        download_data_as_json(package, '{}_package.json'.format(package_type))


def download_package_from_ocid(collection_id, ocid, package_type):
    """
    Selects all releases with the given ocid from the given collection, and invokes a browser download of the packaged
    releases to your local computer.

    :param int collection_id: a collection's ID
    :param str ocid: an OCID
    :param str package_type: "record" or "release"
    :raises UnknownPackageTypeError: when the provided package type is unknown
    """
    if package_type not in ('record', 'release'):
        raise UnknownPackageTypeError("package_type argument must be either 'release' or 'record'")

    sql = """
    SELECT data
    FROM data
    JOIN release ON data.id = release.data_id
    WHERE collection_id = %(collection_id)s AND ocid = %(ocid)s
    """

    params = {'ocid': ocid, 'collection_id': collection_id}

    with conn, conn.cursor() as cur:
        _execute_statement(cur, sql, params)

        data = [row[0] for row in cur]
        if package_type == 'record':
            package = {'records': [{'ocid': ocid, 'releases': data}]}
        elif package_type == 'release':
            package = {'releases': data}

        package.update(package_metadata)
        download_data_as_json(package, '{}_{}_package.json'.format(ocid, package_type))


def get_dataframe_from_query(sql, params=None):
    """
    Executes a SQL statement and returns the results as a data frame.

    :param str sql: a SQL statement
    :param params: the parameters to pass to the SQL statement
    :returns: The results as a data frame
    :rtype: pandas.DataFrame
    """
    with conn, conn.cursor() as cur:
        _execute_statement(cur, sql, params)
        return get_dataframe_from_cursor(cur)


def get_dataframe_from_cursor(cur):
    """
    Accepts a database cursor after a SQL statement has been executed and returns the results as a data frame.

    :param psycopg2.extensions.cursor cur: a database cursor
    :returns: The results as a data frame
    :rtype: pandas.DataFrame
    """
    headers = [description[0] for description in cur.description]
    return pandas.DataFrame(cur.fetchall(), columns=headers)


def _execute_statement(cur, sql, params):
    try:
        cur.execute(sql, params)
    except psycopg2.Error:
        cur.execute('rollback')
        raise
