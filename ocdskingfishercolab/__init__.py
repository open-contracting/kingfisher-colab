import json
import warnings
from urllib.parse import urljoin

import flattentool
import gspread
import pandas
import psycopg2
import requests
from google.colab import auth, files
from gspread_dataframe import set_with_dataframe
from libcoveocds.config import LibCoveOCDSConfig
from notebook import notebookapp
from oauth2client.client import GoogleCredentials
from psycopg2.sql import SQL, Identifier
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


def list_source_ids(pattern=''):
    """
    Returns, as a data frame, a list of source IDs matching the given pattern.

    :param str pattern: a substring, like "paraguay"
    :returns: the results as a data frame
    :rtype: pandas.DataFrame
    """
    sql = """
    SELECT source_id
    FROM collection
    WHERE source_id ILIKE %(pattern)s
    GROUP BY source_id
    ORDER BY source_id
    """

    return get_dataframe_from_query(sql, {'pattern': '%{}%'.format(pattern)})


def list_collections(source_id):
    """
    Returns, as a data frame, a list of collections with the given source ID.

    :param str source_id: a source ID
    :returns: the results as a data frame
    :rtype: pandas.DataFrame
    """
    sql = """
    SELECT *
    FROM collection
    WHERE source_id = %(source_id)s
    ORDER BY id DESC
    """

    return get_dataframe_from_query(sql, {'source_id': source_id})


def set_search_path(schema_name):
    """
    Sets the `search_path <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-SEARCH-PATH>`__
    to the given schema, followed by the ``public`` schema.

    :param str schema_name: a schema name
    """
    with conn, conn.cursor() as cur:
        execute_statement(cur, SQL("SET search_path = {}, public").format(Identifier(schema_name)))


def execute_statement(cur, sql, params=None):
    """
    Executes a SQL statement, adding a comment with a link to the notebook for database administrators.

    :param psycopg2.extensions.cursor cur: a database cursor
    :param str sql: a SQL statement
    :param params: the parameters to pass to the SQL statement
    """
    if not params:
        params = {}

    comment = '/* https://colab.research.google.com/drive/{} */'.format(_notebook_id())
    if not isinstance(sql, str):
        comment = SQL(comment)

    try:
        cur.execute(comment + sql, params)
    except psycopg2.Error:
        cur.execute('rollback')
        raise


def get_list_from_query(sql, params=None):
    """
    Executes a SQL statement and returns the results as a list of tuples.

    :param str sql: a SQL statement
    :param params: the parameters to pass to the SQL statement
    :returns: the results as a list of tuples
    :rtype: list
    """
    with conn, conn.cursor() as cur:
        execute_statement(cur, sql, params)
        return cur.fetchall()


def get_dataframe_from_query(sql, params=None):
    """
    Executes a SQL statement and returns the results as a data frame.

    :param str sql: a SQL statement
    :param params: the parameters to pass to the SQL statement
    :returns: the results as a data frame
    :rtype: pandas.DataFrame
    """
    with conn, conn.cursor() as cur:
        execute_statement(cur, sql, params)
        return get_dataframe_from_cursor(cur)


def get_dataframe_from_cursor(cur):
    """
    Accepts a database cursor after a SQL statement has been executed and returns the results as a data frame.

    :param psycopg2.extensions.cursor cur: a database cursor
    :returns: the results as a data frame
    :rtype: pandas.DataFrame
    """
    headers = [description[0] for description in cur.description]
    return pandas.DataFrame(cur.fetchall(), columns=headers)


def save_dataframe_to_sheet(dataframe, sheetname, prompt=True):
    """
    Saves a data frame to a worksheet in Google Sheets, after asking the user for confirmation.

    Use :meth:`ocdskingfishercolab.set_spreadsheet_name` to set the spreadsheet name.

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


def save_dataframe_to_spreadsheet(dataframe, name):
    """
    Dumps the ``release_package`` column of a data frame to a JSON file, converts the JSON file to an Excel file,
    and uploads the Excel file to Google Drive.

    :param pandas.DataFrame dataframe: a data frame
    :param str name: the basename of the Excel file to write
    """
    write_data_as_json(dataframe['release_package'][0], 'release_package.json')

    # Use similar code to Toucan.
    config = LibCoveOCDSConfig().config
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')  # flattentool uses UserWarning, so we can't set a specific category

        flattentool.flatten(
            'release_package.json',
            main_sheet_name=config['root_list_path'],
            root_list_path=config['root_list_path'],
            root_id=config['root_id'],
            schema=config['schema_version_choices']['1.1'][1] + 'release-schema.json',
            disable_local_refs=config['flatten_tool']['disable_local_refs'],
            remove_empty_schema_columns=config['flatten_tool']['remove_empty_schema_columns'],
            root_is_list=False,
            output_format='xlsx',
        )

    drive_file = _save_file_to_drive({'title': name + '.xlsx'}, 'flattened.xlsx')
    print('Uploaded file with ID {!r}'.format(drive_file['id']))


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
    write_data_as_json(data, filename)
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

    data = [row[0] for row in get_list_from_query(sql, params)]

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

    data = [row[0] for row in get_list_from_query(sql, params)]

    if package_type == 'record':
        package = {'records': [{'ocid': ocid, 'releases': data}]}
    elif package_type == 'release':
        package = {'releases': data}

    package.update(package_metadata)
    download_data_as_json(package, '{}_{}_package.json'.format(ocid, package_type))


def write_data_as_json(data, filename):
    """
    Dumps the data to a JSON file.

    :param data: JSON-serializable data
    :param str filename: a file name
    """
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _notebook_id():
    server = next(notebookapp.list_running_servers())
    return requests.get(urljoin(server['url'], 'api/sessions')).json()[0]['path'][7:]  # fileId=


def _save_file_to_drive(metadata, filename):
    drive = authenticate_pydrive()
    drive_file = drive.CreateFile(metadata)
    drive_file.SetContentFile(filename)
    drive_file.Upload()
    return drive_file


class OCDSKingfisherColabError(Exception):
    """Base class for exceptions from within this package"""


class UnknownPackageTypeError(OCDSKingfisherColabError, ValueError):
    """Raised when the provided package type is unknown"""
