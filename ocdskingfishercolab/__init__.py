"""
Functions for use in `Google Colaboratory <https://colab.research.google.com/notebooks/intro.ipynb>`__ notebooks.

To import all functions:

.. code-block:: python

   from ocdskingfishercolab import (create_connection, authenticate_gspread, getResults, saveToCSV, saveToSheets,
                                    saveStraightToSheets, downloadReleases, output_notebook, set_spreadsheet_name,
                                    authenticate_pydrive)
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


def create_connection(database, user, password='', host='localhost', port='5432'):
    """
    Creates a connection to the database.

    :returns: a database connection
    :rtype: psycopg2.extensions.connection
    """
    global conn
    if conn and conn.closed:
        reset_connection()
    if not conn:
        conn = psycopg2.connect(dbname=database, user=user, password=password, host=host, port=port)
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


def get_results(cur):
    """
    Accepts a database cursor after a SQL statement has been executed and returns the results as a data frame.

    :param psycopg2.extensions.cursor cur: a database cursor
    :returns: The results as a data frame
    :rtype: pandas.DataFrame
    """
    headers = [desc[0] for desc in cur.description]
    return pandas.DataFrame(cur.fetchall(), columns=headers)


def download_dataframe(dataframe, filename):
    """
    Converts the data frame to a CSV file, and invokes a browser download of the CSV file to your local computer.

    :param pandas.DataFrame dataframe: a data frame
    :param str filename: a file name
    """
    dataframe.to_csv(filename)
    files.download(filename)


def set_spreadsheet_name(name):
    """
    Sets the name of the spreadsheet to save. Used by saveStraightToSheets.

    :param str name: a sheet name
    """
    global spreadsheet_name
    spreadsheet_name = name


def save_to_sheets(dataframe, sheetname, prompt=True):
    """
    Saves a data frame to Google Sheets, after asking the user for confirmation.

    :param pandas.DataFrame dataframe: a data frame
    :param str sheetname: a sheet name
    :param bool prompt: whether to prompt the user
    """
    if prompt is False or input('Save to Google Sheets? (y/N)') == 'y':
        gc = authenticate_gspread()
        try:
            sheet = gc.open(spreadsheet_name)
        except Exception:
            sheet = gc.create(spreadsheet_name)

        try:
            worksheet = sheet.add_worksheet(sheetname, dataframe.shape[0], dataframe.shape[1])
        except Exception:
            newsheetname = input('{} already exists, enter a new name:'.format(sheetname))
            worksheet = sheet.add_worksheet(newsheetname, dataframe.shape[0], dataframe.shape[1])

        set_with_dataframe(worksheet, dataframe)


def download_releases(collection_id, ocid, package_type):
    """
    Selects all releases with the given ocid from the given collection, and invokes a browser download of the packaged
    releases to your local computer.

    :param int collection_id: a collection's ID
    :param str ocid: an OCID
    :param str package_type: "record" or "release"
    """
    if package_type not in ('record', 'release'):
        print("package_type parameter must be either 'release' or 'record'")
        return

    statement = """
    SELECT jsonb_agg(data)
    FROM data
    JOIN release ON data.id = release.data_id
    WHERE collection_id = %(collection_id)s AND ocid = %(ocid)s
    """

    with conn, conn.cursor() as cur:
        cur.execute(statement, {'ocid': ocid, 'collection_id': collection_id})

        package = {'releases': cur.fetchone()[0]}
        if package_type == 'record':
            package = {'ocid': ocid, 'records': [package]}

        filename = '{}_{}_package.json'.format(ocid, package_type)
        with open(filename, 'w') as f:
            json.dump(package, f, indent=2, ensure_ascii=False)

        files.download(filename)


def output_notebook(sql, params=None):
    """
    Executes a SQL statement and returns the results as a data frame.

    :param str sql: a SQL statement
    :param params: the parameters to pass to the SQL statement
    :returns: The results as a data frame
    :rtype: pandas.DataFrame
    """
    with conn, conn.cursor() as cur:
        try:
            cur.execute(sql, params)
            return get_results(cur)
        except Exception as e:
            cur.execute('rollback')
            return e


def downloadReleases(*args, **kwargs):
    warnings.warn('downloadReleases() is deprecated. Use download_releases() instead.',
                  DeprecationWarning, stacklevel=2)
    download_releases(*args, **kwargs)


def getResults(*args, **kwargs):
    warnings.warn('getResults() is deprecated. Use get_results() instead.',
                  DeprecationWarning, stacklevel=2)
    get_results(*args, **kwargs)


def saveToCSV(*args, **kwargs):
    warnings.warn('saveToCSV() is deprecated. Use download_dataframe() instead.',
                  DeprecationWarning, stacklevel=2)
    download_dataframe(*args, **kwargs)


def saveToSheets(*args, **kwargs):
    warnings.warn('saveToSheets() is deprecated. Use save_to_sheets() instead.',
                  DeprecationWarning, stacklevel=2)
    save_to_sheets(*args, **kwargs)


def saveStraightToSheets(dataframe, sheetname):
    warnings.warn('saveStraightToSheets() is deprecated. Use save_to_sheets(..., prompt=False) instead.',
                  DeprecationWarning, stacklevel=2)
    save_to_sheets(*args, **kwargs, prompt=False)
