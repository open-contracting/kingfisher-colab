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


def create_connection(database, user, password, host, port='5432'):
    """
    Creates a connection to the database.

    :returns: a database connection
    :rtype: psycopg2.extensions.connection
    """
    global conn
    if conn and conn.closed:
        reset_connection()
    if not conn:
        conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
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
        except:
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


def getResults(cur):
    """
    Takes in a database cursor and returns a Pandas DataFrame.

    :param psycopg2.extensions.cursor cur: a database cursor
    :returns: The results as a data frame
    :rtype: pandas.DataFrame
    """
    headers = [desc[0] for desc in cur.description]
    return pandas.DataFrame(cur.fetchall(), columns=headers)


def saveToCSV(dataframe, filename):
    """
    Takes in a Pandas DataFrame and save the data to a file in the notebook.

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


def saveStraightToSheets(dataframe, sheetname):
    """
    Saves a data frame to Google Sheets, without asking the user for conformation.

    :param pandas.DataFrame dataframe: a data frame
    :param str sheetname: a sheet name
    """
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


def saveToSheets(dataframe, sheetname):
    """
    Saves a data frame to Google Sheets, after asking the user for confirmation.

    :param pandas.DataFrame dataframe: a data frame
    :param str sheetname: a sheet name
    """
    if input('Save to Google Sheets? (y/N)') == 'y':
        saveStraightToSheets(dataframe, sheetname)


def downloadReleases(collection_id, ocid, package_type):
    """
    Downloads some releases into a file in the notebook.

    :param int collection_id: a collection's ID
    :param str ocid: an OCID
    :param str package_type: "record" or "release"
    """
    if package_type not in ('record', 'release'):
        print("package_type parameter must be either 'release' or 'record'")
        return

    querystring = """
    WITH releases AS (
        SELECT
            ocid,
            data
        FROM data
        JOIN release_with_collection ON data.id = release_with_collection.data_id
        WHERE collection_id = %(collection_id)s
    )
    SELECT
      jsonb_build_object('releases', jsonb_agg(data)),
      jsonb_build_object(
          'ocid', %(ocid)s,
          'records', jsonb_build_array(jsonb_build_object('releases', jsonb_agg(data)))
      )
    FROM releases
    WHERE ocid = %(ocid)s
    """

    with conn, conn.cursor() as cur:
        cur.execute(querystring, {'ocid': ocid, 'collection_id': collection_id})

        file = '{}_{}_package.json'.format(ocid, package_type)

        result = cur.fetchone()
        if package_type == 'release':
            index = 0
        elif package_type == 'record':
            index = 1

        with open(file, 'w') as f:
            json.dump(result[index], f, indent=2, ensure_ascii=False)

        files.download(file)


def output_notebook(sql, params=None):
    """
    Runs a SQL query and returns results to the notebook.

    :param str sql: a SQL statement
    :param params: the parameters to pass to the SQL statement
    :returns: The results as a data frame
    :rtype: pandas.DataFrame
    """
    with conn, conn.cursor() as cur:
        try:
            cur.execute(sql, params)
            return getResults(cur)
        except Exception as e:
            cur.execute("rollback")
            return e
