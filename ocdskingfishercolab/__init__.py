"""
Functions for use in `Google Colaboratory <https://colab.research.google.com/notebooks/intro.ipynb>`__ notebooks.

To import all functions:

.. code-block:: python

   from ocdskingfishercolab import (create_connection, authenticate_gspread, getResults, saveToCSV, saveToSheets,
                                    saveStraightToSheets, downloadReleases, output_notebook, set_spreadsheet_name,
                                    authenticate_pydrive, downloadReleases)
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
    """
    global conn
    if conn and conn.closed > 0:
        reset_connection()
    if not conn:
        conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,
        )
    return conn


def reset_connection():
    """
    Closes and resets the connection to the database.

    This does not re-open the connection again.
    """
    global conn
    if conn is not None and conn.closed != 0:
        try:
            conn.cancel()
            conn.reset()
        except:
            pass

    conn = None


def authenticate_gspread():
    """
    Authenticates the current user and gives the notebook permission to connect to Google Spreadsheets.
    """
    auth.authenticate_user()
    gc = gspread.authorize(GoogleCredentials.get_application_default())
    return gc


def authenticate_pydrive():
    """
    Authenticates the current user and gives the notebook permission to connect to Google Drive.
    """
    auth.authenticate_user()
    gauth = GoogleAuth()
    gauth.credentials = GoogleCredentials.get_application_default()
    drive = GoogleDrive(gauth)
    return drive


def getResults(cur):
    """
    Takes in a database cursor and returns a Pandas DataFrame.
    """
    headers = [desc[0] for desc in cur.description]
    results = pandas.DataFrame(cur.fetchall(), columns=headers)
    return results


def saveToCSV(dataframe, filename):
    """
    Takes in a Pandas DataFrame and save the data to a file in the notebook.
    """
    dataframe.to_csv(filename)
    files.download(filename)


def set_spreadsheet_name(name):
    """
    Sets the name of the spreadsheet to save. Used by saveStraightToSheets.
    """
    global spreadsheet_name
    spreadsheet_name = name


# option to bypass confirmation in save to sheets
def saveStraightToSheets(dataframe, sheetname):
    """
    Saves a DataFrame straight to a Google Spreadsheet.
    """
    gc = authenticate_gspread()
    # open or create gSheet
    try:
        gSheet = gc.open(spreadsheet_name)
    except Exception:
        gSheet = gc.create(spreadsheet_name)

    try:
        worksheet = gSheet.add_worksheet(sheetname, dataframe.shape[0], dataframe.shape[1])
    except Exception:
        newsheetname = input(sheetname + " already exists, enter a different name: ")
        worksheet = gSheet.add_worksheet(newsheetname, dataframe.shape[0], dataframe.shape[1])

    # save dataframe to worksheet
    set_with_dataframe(worksheet, dataframe)


# saves dataframe to sheets after user confirms yes
def saveToSheets(dataframe, sheetname):
    """
    The same as saveStraightToSheets, except the user is prompted if they are sure first.
    """
    if input("Save to Google Sheets? (y/n)") == 'y':
        saveStraightToSheets(dataframe, sheetname)


def downloadReleases(collection_id, ocid, package_type):
    """
    Downloads some releases into a file in the notebook.
    """
    with conn, conn.cursor() as cur:
        if package_type != 'release' and package_type != 'record':
            print("package_type parameter must be either 'release' or 'record'")
        else:

            querystring = """

          with releases as (

              select
                  ocid,
                  data
              from
                  data
              join
                  release_with_collection on data.id = release_with_collection.data_id
              where collection_id = %(collection_id)s

          )

          SELECT
            jsonb_build_object('releases', jsonb_agg(data)),
            jsonb_build_object(
                'ocid', %(ocid)s,
                'records', jsonb_build_array(jsonb_build_object('releases', jsonb_agg(data)))
            )
          FROM
            releases
          WHERE
            ocid = %(ocid)s

          """

            cur.execute(querystring,
                        {"ocid": ocid, "collection_id": collection_id}
                        )

            file = ocid + '_' + package_type + '_package.json'

            with open(file, 'w') as f:
                if package_type == 'release':
                    json.dump(cur.fetchone()[0], f, indent=2, ensure_ascii=False)
                elif package_type == 'record':
                    json.dump(cur.fetchone()[1], f, indent=2, ensure_ascii=False)

            files.download(file)


def output_notebook(sql, params=None):
    """
    Runs a SQL query and returns results to the notebook.
    """
    with conn, conn.cursor() as cur:
        try:
            cur.execute(sql, params)
            return getResults(cur)
        except Exception as e:
            cur.execute("rollback")
            return e
