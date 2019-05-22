import psycopg2
from google.colab import auth
import gspread
from oauth2client.client import GoogleCredentials
import pandas
from google.colab import files
from gspread_dataframe import set_with_dataframe
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

spreadsheet_name = None
conn = None


def create_connection(database, user, password, host, port='5432'):
    global conn
    
    if conn and conn.closed != 0:
        reset_connection()
    
    if conn is None:
        conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,
        )
    return conn


def reset_connection():
    """Resets closed connections"""
    global conn
    if conn is not None and conn.closed != 0:
        try:
            conn.cancel()
            conn.reset()
        except:
            pass
    
    conn = None


def authenticate_gspread():
    auth.authenticate_user()
    gc = gspread.authorize(GoogleCredentials.get_application_default())
    return gc


def authenticate_pydrive():
    auth.authenticate_user()
    gauth = GoogleAuth()
    gauth.credentials = GoogleCredentials.get_application_default()
    drive = GoogleDrive(gauth)
    return drive


def getResults(cur):
    headers = [desc[0] for desc in cur.description]
    results = pandas.DataFrame(cur.fetchall(), columns=headers)
    return results


def saveToCSV(dataframe, filename):
    dataframe.to_csv(filename)
    files.download(filename)


def set_spreadsheet_name(name):
    global spreadsheet_name
    spreadsheet_name = name

# option to bypass confirmation in save to sheets
def saveStraightToSheets(dataframe, sheetname):
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
    if input("Save to Google Sheets? (y/n)") == 'y':
        saveStraightToSheets(dataframe, sheetname)


def downloadReleases(collection_id, ocid, package_type):
    with conn.cursor() as cur:
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
            jsonb_build_object('releases',jsonb_agg(data)),
            jsonb_build_object('ocid', %(ocid)s ,'records',jsonb_build_array(jsonb_build_object('releases',jsonb_agg(data))))
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
                    json.dump(cur.fetchone()[0], f, indent=2)
                elif package_type == 'record':
                    json.dump(cur.fetchone()[1], f, indent=2)

            files.download(file)


def output_gsheet(workbook_name, sheet_name, sql, params=None):
    pass


def output_flattened_gsheet(workbook_name, sql, params=None):
    'data column needs to be in results'
    pass


def output_notebook(sql, params=None):
    with conn.cursor() as cur:
        try:
            cur.execute(sql, params)
            return getResults(cur)
        except Exception as e:
            cur.execute("rollback")
            return e


def download_json(root_list_path, sql, params=None):
    'data column needs to be in results'
