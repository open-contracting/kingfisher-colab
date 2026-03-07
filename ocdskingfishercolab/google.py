"""Google Sheets and Google Drive integration."""

import warnings

import flattentool
import google.auth
import gspread
import httplib2
from flattentool.exceptions import FlattenToolWarning
from gspread_dataframe import set_with_dataframe
from oauth2client.client import GoogleCredentials
from oauth2client.contrib.gce import AppAssertionCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

try:
    from google.colab import auth
except ImportError:
    # Assume we are in a testing environment.
    from unittest.mock import Mock

    auth = Mock()

from ocdskingfishercolab.download import write_data_as_json

# Patch PyDrive2 like at: https://github.com/googlecolab/colabtools/blob/main/google/colab/_import_hooks/_pydrive.py
old_local_webserver_auth = GoogleAuth.LocalWebserverAuth


def _local_web_server_auth(self, *args, **kwargs):
    if isinstance(self.credentials, AppAssertionCredentials):
        self.credentials.refresh(httplib2.Http())
        return None
    return old_local_webserver_auth(self, *args, **kwargs)


GoogleAuth.LocalWebserverAuth = _local_web_server_auth


def authenticate_gspread():
    """
    Authenticate the current user and give the notebook permission to connect to Google Spreadsheets.

    :returns: a `Google Sheets Client <https://gspread.readthedocs.io/en/latest/api.html#client>`__ instance
    :rtype: gspread.Client
    """
    auth.authenticate_user()
    credentials, _ = google.auth.default()
    return gspread.authorize(credentials)


def authenticate_pydrive():
    """
    Authenticate the current user and give the notebook permission to connect to Google Drive.

    :returns: a `GoogleDrive <https://gsuitedevs.github.io/PyDrive/docs/build/html/pydrive.html#module-pydrive.drive>`__ instance
    :rtype: pydrive2.drive.GoogleDrive
    """  # noqa: E501
    auth.authenticate_user()
    gauth = GoogleAuth()
    gauth.credentials = GoogleCredentials.get_application_default()
    return GoogleDrive(gauth)


def _save_file_to_drive(metadata, filename):
    drive = authenticate_pydrive()
    drive_file = drive.CreateFile(metadata)
    drive_file.SetContentFile(filename)
    drive_file.Upload()
    return drive_file


def save_dataframe_to_sheet(spreadsheet_name, dataframe, sheetname, *, prompt=True):
    """
    Save a data frame to a worksheet in Google Sheets, after asking the user for confirmation.

    :param str spreadsheet_name: the name of the spreadsheet
    :param pandas.DataFrame dataframe: a data frame
    :param str sheetname: the name of the sheet to add
    :param bool prompt: whether to prompt the user
    """
    if dataframe.empty:
        print("Data frame is empty.")  # noqa: T201
        return

    if not prompt or input("Save to Google Sheets? (y/N)") == "y":
        gc = authenticate_gspread()
        try:
            sheet = gc.open(spreadsheet_name)
        except gspread.SpreadsheetNotFound:
            sheet = gc.create(spreadsheet_name)

        try:
            worksheet = sheet.add_worksheet(sheetname, dataframe.shape[0], dataframe.shape[1])
        except gspread.exceptions.APIError:
            newsheetname = input(f"{sheetname} already exists, enter a new name:")
            worksheet = sheet.add_worksheet(newsheetname, dataframe.shape[0], dataframe.shape[1])

        set_with_dataframe(worksheet, dataframe)


def save_dataframe_to_spreadsheet(dataframe, name):
    """
    Dump the ``release_package`` column of a data frame to a JSON file, convert the JSON file to an Excel file,
    and upload the Excel file to Google Drive.

    :param pandas.DataFrame dataframe: a data frame
    :param str name: the basename of the Excel file to write
    """
    if dataframe.empty:
        print("Data frame is empty.")  # noqa: T201
        return

    write_data_as_json(dataframe["release_package"][0], "release_package.json")

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FlattenToolWarning)

        flattentool.flatten(
            "release_package.json",
            main_sheet_name="releases",
            root_list_path="releases",
            root_id="ocid",
            schema="https://standard.open-contracting.org/1.1/en/release-schema.json",
            disable_local_refs=True,
            remove_empty_schema_columns=True,
            root_is_list=False,
            output_format="xlsx",
        )

    drive_file = _save_file_to_drive({"title": f"{name}.xlsx"}, "flattened.xlsx")
    print(f"Uploaded file with ID {drive_file['id']!r}")  # noqa: T201
