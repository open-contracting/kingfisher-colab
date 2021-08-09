import json
import os
import warnings
from urllib.parse import urljoin

import flattentool
import gspread
import requests
import sql
from sqlalchemy.exc import ResourceClosedError

try:
    from google.colab import auth, files
except ImportError:
    # Assume we are in a testing environment.
    from unittest.mock import Mock

    files = Mock()
    files.download.return_value = None
from gspread_dataframe import set_with_dataframe
from IPython import get_ipython
from IPython.display import HTML
from libcoveocds.config import LibCoveOCDSConfig
from notebook import notebookapp
from oauth2client.client import GoogleCredentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Monkeypatch ipython-sql's sql run function, to add a comment linking to the
# colab notebook that it's run from
# We monkeypatch run(), but don't call it directly below, as calling the magic
# will handle connections for us.
old_run = sql.run.run


def run(conn, _sql, *args, **kwargs):
    try:
        comment = '/* https://colab.research.google.com/drive/{} */'.format(_notebook_id())
    except KeyError:
        comment = "/* run from a notebook, but no colab id */"
    return old_run(conn, comment + _sql, *args, **kwargs)


sql.run.run = run


spreadsheet_name = None

# Use the same placeholder values as OCDS Kit.
package_metadata = {
    'uri': 'placeholder:',
    'publisher': {
        'name': '',
    },
    'publishedDate': '9999-01-01T00:00:00Z',
    'version': '1.1',
}


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
    Returns, as a ResultSet or DataFrame, a list of source IDs matching the given pattern.

    :param str pattern: a substring, like "paraguay"
    :returns: the results as a pandas DataFrame or an ipython-sql `ResultSet <https://github.com/catherinedevlin/ipython-sql/blob/b24ac6e9410416eafde86ae22fd8d6f34acbe05d/src/sql/run.py#L99>`__, depending on whether ``%config SqlMagic.autopandas`` is ``True`` or ``False`` respectively. This is the same behaviour as ipython-sql's ``%sql`` magic.
    :rtype: pandas.DataFrame or sql.run.ResultSet
    """  # noqa: E501
    sql = """
    SELECT source_id
    FROM collection
    WHERE source_id ILIKE :pattern
    GROUP BY source_id
    ORDER BY source_id
    """

    pattern = f'%{pattern}%'

    # This inspects locals to find pattern
    return get_ipython().magic(f'sql {sql}')


def list_collections(source_id):
    """
    Returns, as a ResultSet or DataFrame, a list of collections with the given source ID.

    :param str source_id: a source ID
    :returns: the results as a pandas DataFrame or an ipython-sql `ResultSet <https://github.com/catherinedevlin/ipython-sql/blob/b24ac6e9410416eafde86ae22fd8d6f34acbe05d/src/sql/run.py#L99>`__, depending on whether ``%config SqlMagic.autopandas`` is ``True`` or ``False`` respectively. This is the same behaviour as ipython-sql's ``%sql`` magic.
    :rtype: pandas.DataFrame or sql.run.ResultSet
    """  # noqa: E501
    sql = """
    SELECT *
    FROM collection
    WHERE source_id = :source_id
    ORDER BY id DESC
    """

    # This inspects locals to find source_id
    return get_ipython().magic(f'sql {sql}')


def set_search_path(schema_name):
    """
    Sets the `search_path <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-SEARCH-PATH>`__
    to the given schema, followed by the ``public`` schema.

    :param str schema_name: a schema name
    """
    try:
        get_ipython().magic(f'sql SET search_path = {schema_name}, public')
    # https://github.com/catherinedevlin/ipython-sql/issues/191
    except ResourceClosedError:
        pass


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


def get_ipython_sql_resultset_from_query(sql):
    """
    Executes a SQL statement and returns a ResultSet.

    Parameters are taken from the scope this function is called from (same behaviour as ipython-sql's ``%sql`` magic).

    :param str sql: a SQL statement
    :returns: the results as a `ResultSet <https://github.com/catherinedevlin/ipython-sql/blob/b24ac6e9410416eafde86ae22fd8d6f34acbe05d/src/sql/run.py#L99>`__
    :rtype: sql.run.ResultSet
    """  # noqa: E501
    ipython = get_ipython()
    autopandas = ipython.magic('config SqlMagic.autopandas')
    # Disable autopandas, so we know that the sql magic call will always return
    # a ResultSet (rather than a pandas DataFrame). Since the DataFrame would
    # be created from the ResultSet, it would be less efficient.
    if autopandas:
        ipython.magic('config SqlMagic.autopandas=False')
    # Use ipython.run_line_magic instead of ipython.magic here
    # to get variables from the scope of the ipython cell,
    # instead of the scope in this function.
    results = ipython.run_line_magic('sql', sql)
    if autopandas:
        ipython.magic('config SqlMagic.autopandas=True')
    return results


def download_package_from_query(sql, package_type=None):
    """
    Executes a SQL statement that SELECTs only the ``data`` column of the ``data`` table, and invokes a browser
    download of the packaged data to your local computer.

    :param str sql: a SQL statement
    :param str package_type: "record" or "release"
    :raises UnknownPackageTypeError: when the provided package type is unknown
    """
    if package_type not in ('record', 'release'):
        raise UnknownPackageTypeError("package_type argument must be either 'release' or 'record'")

    data = [row[0] for row in get_ipython_sql_resultset_from_query(sql)]

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
    WHERE collection_id = :collection_id AND ocid = :ocid
    ORDER BY data->>'date' DESC
    """

    ipython = get_ipython()
    autopandas = ipython.magic('config SqlMagic.autopandas')
    # Disable autopandas, so we know that the sql magic call will always return
    # a ResultSet (rather than a pandas DataFrame). Since the DataFrame would
    # be created from the ResultSet, it would be less efficient.
    if autopandas:
        ipython.magic('config SqlMagic.autopandas=False')
    # This inspects locals to find ocid and collection_id
    results = ipython.magic(f'sql {sql}')
    if autopandas:
        ipython.magic('config SqlMagic.autopandas=True')

    data = [row[0] for row in results]

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
    with open(filename.replace(os.sep, '_'), 'w') as f:
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


def render_json(json_string):
    """
    Renders JSON into collapsible HTML.

    :param json_string: JSON-deserializable string
    """
    if not isinstance(json_string, str):
        json_string = json.dumps(json_string)
    return HTML(f"""
        <script
        src="https://cdn.jsdelivr.net/gh/caldwell/renderjson@master/renderjson.js">
        </script>
        <script>
        renderjson.set_show_to_level(1)
        document.body.appendChild(renderjson({json_string}))
        new ResizeObserver(google.colab.output.resizeIframeToContent).observe(document.body)
        </script>
        """)


class OCDSKingfisherColabError(Exception):
    """Base class for exceptions from within this package"""


class UnknownPackageTypeError(OCDSKingfisherColabError, ValueError):
    """Raised when the provided package type is unknown"""
