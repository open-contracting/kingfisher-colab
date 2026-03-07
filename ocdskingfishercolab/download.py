"""Write and download data."""

import json
import os

from ocdskingfishercolab.exceptions import UnknownPackageTypeError
from ocdskingfishercolab.sql import _pluck

try:
    from google.colab import files
except ImportError:
    # Assume we are in a testing environment.
    from unittest.mock import Mock

    files = Mock()
    files.download.return_value = None

# Use the same placeholder values as OCDS Kit.
package_metadata = {
    "uri": "placeholder:",
    "publisher": {
        "name": "",
    },
    "publishedDate": "9999-01-01T00:00:00Z",
    "version": "1.1",
}


def write_data_as_json(data, filename):
    """
    Dump the data to a JSON file.

    :param data: JSON-serializable data
    :param str filename: a file name
    """
    with open(filename.replace(os.sep, "_"), "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def download_dataframe_as_csv(dataframe, filename):
    """
    Convert the data frame to a CSV file, and invoke a browser download of the CSV file to your local computer.

    :param pandas.DataFrame dataframe: a data frame
    :param str filename: a file name
    """
    dataframe.to_csv(filename)
    files.download(filename)


def download_data_as_json(data, filename):
    """
    Dump the data to a JSON file, and invoke a browser download of the CSV file to your local computer.

    :param data: JSON-serializable data
    :param str filename: a file name
    """
    write_data_as_json(data, filename)
    files.download(filename)


def download_package_from_query(sql, package_type=None):
    """
    Execute a SQL statement that SELECTs only the ``data`` column of the ``data`` table, and invoke a browser
    download of the packaged data to your local computer.

    :param str sql: a SQL statement
    :param str package_type: "release" or "record"
    :raises UnknownPackageTypeError: when the provided package type is unknown
    """
    if package_type not in {"release", "record"}:
        raise UnknownPackageTypeError("package_type argument must be either 'release' or 'record'")

    data = _pluck(sql)

    if package_type == "record":
        package = {"records": data}
    elif package_type == "release":
        package = {"releases": data}

    package.update(package_metadata)
    download_data_as_json(package, f"{package_type}_package.json")


def download_package_from_ocid(collection_id, ocid, package_type):
    """
    Select all releases with the given ocid from the given collection, and invoke a browser download of the packaged
    releases to your local computer.

    :param int collection_id: a collection's ID
    :param str ocid: an OCID
    :param str package_type: "release" or "record"
    :raises UnknownPackageTypeError: when the provided package type is unknown
    """
    if package_type not in {"release", "record"}:
        raise UnknownPackageTypeError("package_type argument must be either 'release' or 'record'")

    sql = """
    SELECT
        data
    FROM
        release
        JOIN data ON data.id = data_id
    WHERE
        collection_id = :_collection_id
        AND ocid = :_ocid
    ORDER BY
        release_date DESC
    """

    data = _pluck(sql, _collection_id=collection_id, _ocid=ocid)

    if package_type == "record":
        package = {"records": [{"ocid": ocid, "releases": data}]}
    elif package_type == "release":
        package = {"releases": data}

    package.update(package_metadata)
    download_data_as_json(package, f"{ocid}_{package_type}_package.json")
