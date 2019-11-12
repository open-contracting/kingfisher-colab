Functions
=========

All functions are in the package; to import them use something like:

.. code-block:: python

    from kingfishercolab import create_connection, authenticate_gspread, getResults, saveToCSV, saveToSheets, saveStraightToSheets, downloadReleases, output_notebook, set_spreadsheet_name, authenticate_pydrive, downloadReleases

create_connection
-----------------

Creates a connection to the database.

reset_connection
----------------

Closes and resets connection to database.

This does not re-open the connection again.

authenticate_gspread
--------------------

Authenticates the current user and gives the notebook permission to connect to Google Spreadsheets.

authenticate_pydrive
--------------------

Authenticates the current user and gives the notebook permission to connect to Google Drive.

getResults
----------

Takes in a database cursor and returns a Pandas DataFrame.

saveToCSV
---------

Takes in a Pandas DataFrame and save the data to a file in the notebook.

set_spreadsheet_name
--------------------

Sets the name of the spreadsheet to save. Used by saveStraightToSheets.

saveStraightToSheets
--------------------

Saves a DataFrame straight to a Google Spreadsheet.

saveToSheets
------------

The same as saveStraightToSheets, except the user is prompted if they are sure first.

downloadReleases
----------------

Downloads some releases into a file in the notebook.

output_gsheet
-------------

This has not been coded yet.

output_flattened_gsheet
-----------------------

This has not been coded yet.

output_notebook
---------------

Runs a SQL query and returns results to the notebook.

download_json
-------------

This has not been coded yet.





