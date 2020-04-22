Changelog
=========

0.2.2 (2020-04-22)
------------------

Added
~~~~~

-  :meth:`ocdskingfishercolab.set_search_path`
-  :meth:`ocdskingfishercolab.get_list_from_query`

Fixed
~~~~~

-  :meth:`~ocdskingfishercolab.execute_statement` no longer has a mutable default argument value.

0.2.1 (2020-04-21)
------------------

Fixed
~~~~~

-  :meth:`~ocdskingfishercolab.execute_statement` no longer errors if given a ``psycopg2.sql.Composable``.

0.2.0 (2020-04-21)
------------------

**Upgrade instructions:**

-  Upgrade to 0.1.x if you have not already, and address any deprecation warnings. Then, upgrade to 0.2.x.
-  Install alembic and SQLAlchemy separately, if used in the notebook.

Added
~~~~~

-  :meth:`~ocdskingfishercolab.list_source_ids`
-  :meth:`~ocdskingfishercolab.list_collections`
-  :meth:`~ocdskingfishercolab.execute_statement`
-  :meth:`~ocdskingfishercolab.save_dataframe_to_spreadsheet`
-  :meth:`~ocdskingfishercolab.download_data_as_json`
-  :meth:`~ocdskingfishercolab.write_data_as_json`
-  Add a comment to all SQL queries with a link to the notebook, for database administrators.

Changed
~~~~~~~

-  **Backwards-incompatible**: The methods deprecated in 0.1.x are removed.
-  **Backwards-incompatible**: alembic and SQLAlchemy are no longer installed.

Fixed
~~~~~

-  :meth:`~ocdskingfishercolab.download_package_from_ocid` returns structurally correct records. Previously, the ``ocid`` field was at the package-level instead of the record-level.
-  :meth:`~ocdskingfishercolab.download_package_from_ocid` and :meth:`~ocdskingfishercolab.download_package_from_query` return structurally correct packages. Previously, required fields were omitted.

0.1.1 (2020-03-20)
------------------

Fixed
~~~~~

-  The deprecated methods ``output_notebook`` and ``get_results`` return values again.

0.1.0 (2020-03-20)
------------------

**Upgrade instructions:**

-  Use ``pip install 'ocdskingfishercolab<0.2'`` instead of any previous ``pip install`` command.
-  Import :mod:`ocdskingfishercolab` instead of ``kingfishercolab``.

Added
~~~~~

-  :meth:`~ocdskingfishercolab.download_package_from_query`

Changed
~~~~~~~

-  **Backwards-incompatible**: Renamed package from kingfishercolab to ocdskingfishercolab.
-  Renamed methods for consistent style. Old-style methods are deprecated:

   - ``saveToSheets`` is now :meth:`~ocdskingfishercolab.save_dataframe_to_sheet`
   - ``saveStraightToSheets`` is now :meth:`~ocdskingfishercolab.save_dataframe_to_sheet` with ``prompt=False``
   - ``saveToCSV`` is now :meth:`~ocdskingfishercolab.download_dataframe_as_csv`
   - ``downloadReleases`` is now :meth:`~ocdskingfishercolab.download_package_from_ocid`
   - ``output_notebook`` is now :meth:`~ocdskingfishercolab.get_dataframe_from_query`
   - ``getResults`` is now :meth:`~ocdskingfishercolab.get_dataframe_from_cursor`

-  :meth:`~ocdskingfishercolab.get_dataframe_from_query` raises an error instead of returning an error.
-  :meth:`~ocdskingfishercolab.download_package_from_ocid` raises an error instead of printing a message.

0.0.1 (2020-03-20)
------------------

Initial release.
