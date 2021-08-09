Changelog
=========

0.3.5 (2021-08-09)
------------------

Added
~~~~~

-  Add :meth:`~ocdskingfishercolab.render_json` to render JSON into collapsible HTML. 

0.3.4 (2021-04-16)
------------------

Fixed
~~~~~

-  :meth:`~ocdskingfishercolab.set_search_path` no longer outputs an error message.

0.3.3 (2021-04-10)
------------------

Added
~~~~~

-  Add Python wheels distribution.

0.3.2 (2021-02-16)
------------------

Fixed
~~~~~

-  Set the minimum version of ipython-sql.

0.3.1 (2021-02-16)
------------------

Fixed
~~~~~

-  Fixed dependencies to install cleanly both locally and on Google Colaboratory.

0.3.0 (2020-12-15)
------------------

Changed
~~~~~~~

-  Refactor to build upon `ipython-sql <https://pypi.org/project/ipython-sql/>`__.
   Removes several functions that can be replaced with direct usage of ipython-sql magics in the notebook, and replace all remaining sql calls with calls to ipython-sql.

   Replacements (must run ``%load_ext sql`` first, and ``%config SqlMagic.autopandas = True`` to get a pandas ``DataFrame``):

   -  ``ocdskingfishercolab.create_connection`` — replaced by using an ipython-sql connection string, e.g. ``%sql postgresql://user:pass@host/db``
   -  ``ocdskingfishercolab.execute_statement``, ``ocdskingfishercolab.get_dataframe_from_cursor`` and ``ocdskingfishercolab.get_dataframe_from_query`` — replaced by ipython-sql's ``%sql`` magic, e.g. ``%sql SELECT a FROM b``
   -  ``ocdskingfishercolab.get_list_from_query`` — replaced by :meth:`ocdskingfishercolab.get_ipython_sql_resultset_from_query`. This returns an `ipython-sql ResultSet <https://pypi.org/project/ipython-sql/#examples>`__, the type returned by the ``%sql%`` magic when ``autopandas`` is off. It behaves like a list, but with extra methods.
   -  :meth:`ocdskingfishercolab.download_package_from_query` no longer takes a ``params`` argument, and instead uses variables from the local scope, to be consisent with the ipython-sql's ``%sql`` magic.

   There's a shared (but not public) `colab notebook of examples run against live kingfisher <https://colab.research.google.com/drive/1cUYY4on72831DPSiQ_JLxJEY2uGTfVrN#scrollTo=I-QPDbliMVXC>`__.

-  :meth:`~ocdskingfishercolab.create_connection` creates a new connection if the current connection is closed.
-  :meth:`~ocdskingfishercolab.download_package_from_ocid` orders packaged releases in reverse date order.
-  Remove :meth:`~ocdskingfishercolab.reset_connection`.

Fixed
~~~~~

-  :meth:`~ocdskingfishercolab.write_data_as_json` replaces path separators with underscores in filenames.

0.2.2 (2020-04-22)
------------------

Added
~~~~~

-  :meth:`~ocdskingfishercolab.set_search_path`
-  :meth:`~ocdskingfishercolab.get_list_from_query`

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

0.1.1 (2020-04-20)
------------------

Fixed
~~~~~

-  The deprecated methods ``output_notebook`` and ``get_results`` return values again.

0.1.0 (2020-04-20)
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

0.0.1 (2020-04-20)
------------------

Initial release.
