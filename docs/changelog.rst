Changelog
=========

0.6.0 (2025-11-13)
------------------

Changed
~~~~~~~

-  Upgrade to Python 3.12 to match Google Colab.

0.5.0 (2025-08-14)
------------------

Removed
~~~~~~~

-  Remove support for Jupyter Notebook 6 or earlier.

0.4.0 (2025-05-13)
------------------

Changed
~~~~~~~

-  Add ``spreadsheet_name`` as the first argument to :func:`~ocdskingfishercolab.save_dataframe_to_sheet`.

Removed
~~~~~~~

-  Remove ``ocdskingfishercolab.set_spreadsheet_name``.

0.3.14 (2024-11-25)
-------------------

Fixed
~~~~~

-  Constrain the version of prettytable to be compatible with ipython-sql.

0.3.13 (2023-10-27)
-------------------

Fixed
~~~~~

-  Restore support for Jupyter Notebook 6.

0.3.12 (2023-10-23)
-------------------

Changed
~~~~~~~

-  Upgrade to Python 3.10 and other dependencies to match Google Colab.

0.3.11 (2023-07-04)
-------------------

Added
~~~~~

-  :func:`~ocdskingfishercolab.set_dark_mode`, if *Tools > Settings > Site > Theme* is set to *dark* in Google Colab.
-  :func:`~ocdskingfishercolab.set_light_mode`, for the default Seaborn theme.
-  :func:`~ocdskingfishercolab.format_thousands`.

0.3.10 (2023-02-03)
-------------------

Fixed
~~~~~

-  Pin to SQLAlchemy 1.x.

0.3.9 (2022-06-30)
------------------

Changed
~~~~~~~

-  :func:`~ocdskingfishercolab.save_dataframe_to_sheet` and :func:`~ocdskingfishercolab.save_dataframe_to_spreadsheet` do nothing if the data frame is empty. :commit:`9b83348`
-  :func:`~ocdskingfishercolab.calculate_coverage`: Rename keyword arguments from ``sql`` to ``print_sql`` and ``sql_only`` to ``return_sql``. :commit:`d706145`
-  :func:`~ocdskingfishercolab.calculate_coverage`: Simplify the query if ``"ALL "`` is prefixed to a field that is not within an array. :commit:`869a9d0`
-  :func:`~ocdskingfishercolab.calculate_coverage`: Raise an error if no ``fields`` are provided. :commit:`8896336`

Fixed
~~~~~

-  :func:`~ocdskingfishercolab.calculate_coverage`: Use the correct parent table if ``scope`` is not set. Previously, Kingfisher Colab would not use: :commit:`a7c0936`

   -  ``award_documents``
   -  ``award_items``
   -  ``award_suppliers``
   -  ``contract_documents``
   -  ``contract_items``
   -  ``contract_milestones``
   -  ``contract_implementation_documents``
   -  ``contract_implementation_milestones``
   -  ``contract_implementation_transactions``

-  :func:`~ocdskingfishercolab.calculate_coverage`: Construct correct conditions and warnings if a field is within nested arrays. :commit:`3dced1a`
-  :func:`~ocdskingfishercolab.calculate_coverage`: Use the ``relatedprocesses_summary`` table for fields starting with ``relatedProcesses/``, where appropriate. :commit:`9e6cdb7`
-  :func:`~ocdskingfishercolab.calculate_coverage`: Prefix ``all_`` to the column if ``"ALL "`` is prefixed to the field, to avoid duplicate columns. :commit:`e9427b2`
-  :func:`~ocdskingfishercolab.calculate_coverage`: No longer warn about ``address`` fields. :commit:`e2b8d72`

0.3.8 (2022-04-27)
------------------

Changed
~~~~~~~

-  Removed dependency on `libcoveocds <https://pypi.org/project/libcoveocds/>`__ (GPL).

0.3.7 (2022-03-11)
------------------

Added
~~~~~

-  Add :func:`~ocdskingfishercolab.calculate_coverage` to calculate the co-occurence coverage of a group of fields.

0.3.6 (2021-09-15)
------------------

Changed
-------

- :func:`~ocdskingfishercolab.list_collections`: `source_id` is now an optional argument. If omitted, all collections are returned.

0.3.5 (2021-08-09)
------------------

Added
~~~~~

-  Add :func:`~ocdskingfishercolab.render_json` to render JSON into collapsible HTML.

0.3.4 (2021-04-16)
------------------

Fixed
~~~~~

-  :func:`~ocdskingfishercolab.set_search_path` no longer outputs an error message.

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

-  Fixed dependencies to install cleanly both locally and on Google Colab.

0.3.0 (2020-12-15)
------------------

Changed
~~~~~~~

-  Refactor to build upon `ipython-sql <https://pypi.org/project/ipython-sql/>`__.

   You must run ``%load_ext sql`` first, and ``%config SqlMagic.autopandas = True`` to get a pandas ``DataFrame``.

   -  Remove functions that can be replaced by ipython-sql magics in the notebook.

      -  ``create_connection``: Use an ipython-sql connection string, e.g. ``%sql postgresql://user:pass@host/db``
      -  ``execute_statement``, ``get_dataframe_from_cursor``, ``get_dataframe_from_query``: Use ipython-sql's ``%sql`` magic, e.g. ``%sql SELECT a FROM b``

   -  Replace SQL calls with ipython-sql calls in remaining functions.

      -  ``get_list_from_query``: Use :func:`~ocdskingfishercolab.get_ipython_sql_resultset_from_query`. This returns an `ipython-sql ResultSet <https://pypi.org/project/ipython-sql/#examples>`__, the type returned by the ``%sql%`` magic when ``autopandas`` is off. It behaves like a list, but with extra methods.
      -  :func:`ocdskingfishercolab.download_package_from_query` no longer takes a ``params`` argument, and instead uses variables from the local scope, to be consistent with the ipython-sql's ``%sql`` magic.

   There's a shared (but not public) `colab notebook of examples run against Kingfisher <https://colab.research.google.com/drive/1cUYY4on72831DPSiQ_JLxJEY2uGTfVrN#scrollTo=I-QPDbliMVXC>`__.

-  :func:`~ocdskingfishercolab.create_connection` creates a new connection if the current connection is closed.
-  :func:`~ocdskingfishercolab.download_package_from_ocid` orders packaged releases in reverse date order.
-  Remove :func:`~ocdskingfishercolab.reset_connection`.

Fixed
~~~~~

-  :func:`~ocdskingfishercolab.write_data_as_json` replaces path separators with underscores in filenames.

0.2.2 (2020-04-22)
------------------

Added
~~~~~

-  :func:`~ocdskingfishercolab.set_search_path`
-  :func:`~ocdskingfishercolab.get_list_from_query`

Fixed
~~~~~

-  :func:`~ocdskingfishercolab.execute_statement` no longer has a mutable default argument value.

0.2.1 (2020-04-21)
------------------

Fixed
~~~~~

-  :func:`~ocdskingfishercolab.execute_statement` no longer errors if given a ``psycopg2.sql.Composable``.

0.2.0 (2020-04-21)
------------------

**Upgrade instructions:**

-  Upgrade to 0.1.x if you have not already, and address any deprecation warnings. Then, upgrade to 0.2.x.
-  Install alembic and SQLAlchemy separately, if used in the notebook.

Added
~~~~~

-  :func:`~ocdskingfishercolab.list_source_ids`
-  :func:`~ocdskingfishercolab.list_collections`
-  :func:`~ocdskingfishercolab.execute_statement`
-  :func:`~ocdskingfishercolab.save_dataframe_to_spreadsheet`
-  :func:`~ocdskingfishercolab.download_data_as_json`
-  :func:`~ocdskingfishercolab.write_data_as_json`
-  Add a comment to all SQL queries with a link to the notebook, for database administrators.

Changed
~~~~~~~

-  **Backwards-incompatible**: The methods deprecated in 0.1.x are removed.
-  **Backwards-incompatible**: alembic and SQLAlchemy are no longer installed.

Fixed
~~~~~

-  :func:`~ocdskingfishercolab.download_package_from_ocid` returns structurally correct records. Previously, the ``ocid`` field was at the package-level instead of the record-level.
-  :func:`~ocdskingfishercolab.download_package_from_ocid` and :func:`~ocdskingfishercolab.download_package_from_query` return structurally correct packages. Previously, required fields were omitted.

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

-  :func:`~ocdskingfishercolab.download_package_from_query`

Changed
~~~~~~~

-  **Backwards-incompatible**: Renamed package from kingfishercolab to ocdskingfishercolab.
-  Renamed methods for consistent style. Old-style methods are deprecated:

   - ``saveToSheets`` is now :func:`~ocdskingfishercolab.save_dataframe_to_sheet`
   - ``saveStraightToSheets`` is now :func:`~ocdskingfishercolab.save_dataframe_to_sheet` with ``prompt=False``
   - ``saveToCSV`` is now :func:`~ocdskingfishercolab.download_dataframe_as_csv`
   - ``downloadReleases`` is now :func:`~ocdskingfishercolab.download_package_from_ocid`
   - ``output_notebook`` is now :func:`~ocdskingfishercolab.get_dataframe_from_query`
   - ``getResults`` is now :func:`~ocdskingfishercolab.get_dataframe_from_cursor`

-  :func:`~ocdskingfishercolab.get_dataframe_from_query` raises an error instead of returning an error.
-  :func:`~ocdskingfishercolab.download_package_from_ocid` raises an error instead of printing a message.

0.0.1 (2020-04-20)
------------------

Initial release.
