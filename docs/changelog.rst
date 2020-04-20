Changelog
=========

0.1.1 (2020-03-20)
------------------

Fixed
~~~~~

-  The deprecated methods ``output_notebook`` and ``get_results`` return values again.

0.1.0 (2020-03-20)
------------------

**Upgrade instructions:**

-  Use ``pip install -q 'ocdskingfishercolab<0.2'`` instead of any previous ``pip install`` command.
-  Many methods are now deprecated and will be removed in 0.2.

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
