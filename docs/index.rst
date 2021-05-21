OCDS Kingfisher Colab |release|
===============================

.. include:: ../README.rst

.. toctree::
   :hidden:

   changelog.rst

Troubleshooting
---------------

Using Jupyter Notebook
^^^^^^^^^^^^^^^^^^^^^^

If you are using Kingfisher Colab in a Jupyter Notebook (not on Google Colaboratory), you need to:

#. Install the ``google-colab`` package:

   .. code-block:: bash

      pip install google-colab

#. Upgrade the ``ipykernel`` package:

   .. code-block:: bash

      pip install --upgrade ipykernel

Using JSON operators with the %sql magic
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using the ipython-sql ``%sql`` line magic, you must avoid spaces around JSON operators.

E.g. ``data->'ocid'`` not ``data -> 'ocid'``

API
---

.. automodule:: ocdskingfishercolab
   :members:
