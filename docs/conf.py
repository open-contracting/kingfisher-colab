# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "OCDS Kingfisher Colab"
copyright = "2019, Open Contracting Partnership"
author = "Open Contracting Partnership"

version = "0.6.0"
release = version

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
html_static_path = []

# -- Extension configuration -------------------------------------------------

autodoc_default_options = {
    "members": None,
    "member-order": "bysource",
}
autodoc_typehints = "description"
autodoc_type_aliases = {}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

extlinks = {
    "commit": ("https://github.com/open-contracting/kingfisher-colab/commit/%s", "%s"),
    "ipython-sql": (
        "https://github.com/catherinedevlin/ipython-sql/blob/b24ac6e9410416eafde86ae22fd8d6f34acbe05d/%s",
        None,
    ),
}
