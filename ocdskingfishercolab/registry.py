"""Data registry utilities."""

import pandas as pd
import requests
from ipywidgets import widgets

DATA_REGISTRY_BASE_URL = "https://data.open-contracting.org/en/"
PUBLICATIONS_URL = f"{DATA_REGISTRY_BASE_URL}publications.json"


def get_publications():
    """Return all publications from the data registry, with a label field added."""
    publications = requests.get(PUBLICATIONS_URL, timeout=10).json()
    for publication in publications:
        publication["label"] = f"{publication['country']} - {publication['title']}"
    return publications


def get_publication_select_box():
    """Return a dropdown widget listing all available publications."""
    return widgets.Dropdown(
        options=sorted([entry["label"] for entry in get_publications()]),
        description="Publication:",
        disabled=False,
    )


def format_coverage(coverage):
    """Return a DataFrame of field paths from a coverage dictionary."""
    if not coverage:
        return pd.DataFrame(columns=["path"])
    fields = (
        pd.DataFrame.from_dict(coverage, orient="index", columns=["count"])
        .reset_index()
        .rename(columns={"index": "path"})
    )
    # Leaves only object members
    fields_table = fields[fields.path.str.contains("[a-z]$")].copy()
    fields_table["path"] = fields_table["path"].str.replace(r"[][]|^/", "", regex=True)
    return fields_table
