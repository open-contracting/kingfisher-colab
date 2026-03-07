"""Display data and change UI."""

import json

import matplotlib.ticker
import seaborn as sns
from babel.numbers import format_decimal
from IPython.display import HTML


def set_dark_mode():
    """Set the Seaborn theme to match Google Colab's dark mode."""
    sns.set_style(
        "dark",
        {
            "figure.facecolor": "#383838",
            "axes.edgecolor": "#d5d5d5",
            "axes.facecolor": "#383838",
            "axes.labelcolor": "#d5d5d5",
            "text.color": "#d5d5d5",
            "xtick.color": "#d5d5d5",
            "ytick.color": "#d5d5d5",
        },
    )


def set_light_mode():
    """Set the Seaborn theme to light mode, for exporting plots."""
    sns.set_theme()


def format_thousands(axis, locale="en_US"):
    """Set the thousands separator on the given axis for the given locale, e.g. ``en_US``."""
    axis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, _: format_decimal(x, format="#", locale=locale))
    )


def render_json(json_string):
    """
    Render JSON into collapsible HTML.

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
