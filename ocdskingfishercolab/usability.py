"""Functions for checking usability and red flag indicators against OCDS data."""

import pandas as pd

from ocdskingfishercolab import authenticate_gspread

RELEVANT_RULES = {
    "who": [
        "buyer/id",
        "buyer/name",
        "tender/procuringEntity/id",
        "tender/procuringEntity/name",
    ],
    "bought what": [
        "tender/items/classification/id",
        "awards/items/classification/id",
        "contracts/items/classification/id",
        "tender/items/classification/description",
        "awards/items/classification/description",
        "contracts/items/classification/description",
        "tender/items/description",
        "awards/items/description",
        "contracts/items/description",
        "tender/description",
        "awards/description",
        "contracts/description",
        "tender/title",
        "awards/title",
        "contracts/title",
    ],
    "from whom": [
        "awards/suppliers/id",
        "awards/suppliers/name",
    ],
    "for how much": [
        "awards/value/amount",
        "contracts/value/amount",
        [
            "awards/items/quantity",
            "awards/items/unit/value/amount",
        ],
        [
            "contracts/items/quantity",
            "contracts/items/unit/value/amount",
        ],
    ],
    "when": [
        "tender/tenderPeriod/endDate",
        "awards/date",
        "contracts/dateSigned",
    ],
    "how": [
        "tender/procurementMethod",
        "tender/procurementMethodDetails",
    ],
}


def check_red_flags_indicators(result):
    """
    Merge result DataFrame with red flag indicator metadata from the NEW Red Flags to OCDS mapping #Public
    spreadsheet.
    """
    # NEW Red Flags to OCDS mapping #Public
    spreadsheet_key = "1GACSPd64X5Tm-nu6LKttyEpaEp1CLsaCUGrEutljnFU"
    rows = authenticate_gspread().open_by_key(spreadsheet_key).get_worksheet(1).get_all_values()
    indicators = pd.DataFrame(rows).pipe(lambda df: df.rename(columns=df.iloc[0]).drop(df.index[0]))
    return result.merge(indicators.iloc[:, [0, 5, 6, 7]], on="R_id")


def check_usability_indicators(lang, result):
    """
    Merge result DataFrame with usability indicator metadata from the Use case guide: Indicators linked to OCDS #public
    spreadsheet.
    """
    # Use case guide: Indicators linked to OCDS #public
    if lang.value == "English":
        spreadsheet_key = "1j-Y0ktZiOyhZzi-2GSabBCnzx6fF5lv8h1KYwi_Q9GM"
    else:  # [ES]
        spreadsheet_key = "1l_p_e1iNUUuR5AObTJ8EY9VrcCLTAq3dnG_Fj73UH9w"

    rows = authenticate_gspread().open_by_key(spreadsheet_key).get_worksheet(0).get_all_values()
    indicators = pd.DataFrame(rows).pipe(lambda df: df.rename(columns=df.iloc[0]).drop(df.index[0]))

    if lang.value == "English":
        return result.merge(indicators.iloc[:, [0, 3, 4, 9]], on="U_id")

    return (
        indicators.iloc[:, [0, 3, 4, 5, 9]]
        .merge(result, on="U_id")
        .drop(columns="indicator")
        .rename(
            columns={
                "fields needed": "Campos necesarios",
                "calculation": "¿Se puede calcular?",
                "missing fields": "Campos faltantes",
                "coverage": "Cobertura",
            }
        )
        .replace({"¿Se puede calcular?": {"possible to calculate": "sí", "missing fields": "campos faltantes"}})
    )


def is_relevant(field_list):
    """
    Check if the dataset has the basic fields to answer: who bought what, from whom, for how much, when, and how.

    Each rule in RELEVANT_RULES is satisfied if ANY of its options is present:
    - String options: the field must be in field_list
    - List options: all fields in the list must be in field_list
    """
    results = []
    for rule_name, options in RELEVANT_RULES.items():
        available = []
        missing = []
        possible = False

        for option in options:
            if isinstance(option, str):
                if option in field_list:
                    available.append(option)
                    possible = True
                else:
                    missing.append(option)
            else:
                all_present = True
                for opt in option:
                    if opt in field_list:
                        available.append(opt)
                    else:
                        missing.append(opt)
                        all_present = False
                if all_present:
                    possible = True

        results.append(
            {
                "rule": rule_name,
                "possible_to_calculate": "Yes" if possible else "No",
                "available_fields": available,
                "missing_fields": missing,
            }
        )

    df = pd.DataFrame(results)
    return (df["possible_to_calculate"] == "Yes").all(), df
