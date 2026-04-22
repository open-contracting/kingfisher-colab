from collections import Counter

import numpy as np
import pandas as pd
from ipywidgets import widgets

from ocdskingfishercolab import authenticate_gspread, calculate_coverage


def get_coverage(indicators_dic):
    coverage = []
    for i in indicators_dic.values():
        fields = [item for sublist in i for item in sublist][1:]
        result = calculate_coverage(fields, "release_summary")
        result_value = pd.to_numeric(result["total_percentage"][0])
        coverage.append(result_value)
    return coverage


def most_common_fields_to_calculate_indicators(indicators_dict, fields_table):
    fields = list(indicators_dict.values())
    fields = [item[1:] for item in fields]
    flat_list = [item for sublist in [item for sublist in fields for item in sublist] for item in sublist]
    fields_list = Counter(flat_list)

    fields_count = (
        pd.DataFrame.from_dict(fields_list, orient="index")
        .reset_index()
        .rename(columns={"index": "field", 0: "number of indicators"})
    )

    fields_count = fields_count.sort_values("number of indicators", ascending=False).reset_index(drop=True)
    fields_count["published"] = np.where(fields_count["field"].isin(fields_table["path"]), "yes", "no")

    return fields_count


def _indicator_checks(fields_list, indicators_dic, name_col, id_col, check_coverage=False):
    results_list = []
    missing_fields = []

    for i in indicators_dic.values():
        check = any(item not in fields_list for item in i[1])
        result = "missing fields" if check else "possible to calculate"
        missing = [i[1][j] for j in range(len(i[1])) if i[1][j] not in fields_list]
        missing_fields.append(missing)
        results_list.append(result)

    indicatordf = pd.DataFrame(
        list(
            zip(
                list(indicators_dic),
                [indicators_dic[i][0] for i in indicators_dic],
                [indicators_dic[i][1:] for i in indicators_dic],
                strict=True,
            )
        ),
        columns=[name_col, id_col, "fields needed"],
    )
    indicatordf[id_col] = indicatordf[id_col].apply(lambda x: ", ".join(map(str, x)))
    indicatordf["fields needed"] = indicatordf["fields needed"].astype(str).str.replace(r"\[|\]|'", "", regex=True)
    indicatordf["calculation"] = results_list
    indicatordf["missing fields"] = missing_fields
    indicatordf["missing fields"] = indicatordf["missing fields"].apply(lambda x: ", ".join(map(str, x)))

    if check_coverage:
        indicatordf["coverage"] = get_coverage(indicators_dic)

    return indicatordf


def check_red_flags_indicators(result):
    gc = authenticate_gspread()

    # NEW Red Flags to OCDS mapping #Public
    worksheet = gc.open_by_key("1GACSPd64X5Tm-nu6LKttyEpaEp1CLsaCUGrEutljnFU").get_worksheet(1)

    # get_all_values gives a list of rows.
    rows = worksheet.get_all_values()
    # Convert to a DataFrame and render.

    indicators = pd.DataFrame(rows)
    indicators = indicators.rename(columns=indicators.iloc[0]).drop(indicators.index[0])
    indicatorsdf = indicators.iloc[:, [0, 5, 6, 7]]

    return result.merge(indicatorsdf, on="R_id")


def get_red_flags_dictionary(fields_list):
    """To calculate some indicators there are alternative fields that can be used, for example to calculate the
    number of tenderers  both the `tender/numberOfTenderers` or the `tender/tenderers/id` could be used.  This
    section checks which fields are available in the publication. """
    # buyers
    buyer = ["buyer/name", "buyer/id"]
    procuring = ["tender/procuringEntity/name", "tender/procuringEntity/id"]
    parties = ["parties/name", "parties/id", "parties/roles"]
    if not any(item not in fields_list for item in buyer):
        buyer_var = buyer
    elif not any(item not in fields_list for item in procuring):
        buyer_var = procuring
    elif not any(item not in fields_list for item in parties):
        buyer_var = parties
    else:
        buyer_var = buyer

    # bidders
    if "tender/tenderers/id" in fields_list:
        bidders_val = "tender/tenderers/id"
    elif "bids/details/tenderers/id" in fields_list:
        bidders_val = "bids/details/tenderers/id"
    else:
        bidders_val = "tender/tenderers/id"

    # number of tendereres
    if "tender/numberOfTenderers" in fields_list:
        bidders_val2 = "tender/numberOfTenderers"
    elif "tender/tenderers/id" in fields_list:
        bidders_val2 = "tender/tenderers/id"
    elif "bids/details/tenderers/id" in fields_list:
        bidders_val2 = "bids/details/tenderers/id"
    elif "bids/statistics/value" in fields_list:
        bidders_val2 = "bids/statistics/value"
    else:
        bidders_val2 = "tender/numberOfTenderers"

    # suppliers
    parties = ["parties/id", "parties/roles"]

    bidders_val = "bids/details/tenderers/id" if "bids/details/tenderers/id" in fields_list else "tender/tenderers/id"

    # awards and contracts fields
    aw = ["awards/status", "awards/date", "awards/value/amount", "awards/value/currency"]
    con = ["contracts/status", "contracts/dateSigned", "contracts/value/amount", "contracts/value/currency"]
    if not any(item not in fields_list for item in aw):
        awards_val3 = aw
    elif not any(item not in fields_list for item in con):
        awards_val3 = con
    else:
        awards_val3 = aw

    # items
    tender_it = ["tender/items/classification/id", "tender/items/classification/scheme"]
    awards_it = ["awards/items/classification/id", "awards/items/classification/scheme"]
    contracts_it = ["contracts/items/classification/id", "contracts/items/classification/scheme"]
    if not any(item not in fields_list for item in tender_it):
        item_var = tender_it
    elif not any(item not in fields_list for item in awards_it):
        item_var = awards_it
    elif not any(item not in fields_list for item in contracts_it):
        item_var = contracts_it
    else:
        item_var = tender_it

    # unit items
    unit_tender_it = ["tender/items/unit/value/amount", "tender/items/unit/value/currency"]
    unit_awards_it = ["awards/items/unit/value/amount", "awards/items/unit/value/amount"]
    unit_contracts_it = ["contracts/items/unit/value/amount", "contracts/items/unit/value/amount"]
    if not any(item not in fields_list for item in unit_tender_it):
        unit_item_var = unit_tender_it
    elif not any(item not in fields_list for item in unit_awards_it):
        unit_item_var = unit_awards_it
    elif not any(item not in fields_list for item in unit_contracts_it):
        unit_item_var = unit_contracts_it
    else:
        unit_item_var = unit_tender_it

    # date fields
    if "tender/tenderPeriod/startDate" in fields_list:
        date_var = "tender/tenderPeriod/startDate"
    elif "awards/date" in fields_list:
        date_var = "awards/date"
    else:
        date_var = "tender/tenderPeriod/startDate"

    # amounts fields
    if "tender/value/amount" in fields_list:
        amount_var = "tender/value/amount"
    elif "bids/details/value/amount" in fields_list:
        amount_var = "bids/details/value/amount"
    elif "awards/value/amount" in fields_list:
        amount_var = "awards/value/amount"
    elif "contracts/value/amount" in fields_list:
        amount_var = "contracts/value/amount"
    else:
        amount_var = "bids/details/value/amount"

    # winning bids
    win_bid = ["bids/awards/relatedBid"]
    win_bid2 = ["bids/details/tenderers/id", "awards/suppliers/id"]
    if not any(item not in fields_list for item in win_bid):
        win_bid_var = win_bid
    elif not any(item not in fields_list for item in win_bid2):
        win_bid_var = win_bid2
    else:
        win_bid_var = win_bid

    # bidder info
    if "parties/contactPoint/telephone" in fields_list:
        bidders_info_var = "parties/contactPoint/telephone"
    elif "parties/address/streetAddress" in fields_list:
        bidders_info_var = "parties/address/streetAddress"
    elif "parties/address/postalCode" in fields_list:
        bidders_info_var = "parties/address/postalCode"
    else:
        bidders_info_var = "parties/contactPoint/telephone"

    # contact info
    if "parties/contactPoint/telephone" in fields_list:
        contact_info_var = "parties/contactPoint/telephone"
    elif "parties/contactPoint/email" in fields_list:
        contact_info_var = "parties/contactPoint/email"
    elif "parties/contactPoint/name" in fields_list:
        contact_info_var = "parties/contactPoint/name"
    else:
        contact_info_var = "parties/contactPoint/telephone"

    # implementation values
    imp_val = ["contracts/implementation/finalValue/amount", "contracts/implementation/finalValue/currency"]
    imp_val2 = [
        "contracts/implementation/transactions/value/amount",
        "contracts/implementation/transactions/value/currency",
    ]
    if not any(item not in fields_list for item in imp_val):
        imp_val_var = imp_val
    elif not any(item not in fields_list for item in imp_val2):
        imp_val_var = imp_val2
    else:
        imp_val_var = imp_val

    return {
        "Planning documents not available": [["R001"], ["planning/documents/documentType"]],
        "Manipulation of procurement thresholds": [
            ["R002"],
            [
                "tender/value/amount",
                "tender/value/currency",
                "tender/procurementMethod",
                "tender/tenderPeriod/startDate",
                *buyer_var,
            ],
        ],
        " The submission period is too short": [
            ["R003"],
            ["tender/tenderPeriod/startDate", "tender/tenderPeriod/endDate", "tender/procurementMethod"],
        ],
        "Failure to adequately advertise the request for bids": [
            ["R004"],
            ["tender/documents/documentType", "tender/documents/datePublished", "tender/tenderPeriod/startDate"],
        ],
        "Key tender information and documents are not available": [
            ["R005"],
            [
                "tender/documents/documentType",
                "tender/documents/datePublished",
                "tender/tenderPeriod/startDate",
                "tender/tenderPeriod/endDate",
            ],
        ],
        "Unreasonable prequalification requirements": [["R006"], ["tender/eligibilityCriteria"]],
        "Unreasonable technical specifications": [
            ["R007"],
            [
                "tender/documents/documentType",
                "tender/procurementMethod",
                "tender/items/classification/id",
                "tender/items/classification/scheme",
                *buyer_var,
                "tender/value/amount",
            ],
        ],
        "Unreasonable participation fees": [
            ["R008"],
            [
                "tender/participationFees/value/amount",
                "tender/participationFees/value/currency",
                "tender/value/amount",
            ],
        ],
        "Buyer increases the cost of the bidding documents": [
            ["R009"],
            ["tender/participationFees/value/amount", "tender/participationFees/value/currency", "date"],
        ],
        "Unjustified use of non competitive procedure": [
            ["R010"],
            ["tender/procurementMethod", "tender/procurementMethodDetails", "tender/procurementMethodRationale"],
        ],
        "Splitting purchases to avoid procurement thresholds": [
            ["R011"],
            [
                "tender/procurementMethod",
                *item_var,
                "tender/value/amount",
                "tender/value/currency",
                "tender/tenderPeriod/startDate",
                *buyer_var,
            ],
        ],
        "Direct awards in contravention of the provisions of the procurement plan": [
            ["R012"],
            ["tender/procurementMethod", "tender/procurementMethodDetails", "planning/documents/documentType"],
        ],
        "High use of non competitive methods": [["R013"], ["tender/procurementMethod", *buyer_var]],
        "Short time between tender advertising and bid opening": [
            ["R014"],
            ["tender/tenderPeriod/startDate", "tender/bidOpening/date", "tender/procurementMethod"],
        ],
        "Long time between bid opening and bid evaluation": [
            ["R015"],
            ["tender/bidOpening/date", "tender/awardPeriod/startDate", "tender/procurementMethod"],
        ],
        "Tender value is higher or lower than average for this item category": [
            ["R016"],
            ["tender/value/amount", "tender/value/currency", *item_var, "tender/procurementMethod"],
        ],
        "Unreasonably low or high line item": [["R017"], [*item_var, *unit_item_var]],
        "Single bid received": [["R018"], ["tender/procurementMethod", bidders_val2]],
        "Low number of bidders for item and procuring entity": [
            ["R019"],
            ["tender/procurementMethod", *item_var, *buyer_var, bidders_val2],
        ],
        "Tender has a complaint": [["R020"], ["complaints/id"]],
        "High use of discretionary evaluation criteria": [["R021"], ["tender/awardCriteria", *buyer_var]],
        "Wide disparity in bid prices": [
            ["R022"],
            ["bids/details/id", "bids/details/value/amount", "bids/details/value/currency", "bids/details/status"],
        ],
        "Fixed multiple bid prices": [
            ["R023"],
            ["bids/details/id", "bids/details/value/amount", "bids/details/value/currency", "bids/details/status"],
        ],
        "Price close to winning bid": [
            ["R024"],
            [
                "bids/details/id",
                "bids/details/value/amount",
                "bids/details/value/currency",
                "bids/details/status",
                *win_bid_var,
            ],
        ],
        "Excessive unsuccessful bids": [["R025"], ["awards/suppliers/id", "bids/details/status", bidders_val]],
        "Prevalence of consortia": [
            ["R026"],
            ["awards/suppliers/id", "awards/suppliers/name", "awards/status", "awards/date", *item_var],
        ],
        "Missing bidders": [["R027"], ["tender/procurementMethod", *item_var, bidders_val, date_var]],
        "Identical bid prices": [
            ["R028"],
            [
                "bids/details/id",
                "bids/details/value/amount",
                "bids/details/value/currency",
                "bids/details/tenderers/id",
            ],
        ],
        "Bid prices deviate from Benford's Law": [["R029"], [amount_var, *item_var]],
        "Late bid won": [
            ["R030"],
            [
                "bids/details/id",
                "bids/details/date",
                "bids/details/status",
                "tender/tenderPeriod/endDate",
                *win_bid_var,
            ],
        ],
        "Winning bid price very close or higher than estimated price": [
            ["R031"],
            [
                "bids/details/id",
                "bids/details/value/amount",
                "bids/details/value/currency",
                "bids/details/status",
                "tender/value/amount",
                "tender/value/currency",
                *win_bid_var,
            ],
        ],
        "Bidders share same beneficial owner": [
            ["R032"],
            ["parties/roles", "parties/id", "parties/beneficialOwners/name", "parties/beneficialOwners/id"],
        ],
        "Bidders share same major shareholder": [
            ["R033"],
            [
                "parties/roles",
                "parties/id",
                "parties/shareholders/shareholder/id",
                "parties/shareholders/shareholding",
            ],
        ],
        " Bids submitted in same order": [
            ["R034"],
            [
                "bids/details/id",
                "bids/details/date",
                "bids/details/tenderers/id",
                "bids/details/tenderers/name",
                "bids/details/status",
            ],
        ],
        "All except winning bid disqualified": [
            ["R035"],
            ["bids/details/id", "bids/details/status", "awards/status", *win_bid_var],
        ],
        "Lowest bid disqualified ": [
            ["R036"],
            [
                "tender/awardCriteria",
                "bids/details/id",
                "bids/details/value/amount",
                "bids/details/value/currency",
                "bids/details/status",
            ],
        ],
        "Poorly supported disqualifications": [
            ["R037"],
            [
                "tender/awardCriteria",
                "bids/details/id",
                "bids/details/value/amount",
                "bids/details/value/currency",
                "bids/details/status",
                "bids/details/documents",
            ],
        ],
        "Excessive disqualified bids": [["R038"], ["bids/details/id", "bids/details/status", bidders_val, *buyer_var]],
        "Unanswered bidder questions": [
            ["R039"],
            ["tender/enquiries/date", "tender/enquiries/dateAnswered", "tender/enquiries/answer", "tender/status"],
        ],
        "High share of buyers contracts": [
            ["R040"],
            [*buyer_var, "awards/status", "awards/date", "awards/suppliers/id", "awards/suppliers/name"],
        ],
        "Physical similarities in documents by different bidders": [
            ["R041"],
            ["bids/details/id", "bids/details/tenderers/id", "bids/documents/documentType"],
        ],
        "Bidder has abnormal address or phone number": [["R042"], [bidders_info_var]],
        "Bidder has same contact information as project official": [
            ["R043"],
            ["parties/roles", "parties/id", contact_info_var],
        ],
        "Business similarities between bidders": [["R044"], ["parties/roles", "parties/id", bidders_info_var]],
        "Bidder is not listed in  business registries": [["R045"], ["parties/roles", "parties/id"]],
        "Bidder is debarred or on sanctions list": [["R046"], ["parties/roles", "parties/id"]],
        "Supplier is not traceable on the web": [
            ["R047"],
            ["awards/suppliers/name", "awards/suppliers/id", "parties/contactPoint/url"],
        ],
        "Heterogeneous supplier": [["R048"], [*item_var, "awards/suppliers/id", "awards/suppliers/name"]],
        "Direct awards below threshold": [
            ["R049"],
            ["awards/suppliers/id", "awards/suppliers/name", "awards/date", "tender/procurementMethod", *buyer_var],
        ],
        " High market share": [
            ["R050"],
            [
                "awards/suppliers/id",
                "awards/suppliers/name",
                *buyer_var,
                "awards/value/amount",
                "awards/value/currency",
                *item_var,
                "awards/date",
                "awards/status",
            ],
        ],
        "High market concentration": [
            ["R051"],
            ["awards/suppliers/id", "awards/suppliers/name", *awards_val3],
        ],
        "Small initial purchase from supplier followed by much larger purchases": [
            ["R052"],
            ["awards/suppliers/id", "awards/suppliers/name", "tender/procurementMethod", *buyer_var, *awards_val3],
        ],
        "Co-bidding pairs have same recurrent winner": [
            ["R053"],
            ["bids/details/id", "bids/details/status", *win_bid_var],
        ],
        "Direct award followed by change orders  that exceed the competitive threshold": [
            ["R054"],
            [
                "tender/procurementMethod",
                "awards/value/amount",
                "awards/value/currency",
                "contracts/value/amount",
                "contracts/value/currency",
                "contracts/amendments/description",
            ],
        ],
        "Multiple direct awards above or just below competitive threshold": [
            ["R055"],
            ["tender/procurementMethod", "awards/suppliers/id", "awards/suppliers/name", *awards_val3, *buyer_var],
        ],
        "Winning bid does not meet the award criteria": [
            ["R056"],
            ["tender/awardCriteria", "bids/details/status", "bids/details/documents", *win_bid_var],
        ],
        " Bid rotation": [
            ["R057"],
            [
                "bids/details/tenderers/id",
                "bids/details/tenderers/name",
                "awards/suppliers/id",
                "awards/suppliers/name",
                "bids/details/value/amount",
                "bids/details/value/currency",
                *item_var,
            ],
        ],
        "Heavily discounted bid": [
            ["R058"],
            [
                "bids/details/id",
                "bids/details/value/amount",
                "bids/details/value/currency",
                "bids/details/status",
                *win_bid_var,
            ],
        ],
        "Large difference between the award value and final contract amount": [
            ["R059"],
            [
                "awards/id",
                "awards/status",
                "awards/value/amount",
                "awards/value/currency",
                "contracts/awardID",
                "contracts/value/amount",
                "contracts/value/currency",
                "contracts/status",
            ],
        ],
        "Long time between award date and contract signature date": [
            ["R060"],
            ["awards/date", "contracts/dateSigned", "tender/procurementMethod"],
        ],
        "Decision period extremely short": [
            ["R061"],
            ["tender/tenderPeriod/endDate", "awards/date", "tender/procurementMethod"],
        ],
        "Decision period extremely long": [
            ["R062"],
            ["tender/tenderPeriod/endDate", "awards/date", "tender/procurementMethod"],
        ],
        "Contract is not published": [["R063"], ["contracts/documents/documentType"]],
        "Contract has modifications": [["R064"], ["contracts/status", "contracts/amendments/description"]],
        "Contract amendments to reduce line items": [
            ["R065"],
            ["contracts/status", "contracts/amendments/description", "contracts/amendments/rationale"],
        ],
        "Contract amendments to increase line items": [
            ["R066"],
            ["contracts/status", "contracts/amendments/description", "contracts/amendments/rationale"],
        ],
        "Delivery failure": [
            ["R067"],
            [
                "contracts/implementation/milestones/type",
                "contracts/implementation/milestones/dueDate",
                "contracts/implementation/milestones/dateMet",
            ],
        ],
        "Contract transactions exceed contract amount": [
            ["R068"],
            ["contracts/value/amount", "contracts/value/currency", *imp_val_var],
        ],
        "Contract amendments to increase price": [
            ["R069"],
            ["contracts/status", "contracts/amendments/description", "contracts/amendments/rationale"],
        ],
        "Losing bidders are hired as subcontractors": [
            ["R070"],
            [
                "contracts/relatedProcesses",
                "contracts/relatedProcesses/relationship",
                "awards/suppliers/id",
                bidders_val,
            ],
        ],
        "A contractor subcontracts all or most of the work received": [
            ["R071"],
            ["awards/hasSubcontracting", "awards/subcontracting/minimumPercentage"],
        ],
        "High prevalence of subcontracts": [["R072"], ["awards/hasSubcontracting", *buyer_var]],
        "Discrepancies between work completed and contract specifications": [
            ["R073"],
            [
                "contracts/status",
                "contracts/documents/documentType",
                "contracts/implementation/documents/documentType",
            ],
        ],
    }


def redflags_checks(fields_list, indicators_dic, *, check_coverage=False):
    """
    Return a table of the red flags checks.

    It indicates if the fields needed to calculate a particular indicator are present.
    Set check_coverage=True to check for coverage.
    """
    return _indicator_checks(fields_list, indicators_dic, "red_flag", "R_id", check_coverage=check_coverage)


def get_indicators_dictionary(fields_list):
    """
    Check which alternative fields are available for indicators.

    For example, the number of tenderers can use either `tender/numberOfTenderers` or `tender/tenderers/id`.
    """
    # U002
    buyer = ["buyer/name", "buyer/id"]
    procuring = ["tender/procuringEntity/name", "tender/procuringEntity/id"]
    parties = ["parties/identifier/name", "parties/identifier/id", "parties/roles"]
    if not any(item not in fields_list for item in buyer):
        buyer_var = buyer
    elif not any(item not in fields_list for item in procuring):
        buyer_var = procuring
    elif not any(item not in fields_list for item in parties):
        buyer_var = parties
    else:
        buyer_var = buyer

    # U003
    if "tender/tenderers/id" in fields_list:
        bidders_val = "tender/tenderers/id"
    elif "bids/details/tenderers/id" in fields_list:
        bidders_val = "bids/details/tenderers/id"
    else:
        bidders_val = "tender/tenderers/id"

    # U008
    if "tender/items/classification/id" in fields_list and "tender/items/classification/scheme" in fields_list:
        items_val = ["tender/items/classification/id", "tender/items/classification/scheme"]
    elif "awards/items/classification/id" in fields_list and "awards/items/classification/scheme" in fields_list:
        items_val = ["awards/items/classification/id", "awards/items/classification/scheme"]
    elif "contracts/items/classification/id" in fields_list and "contractsitems/classification/scheme" in fields_list:
        items_val = ["contracts/items/classification/id", "contracts/items/classification/scheme"]
    else:
        items_val = ["tender/items/classification/id", "tender/items/classification/scheme"]

    # U012
    if "contracts/id" in fields_list and "contracts/status" in fields_list:
        awards_val = ["contracts/id", "contracts/status"]
    elif "awards/id" in fields_list and "awards/status" in fields_list:
        awards_val = ["awards/id", "awards/status"]
    else:
        awards_val = ["contracts/id", "contracts/status"]

    # U013, UC14
    awards = ["awards/id", "awards/status", "awards/value/amount", "awards/value/currency"]
    contracts = ["contracts/id", "contracts/status", "contracts/value/amount", "contracts/value/currency"]
    if not any(item not in fields_list for item in contracts):
        awards_val2 = contracts
    elif not any(item not in fields_list for item in awards):
        awards_val2 = awards
    else:
        awards_val2 = contracts

    # U015
    if "tender/numberOfTenderers" in fields_list:
        bidders_val2 = "tender/numberOfTenderers"
    elif "tender/tenderers/id" in fields_list:
        bidders_val2 = "tender/tenderers/id"
    elif "bids/details/tenderers/id" in fields_list:
        bidders_val2 = "bids/details/tenderers/id"
    else:
        bidders_val2 = "tender/numberOfTenderers"

    # U034
    aw = [
        "awards/status",
        "awards/value/amount",
        "awards/value/currency",
        "awards/items/classification/id",
        "awards/items/classification/scheme",
    ]
    con = [
        "contracts/status",
        "contracts/value/amount",
        "contracts/value/currency",
        "contracts/items/classification/id",
        "contracts/items/classification/scheme",
    ]
    if not any(item not in fields_list for item in aw):
        awards_val3 = aw
    elif not any(item not in fields_list for item in con):
        awards_val3 = con
    else:
        awards_val3 = aw

    # U042
    if "awards/status" in fields_list:
        awards_val4 = "awards/status"
    elif "contracts/status" in fields_list:
        awards_val4 = "contracts/status"
    else:
        awards_val4 = "awards/status"

    # U061
    if "contracts/period/startDate" in fields_list:
        contract_date = "contracts/period/startDate"
    elif "awards/contractPeriod/startDate" in fields_list:
        contract_date = "awards/contractPeriod/startDate"
    else:
        contract_date = "contracts/period/startDate"

    # U065
    aw2 = ["awards/items/quantity", "awards/items/unit"]
    con2 = ["contracts/items/quantity", "contracts/items/unit"]
    if not any(item not in fields_list for item in aw):
        awards_val5 = aw2
    elif not any(item not in fields_list for item in con):
        awards_val5 = con2
    else:
        awards_val5 = aw2

    # U066, U067
    if "planning/budget/amount/amount" in fields_list and "planning/budget/amount/currency":
        planning = ["planning/budget/amount/amount", "planning/budget/amount/currency"]
    elif "tender/value/amount" in fields_list and "tender/value/currency":
        planning = ["tender/value/amount", "tender/value/currency"]
    else:
        planning = ["planning/budget/amount/amount", "planning/budget/amount/currency"]

    return {
        "Total number of procedures": [["U001"], ["ocid"]],
        "Total number of procuring entities": [["U002"], ["ocid", *buyer_var]],
        "Total number of unique bidders": [["U003"], ["ocid", bidders_val]],
        "Total number of awarded suppliers": [
            ["U004"],
            ["awards/id", "awards/suppliers/id", "awards/suppliers/name", "awards/status"],
        ],
        "Total number of procedures by year or month": [["U005"], ["ocid", "date"]],
        "Total value awarded": [["U006"], ["ocid", "awards/status", "awards/value/amount", "awards/value/currency"]],
        "Share of procedures by status": [["U007"], ["ocid", "tender/status"]],
        "Number of procedures by item type": [["U008"], ["ocid", *items_val]],
        "Proportion of procedures by procurement category": [["U009"], ["ocid", "tender/mainProcurementCategory"]],
        "Percent of tenders by procedure type": [["U010"], ["ocid", "tender/procurementMethod"]],
        "Percent of tenders awarded by means of competitive procedures": [
            ["U011"],
            ["ocid", "tender/procurementMethod", "awards/status"],
        ],
        "Percent of contracts awarded under each procedure type": [
            ["U012"],
            ["ocid", "tender/procurementMethod", *awards_val],
        ],
        "Total contracted value awarded under each procedure type": [
            ["U013"],
            ["ocid", "tender/procurementMethod", *awards_val2],
        ],
        "Total awarded value of tenders awarded by means of competitive procedures": [
            ["U014"],
            ["ocid", "tender/procurementMethod", *awards_val2],
        ],
        "Proportion of single bid tenders": [["U015"], ["ocid", "tender/procurementMethod", bidders_val2]],
        "Proportion of value awarded in single bid tenders vs competitive tenders": [
            ["U016"],
            [
                "ocid",
                "tender/procurementMethod",
                "awards/status",
                "awards/value/amount",
                "awards/value/currency",
                bidders_val2,
            ],
        ],
        "Mean number of bidders per tender": [["U017"], ["ocid", "tender/procurementMethod", bidders_val2]],
        "Median number of bidders per tender": [["U018"], ["ocid", "tender/procurementMethod", bidders_val2]],
        "Mean number of bidders by item type": [
            ["U019"],
            ["ocid", "tender/procurementMethod", bidders_val2, *items_val],
        ],
        "Number of suppliers by item type": [
            ["U020"],
            ["awards/id", "awards/suppliers/id", "awards/suppliers/name", *items_val],
        ],
        "Number of new bidders in a system ": [
            ["U021"],
            ["tender/id", "tender/tenderers/id", "tender/tenderPeriod/startDate"],
        ],
        "Percent of new bidders to all bidders ": [
            ["U022"],
            ["tender/id", "tender/tenderers/id", "tender/tenderPeriod/startDate"],
        ],
        "Percent of tenders with at least three participants deemed qualified": [
            ["U023"],
            ["ocid", "bids/details/tenderers/id", "bids/details/id", "bids/details/status"],
        ],
        "Mean percent of bids which are disqualified": [
            ["U024"],
            ["tender/id", "bids/details/id", "bids/details/status"],
        ],
        "Percent of contracts awarded to top 10 suppliers with largest contracted totals": [
            ["U025"],
            ["awards/id", "awards/suppliers/id", "awards/suppliers/name", *awards_val2],
        ],
        "Mean number of unique suppliers per buyer": [
            ["U026"],
            ["ocid", "awards/suppliers/id", "awards/suppliers/name", *buyer_var],
        ],
        "Number of new awarded suppliers ": [
            ["U027"],
            ["awards/id", "awards/suppliers/id", "awards/suppliers/name", "awards/date"],
        ],
        "Percent of awards awarded to new suppliers": [
            ["U028"],
            ["awards/id", "awards/suppliers/id", "awards/suppliers/name", "awards/date"],
        ],
        "Total awarded value awarded to new suppliers": [
            ["U029"],
            [
                "awards/id",
                "awards/suppliers/id",
                "awards/suppliers/name",
                "awards/date",
                "awards/value/amount",
                "awards/value/currency",
            ],
        ],
        "Percent of new suppliers to all suppliers": [
            ["U030"],
            ["awards/id", "awards/suppliers/id", "awards/suppliers/name", "awards/date"],
        ],
        "Percent of growth of new awarded suppliers in a system": [
            ["U031"],
            ["awards/id", "awards/suppliers/id", "awards/suppliers/name", "awards/date"],
        ],
        "Percent of total awarded value awarded to recurring suppliers": [
            ["U032"],
            [
                "awards/id",
                "awards/suppliers/id",
                "awards/suppliers/name",
                "awards/date",
                "awards/value/amount",
                "awards/value/currency",
            ],
        ],
        "Mean number of bids necessary to win": [
            ["U033"],
            ["ocid", "tender/tenderers/id", "awards/suppliers/id", "awards/suppliers/name"],
        ],
        "Market concentration, market share of the largest company in the market": [
            ["U034"],
            ["awards/suppliers/id", "awards/suppliers/name", *awards_val3],
        ],
        "Proportion of contracts awarded by supplier by non competitive procedures": [
            ["U035"],
            ["ocid", "tender/procurementMethod", "awards/status", "awards/suppliers/id", "awards/suppliers/name"],
        ],
        "Region of the supplier": [["U036"], ["parties/roles", "parties/identifier/id", "parties/address/region"]],
        "Number of bids submitted by supplier": [["U037"], ["awards/suppliers/id", bidders_val]],
        "Success rate of bidders": [
            ["U038"],
            ["ocid", "tender/tenderers/id", "awards/suppliers/id", "awards/suppliers/name"],
        ],
        "Number of unique items classifications awarded by supplier": [
            ["U039"],
            ["awards/id", "awards/suppliers/id", "awards/suppliers/name", *items_val],
        ],
        "Total value awarded by supplier": [["U040"], ["awards/suppliers/id", "awards/suppliers/name", *awards_val2]],
        "Share of total value awarded by supplier": [
            ["U041"],
            ["awards/suppliers/id", "awards/suppliers/name", *awards_val2],
        ],
        "Total number of contracts awarded by supplier": [
            ["U042"],
            ["awards/id", "awards/suppliers/id", "awards/suppliers/name", awards_val4],
        ],
        "Number of procuring entities by supplier": [
            ["U043"],
            ["ocid", "awards/suppliers/id", "awards/suppliers/name", *buyer_var],
        ],
        "Share of single bid awards by supplier": [
            ["U044"],
            [
                "ocid",
                "awards/suppliers/id",
                "awards/suppliers/name",
                "awards/status",
                "tender/procurementMethod",
                bidders_val2,
            ],
        ],
        "Percent of tenders with linked procurement plans": [["U045"], ["tender/id", "tender/documents/documentType"]],
        "Percent of contracts which publish information on debarments": [
            ["U046"],
            ["contracts/id", "contracts/implementation/documents/documentType"],
        ],
        "The percent of tenders for which the tender documentation was added after publication of the announcement ": [
            ["U047"],
            [
                "tender/id",
                "tender/documents/documentType",
                "tender/documents/documentType",
                "tender/documents/datePublished",
            ],
        ],
        "Mean number of contract amendments per buyer": [
            ["U048"],
            ["ocid", "contracts/id", "contracts/amendments", *buyer_var],
        ],
        "Percent of tenders which have been closed for more than 30 days, but whose basic awards information is not published": [
            ["U049"],
            [
                "tender/id",
                "tender/tenderPeriod/endDate",
                "awards/id",
                "awards/date",
                "awards/status",
                "awards/value/amount",
                "awards/suppliers/id",
                "awards/suppliers/name",
            ],
        ],
        "Percent of awards which are older than 30 days, but whose contract is not published": [
            ["U050"],
            [
                "awards/id",
                "awards/date",
                "contracts/awardID",
                "contracts/status",
                "contracts/dateSigned",
                "contracts/documents/documentType",
            ],
        ],
        "Percent of tenders that do not specify place of delivery": [
            ["U051"],
            ["ocid", "tender/items/deliveryLocation", "tender/items/deliveryAddress"],
        ],
        "Percent of tenders that do not specify date of delivery": [
            ["U052"],
            [
                "tender/milestones/id",
                "tender/milestones/type",
                "tender/milestones/description",
                "tender/milestones/dueDate",
            ],
        ],
        "Percent of tenders with short titles for example fewer than 10 characters in the title": [
            ["U053"],
            ["tender/id", "tender/title"],
        ],
        "Percent of tenders with short descriptions for instance fewer than 30 characters in the description": [
            ["U054"],
            ["tender/id", "tender/description"],
        ],
        "Percent of tenders that do not include detailed item codes or item descriptions": [
            ["U055"],
            ["tender/id", "tender/items/classification/id", "tender/items/classification/scheme"],
        ],
        "Percent of contracts that do not have amendments": [["U056"], ["contracts/id", "contracts/amendments"]],
        "Percent of contracts which publish contract implementation details financial": [
            ["U057"],
            [
                "contracts/implementation/transactions/id",
                "contracts/implementation/transactions/value/amount",
                "contracts/implementation/transactions/value/currency",
            ],
        ],
        "Percent of contracts which publish contract implementation details physical": [
            ["U058"],
            [
                "contracts/implementation/milestones/type",
                "contracts/implementation/milestones/id",
                "contracts/implementation/milestones/dueDate",
                "contracts/implementation/milestones/status",
            ],
        ],
        "Average duration of tendering period days": [
            ["U059"],
            ["ocid", "tender/tenderPeriod/startDate", "tender/tenderPeriod/endDate"],
        ],
        "Average duration of decision period days": [["U060"], ["ocid", "tender/tenderPeriod/endDate", "awards/date"]],
        "Average days from award date to start of implementation": [
            ["U061"],
            ["awards/id", "awards/date", contract_date],
        ],
        "Days between award date and tender start date": [
            ["U062"],
            ["ocid", "tender/tenderPeriod/startDate", "awards/date"],
        ],
        "Percent of canceled tenders to awarded tenders": [["U063"], ["ocid", "tender/status", "awards/status"]],
        "Percent of contracts which are canceled": [["U064"], ["contracts/id", "contracts/status"]],
        "Price variation of same item across all awards": [["U065"], awards_val3 + awards_val5],
        "Percent of contracts that exceed budget": [
            ["U066"],
            ["ocid", "contracts/status", "contracts/value/amount", "contracts/value/currency", *planning],
        ],
        "Mean percent overrun of contracts that exceed budget": [
            ["U067"],
            ["ocid", "contracts/status", "contracts/value/amount", "contracts/value/currency", *planning],
        ],
        "Total percent savings difference between budget and contract value": [
            ["U068"],
            [
                "ocid",
                "planning/budget/amount/amount",
                "planning/budget/amount/currency",
                "contracts/value/amount",
                "contracts/value/currency",
            ],
        ],
        "Total percent savings difference between tender value estimate and contract value": [
            ["U069"],
            [
                "ocid",
                "tender/value/amount",
                "tender/value/currency",
                "contracts/value/amount",
                "contracts/value/currency",
            ],
        ],
        "Percent of contracts completed on time ": [
            ["U070"],
            ["contracts/id", "contracts/period/endDate", "contracts/status"],
        ],
        "Share of contracts whose milestones are completed on time": [
            ["U071"],
            [
                "contracts/id",
                "contracts/implementation/milestones/dueDate",
                "contracts/implementation/milestones/dateMet",
            ],
        ],
    }


def usability_checks(fields_list, indicators_dic):
    """
    Return a table of the usability checks.

    It indicates if the fields needed to calculate a particular indicator are present.
    """
    return _indicator_checks(fields_list, indicators_dic, "indicator", "U_id")


def check_usability_indicators(lang, result):
    gc = authenticate_gspread()

    if lang.value == "English":
        # Use case guide: Indicators linked to OCDS #public
        spreadsheet_key = "1j-Y0ktZiOyhZzi-2GSabBCnzx6fF5lv8h1KYwi_Q9GM"
    else:
        # [ES] of Use case guide: Indicators linked to OCDS #public
        spreadsheet_key = "1l_p_e1iNUUuR5AObTJ8EY9VrcCLTAq3dnG_Fj73UH9w"

    worksheet = gc.open_by_key(spreadsheet_key).sheet1

    # get_all_values gives a list of rows.
    rows = worksheet.get_all_values()
    # Convert to a DataFrame and render.

    indicators = pd.DataFrame(rows)
    indicators = indicators.rename(columns=indicators.iloc[0]).drop(indicators.index[0])

    if lang.value == "English":
        indicatorsdf = indicators.iloc[:, [0, 3, 4, 9]]
        result_final = result.merge(indicatorsdf, on="U_id")
    else:
        indicatorsdf = indicators.iloc[:, [0, 3, 4, 5, 9]]
        result_final = indicatorsdf.merge(result, on="U_id").drop(["indicator"], axis=1)
        result_final = result_final.rename(
            columns={
                "fields needed": "Campos necesarios",
                "calculation": "¿Se puede calcular?",
                "missing fields": "Campos faltantes",
                "coverage": "Cobertura",
            },
        )
        result_final = result_final.replace(
            {"¿Se puede calcular?": {"possible to calculate": "sí", "missing fields": "campos faltantes"}}
        )
    return result_final


def is_relevant(field_list):
    relevant_rules = {
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
    final_result = []
    for key, value in relevant_rules.items():
        rule_result = {"rule": key, "possible_to_calculate": "No", "available_fields": [], "missing_fields": []}
        for field in value:
            if isinstance(field, str):
                if field in field_list:
                    rule_result["possible_to_calculate"] = "Yes"
                    rule_result["available_fields"].append(field)
                else:
                    rule_result["missing_fields"].append(field)
            else:
                missing = [item for item in field if item not in field_list]
                rule_result["available_fields"].extend(item for item in field if item in field_list)
                rule_result["missing_fields"].extend(missing)
                if not missing:
                    rule_result["possible_to_calculate"] = "Yes"
        final_result.append(rule_result)

    final_result = pd.DataFrame(final_result)
    relevant = (final_result["possible_to_calculate"] == "Yes").all()
    return relevant, final_result


def get_usability_language_select_box():
    style = {"description_width": "initial"}
    languages = ["Spanish", "English"]
    return widgets.Dropdown(options=languages, description="language", style=style)
