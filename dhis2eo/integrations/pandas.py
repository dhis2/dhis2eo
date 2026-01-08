import pandas as pd

from ..utils.time import DAY, MONTH, YEAR, detect_period_type


def format_value_for_dhis2(value):
    """Format numeric value as decimal string for DHIS2 compatibility.

    DHIS2 API rejects values in scientific notation (e.g., '8.24e-05').
    This converts floats to decimal strings with up to 10 decimal places.
    """
    if pd.isna(value):
        return value
    return f"{value:.10f}".rstrip("0").rstrip(".")


def parse_period(period_value):
    """Convert pandas period or datetime values to DHIS2 period type"""
    # TODO: more robust handling and testing of period formats and types
    period_string = str(period_value).split(" ")[0]  # remove time info after space
    period_type = detect_period_type(period_string)
    if period_type == DAY:
        period_obj = pd.Period(period_value, freq="D")
        return period_obj.strftime("%Y%m%d")
    elif period_type == MONTH:
        period_obj = pd.Period(period_value, freq="M")
        return period_obj.strftime("%Y%m")
    elif period_type == YEAR:
        period_obj = pd.Period(period_value, freq="Y")
        return period_obj.strftime("%Y")
    else:
        raise NotImplementedError(f"Period type {period_type} not yet supported")


def dataframe_to_dhis2_json(df, data_element_id, org_unit_col, period_col, value_col):
    """Translates a pandas.DataFrame to JSON format used by DHIS2 Web API."""
    # subset the df
    df_subset = df[[org_unit_col, period_col, value_col]].copy()

    # remap column names
    remap = {
        org_unit_col: "orgUnit",
        period_col: "period",
        value_col: "value",
    }
    df_subset.rename(columns=remap, inplace=True)

    # parse period column to dhis2 format
    df_subset["period"] = df_subset["period"].apply(parse_period)

    # add dataElement column
    df_subset["dataElement"] = data_element_id

    # remove nan values
    df_subset = df_subset[~pd.isna(df_subset["value"])]

    # format values as decimal strings (DHIS2 rejects scientific notation)
    df_subset["value"] = df_subset["value"].apply(format_value_for_dhis2)

    # convert to list of dicts
    data = df_subset.to_dict(orient="records")
    return {"dataValues": data}
