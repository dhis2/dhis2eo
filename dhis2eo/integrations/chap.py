"""
Chap CSV export utilities.

Context
-------
Chap (Climate Health Analytics Platform) uses a simple *wide* CSV convention for model
training datasets:

- One row per (time_period, location)
- Reserved fields (Chap-recognized columns):
    * Required for training: time_period, location, disease_cases
    * Reserved but optional: population
- All other columns are treated as covariates/features (e.g. temperature, precipitation).

This module provides a thin export layer from a harmonized pandas DataFrame to a
Chap-compatible CSV. It deliberately does NOT:
- fetch or harmonize data (that belongs upstream, e.g. CLIM-181 pipelines)
- decide which columns are "features" vs "targets" beyond Chap's reserved fields
- run modeling or prediction

Design: explicit column mapping
-------------------------------
Input data may use different column names. Rather than guessing (fragile), this module
requires an explicit mapping from Chap-reserved output fields to input column names.

Example
-------
>>> column_map = {
...     "time_period": "period",
...     "location": "org_id",
...     "disease_cases": "dengue_cases",
...     "population": "population",   # optional
... }
>>> csv_text = dataframe_to_chap_csv(df, column_map=column_map)

Temporal continuity
-------------------
Chap expects temporal continuity within each location time-series. This exporter does
NOT impute missing periods (e.g., filling zeros), because that can silently introduce
misleading training signals. Instead, it can detect gaps and:
- raise an error (default)
- emit a warning
- ignore

See `continuity_policy` in `dataframe_to_chap_csv`.
"""

from __future__ import annotations

import re
import warnings
from typing import Mapping, Optional, Sequence, Union, Literal

import pandas as pd


# Common period encodings that show up in harmonized climate/health pipelines.
_PERIOD_YYYYMM = re.compile(r"^\d{6}$")            # e.g. 199801
_PERIOD_YYYY_MM = re.compile(r"^\d{4}-\d{2}$")     # e.g. 1998-01
_PERIOD_YYYY_WWW = re.compile(r"^\d{4}-W\d{2}$")   # e.g. 2023-W01


# Chap-reserved fields. In CLIM-360 scope, the first three are mandatory (training data).
REQUIRED_RESERVED_FIELDS = ("time_period", "location", "disease_cases")
OPTIONAL_RESERVED_FIELDS = ("population",)

ContinuityPolicy = Literal["error", "warn", "ignore"]


def _normalize_time_period(series: pd.Series, freq: str = "monthly") -> pd.Series:
    """Normalize a period-like Series to Chap-compatible time_period strings.

    Monthly: YYYY-MM
      Accepts YYYYMM, YYYY-MM, or datetime-like strings.

    Weekly: YYYY-Wnn (ISO week)
      Accepts YYYY-Wnn or datetime-like strings.

    Notes
    -----
    This is an internal helper used by dataframe_to_chap_csv. It is not intended
    as a general-purpose time normalization utility across dhis2eo.
    """
    s = series.astype(str).str.strip()

    if freq == "monthly":
        # Convert YYYYMM -> YYYY-MM where applicable
        mask = s.str.match(_PERIOD_YYYYMM)
        if mask.any():
            s.loc[mask] = s.loc[mask].str.slice(0, 4) + "-" + s.loc[mask].str.slice(4, 6)

        # For any remaining values not already YYYY-MM, try parsing as date/datetime.
        needs_parse = ~s.str.match(_PERIOD_YYYY_MM)
        if needs_parse.any():
            parsed = pd.to_datetime(s[needs_parse], errors="coerce")
            ok = parsed.notna()
            # Only overwrite values that successfully parsed.
            s.loc[parsed[ok].index] = parsed[ok].dt.strftime("%Y-%m")

        # Final validation
        bad_mask = ~s.str.match(_PERIOD_YYYY_MM)
        if bad_mask.any():
            bad = s[bad_mask].unique()[:5]
            raise ValueError(f"Invalid monthly time_period values (expected YYYY-MM): {bad}")

        return s

    if freq == "weekly":
        # If already in YYYY-Wnn format, keep as-is.
        if s.str.match(_PERIOD_YYYY_WWW).all():
            return s

        # Otherwise parse as datetime-like and derive ISO year/week.
        parsed = pd.to_datetime(s, errors="coerce")
        if parsed.isna().any():
            bad = s[parsed.isna()].unique()[:5]
            raise ValueError(f"Invalid weekly time_period values (expected YYYY-Wnn or date-like): {bad}")

        iso = parsed.dt.isocalendar()
        return iso["year"].astype(str) + "-W" + iso["week"].astype(int).astype(str).str.zfill(2)

    raise ValueError("freq must be 'monthly' or 'weekly'")


def _validate_temporal_continuity(
    df: pd.DataFrame,
    *,
    freq: str,
    location_col: str = "location",
    time_col: str = "time_period",
    max_locations: int = 5,
    max_missing_per_location: int = 6,
) -> dict[str, list[str]]:
    """Detect temporal gaps per location.

    Returns a dict: {location: [missing_periods...]} for locations with gaps.
    Empty dict means continuity holds (given min..max range per location).

    Notes
    -----
    - Continuity is checked within each location between its min and max time_period.
    - This does not attempt to "fix" gaps; it only reports them.
    """
    if freq not in ("monthly", "weekly"):
        raise ValueError("freq must be 'monthly' or 'weekly'")

    gaps: dict[str, list[str]] = {}

    if df.empty:
        return gaps

    # Work on a small subset (location, time) to minimize memory/side effects.
    sub = df[[location_col, time_col]].dropna().copy()

    if freq == "monthly":
        # time_period already normalized to YYYY-MM; convert to Period for easy range checks.
        sub["_p"] = pd.PeriodIndex(sub[time_col].astype(str), freq="M")
        for loc, g in sub.groupby(location_col, sort=False):
            periods = pd.PeriodIndex(g["_p"].unique(), freq="M").sort_values()
            if len(periods) <= 1:
                continue
            expected = pd.period_range(periods.min(), periods.max(), freq="M")
            missing = expected.difference(periods)
            if len(missing) > 0:
                gaps[str(loc)] = [p.strftime("%Y-%m") for p in missing[:max_missing_per_location]]

        return gaps

    # weekly
    # Convert ISO week strings to a datetime representing the Monday of that ISO week.
    # Example: 2023-W01 -> 2023-W01-1 (ISO weekday 1 = Monday)
    sub["_week_start"] = pd.to_datetime(
        sub[time_col].astype(str) + "-1",
        format="%G-W%V-%u",
        errors="coerce",
    )
    if sub["_week_start"].isna().any():
        bad = sub.loc[sub["_week_start"].isna(), time_col].astype(str).unique()[:5]
        raise ValueError(f"Invalid weekly time_period values (expected YYYY-Wnn): {bad}")

    for loc, g in sub.groupby(location_col, sort=False):
        starts = pd.DatetimeIndex(g["_week_start"].unique()).sort_values()
        if len(starts) <= 1:
            continue
        expected = pd.date_range(starts.min(), starts.max(), freq="W-MON")
        missing = expected.difference(starts)
        if len(missing) > 0:
            # Convert back to ISO week strings
            iso = missing.isocalendar()
            missing_str = (iso["year"].astype(str) + "-W" + iso["week"].astype(int).astype(str).str.zfill(2)).tolist()
            gaps[str(loc)] = missing_str[:max_missing_per_location]

    return gaps


def dataframe_to_chap_csv(
    df: pd.DataFrame,
    *,
    column_map: Mapping[str, str],
    freq: str = "monthly",
    continuity_policy: ContinuityPolicy = "error",
    include_other_cols: bool = True,
    value_cols: Optional[Sequence[str]] = None,
    drop_cols: Optional[Sequence[str]] = ("org_name", "population_year"),
    sort: bool = True,
    output_path: Optional[str] = None,
) -> Union[str, None]:
    """Convert a harmonized DataFrame to a Chap-compatible wide CSV.

    Parameters
    ----------
    df:
        Input DataFrame (ideally already harmonized). Typically one row per
        (location, time_period) with additional covariate columns.

    column_map:
        Mapping from Chap-reserved output field names to input column names.

        Required keys for CLIM-360 (training data export):
            - "time_period"
            - "location"
            - "disease_cases"

        Optional key:
            - "population"

        Example:
            {
              "time_period": "period",
              "location": "org_id",
              "disease_cases": "dengue_cases",
              "population": "population",
            }

        All values must be column names present in df.

    freq:
        Period normalization strategy for time_period:
        - "monthly" -> YYYY-MM
        - "weekly"  -> YYYY-Wnn

    continuity_policy:
        How to handle temporal gaps per location:
        - "error"  (default): raise ValueError if gaps are detected
        - "warn": emit a UserWarning and continue
        - "ignore": skip the continuity check

        This function does NOT impute missing periods.

    include_other_cols:
        If True (default), include all non-reserved, non-dropped columns as covariates.

    value_cols:
        Optional explicit list of additional covariate columns to include (in addition to
        reserved fields). If provided, this takes precedence over include_other_cols and
        only these columns (plus reserved fields) are included.

    drop_cols:
        Columns to drop by default if present. Convenience for common metadata.
        Safe to set to None.

    sort:
        Sort output rows by (location, time_period) for deterministic output.

    output_path:
        If provided, write CSV to this path and return None. Otherwise, return CSV text.

    Returns
    -------
    str or None
        CSV content as a string if output_path is None, else None.
    """
    if not isinstance(column_map, Mapping):
        raise TypeError("column_map must be a mapping of Chap field -> input column name")

    # Enforce required Chap-reserved fields for training exports.
    missing_keys = [k for k in REQUIRED_RESERVED_FIELDS if k not in column_map]
    if missing_keys:
        raise KeyError(
            "column_map is missing required Chap fields for training export: "
            + ", ".join(missing_keys)
        )

    # Validate that mapped input columns exist.
    mapped_inputs = list(column_map.values())
    missing_inputs = [c for c in mapped_inputs if c not in df.columns]
    if missing_inputs:
        raise KeyError(f"Input DataFrame is missing mapped columns: {missing_inputs}")

    out = df.copy()

    # Drop common metadata columns (best-effort).
    if drop_cols:
        out = out.drop(columns=[c for c in drop_cols if c in out.columns], errors="ignore")

    # Rename input columns to Chap-reserved output field names.
    # Keys in column_map are Chap fields; values are input df column names.
    rename_dict = {column_map[k]: k for k in column_map.keys()}
    out = out.rename(columns=rename_dict)

    # Normalize time_period formatting after renaming.
    out["time_period"] = _normalize_time_period(out["time_period"], freq=freq)

    # Detect gaps (unless explicitly ignored).
    if continuity_policy not in ("error", "warn", "ignore"):
        raise ValueError("continuity_policy must be one of: 'error', 'warn', 'ignore'")

    if continuity_policy != "ignore":
        gaps = _validate_temporal_continuity(out, freq=freq)
        if gaps:
            # Produce a compact, actionable message.
            sample_items = list(gaps.items())[:5]
            sample_str = "; ".join([f"{loc}: {missing}" for loc, missing in sample_items])
            msg = (
                f"Temporal continuity check failed: detected missing {freq} periods for "
                f"{len(gaps)} location(s). Examples: {sample_str}"
            )
            if continuity_policy == "error":
                raise ValueError(msg)
            warnings.warn(msg, UserWarning)

    # Decide which columns to include in output.
    reserved_present = [k for k in REQUIRED_RESERVED_FIELDS if k in out.columns]
    for k in OPTIONAL_RESERVED_FIELDS:
        if k in column_map and k in out.columns:
            reserved_present.append(k)

    # Determine additional covariate columns.
    if value_cols is not None:
        missing_value_cols = [c for c in value_cols if c not in out.columns]
        if missing_value_cols:
            raise KeyError(f"value_cols not found in DataFrame: {missing_value_cols}")
        covariates = list(value_cols)
    elif include_other_cols:
        covariates = [c for c in out.columns if c not in set(reserved_present)]
    else:
        covariates = []

    # Stable, Chap-friendly column order:
    ordered_cols = ["time_period", "location", "disease_cases"]
    if "population" in reserved_present:
        ordered_cols.append("population")

    # Preserve covariate order as in the DataFrame to reduce surprises.
    ordered_cols.extend([c for c in covariates if c not in ordered_cols])

    out = out[ordered_cols]

    if sort:
        out = out.sort_values(["location", "time_period"])

    if output_path:
        out.to_csv(output_path, index=False)
        return None

    return out.to_csv(index=False)
