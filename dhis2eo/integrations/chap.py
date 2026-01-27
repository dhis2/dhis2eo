"""
Chap CSV export utilities.

This module provides a strict exporter for producing CSV files that are
directly consumable by Chap (Climate Health Analytics Platform).

Key principles
--------------
- One row per (time_period, location)
- Required Chap fields: time_period, location, disease_cases
- Optional reserved field: population
- All locations must share the same global time window
- Missing periods are never imputed

Temporal continuity
-------------------
Temporal continuity is enforced over a *global* time window:
- If start/end are provided, they define the window
- Otherwise, the window is inferred from the dataset (earliest..latest period)

If any location is missing any period in that window, the dataset is not
Chap-ready.

For diagnostics and data repair workflows, use `find_temporal_gaps(...)`,
which returns all missing periods per location.
"""

from __future__ import annotations

import re
import warnings
from typing import Literal

import pandas as pd

from ..utils.types import MaybeString, MaybeStringSequence


# ---------------------------------------------------------------------
# Constants and types
# ---------------------------------------------------------------------

_PERIOD_YYYYMM = re.compile(r"^\d{6}$")
_PERIOD_YYYY_MM = re.compile(r"^\d{4}-\d{2}$")
_PERIOD_YYYY_WWW = re.compile(r"^\d{4}-W\d{2}$")

REQUIRED_RESERVED_FIELDS = ("time_period", "location", "disease_cases")
OPTIONAL_RESERVED_FIELDS = ("population",)

ContinuityPolicy = Literal["error", "warn", "ignore"]


# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------

def _normalize_time_period(series: pd.Series, freq: str = "monthly") -> pd.Series:
    """Normalize period-like values to Chap-compatible strings."""
    s = series.astype(str).str.strip()

    if freq == "monthly":
        mask = s.str.match(_PERIOD_YYYYMM)
        if mask.any():
            s.loc[mask] = s.loc[mask].str[:4] + "-" + s.loc[mask].str[4:6]

        needs_parse = ~s.str.match(_PERIOD_YYYY_MM)
        if needs_parse.any():
            parsed = pd.to_datetime(s[needs_parse], errors="coerce")
            ok = parsed.notna()
            s.loc[parsed[ok].index] = parsed[ok].dt.strftime("%Y-%m")

        if (~s.str.match(_PERIOD_YYYY_MM)).any():
            bad = s[~s.str.match(_PERIOD_YYYY_MM)].unique()[:5]
            raise ValueError(f"Invalid monthly time_period values: {bad}")
        
        # Validate month range (e.g. reject 1998-13) by constructing a PeriodIndex.
        try:
            pd.PeriodIndex(s, freq="M")
        except Exception as e:
            bad = s.unique()[:5]
            raise ValueError(f"Invalid monthly time_period values: {bad}") from e

        return s

    if freq == "weekly":
        if s.str.match(_PERIOD_YYYY_WWW).all():
            return s

        parsed = pd.to_datetime(s, errors="coerce")
        if parsed.isna().any():
            bad = s[parsed.isna()].unique()[:5]
            raise ValueError(f"Invalid weekly time_period values: {bad}")

        iso = parsed.dt.isocalendar()
        return iso["year"].astype(str) + "-W" + iso["week"].astype(int).astype(str).str.zfill(2)

    raise ValueError("freq must be 'monthly' or 'weekly'")


def _rename_to_reserved_fields(df: pd.DataFrame, column_map: dict[str, str]) -> pd.DataFrame:
    rename = {v: k for k, v in column_map.items()}
    return df.rename(columns=rename)


def _require_columns(df: pd.DataFrame, column_map: dict[str, str]) -> None:
    missing_keys = [k for k in REQUIRED_RESERVED_FIELDS if k not in column_map]
    if missing_keys:
        raise KeyError(f"column_map missing required fields: {missing_keys}")

    missing_cols = [v for v in column_map.values() if v not in df.columns]
    if missing_cols:
        raise KeyError(f"Input DataFrame missing columns: {missing_cols}")

def _expected_period_strings(
    normalized_time_period: pd.Series,
    *,
    freq: str,
    start: MaybeString = None,
    end: MaybeString = None,
) -> list[str]:
    """
    Build the expected global time grid as Chap-formatted strings.

    - If start/end are provided, they define the window.
    - Otherwise, infer from the dataset (earliest..latest period).
    """
    if normalized_time_period.empty:
        return []

    if freq == "monthly":
        p = pd.PeriodIndex(normalized_time_period.astype(str), freq="M")
        start_p = pd.Period(start, freq="M") if start else p.min()
        end_p = pd.Period(end, freq="M") if end else p.max()
        exp = pd.period_range(start_p, end_p, freq="M")
        return [x.strftime("%Y-%m") for x in exp]

    if freq == "weekly":
        wk = pd.to_datetime(
            normalized_time_period.astype(str) + "-1",
            format="%G-W%V-%u",
            errors="coerce",
        )
        if wk.isna().any():
            raise ValueError("Invalid weekly time_period values")

        start_d = pd.to_datetime(start + "-1", format="%G-W%V-%u") if start else wk.min()
        end_d = pd.to_datetime(end + "-1", format="%G-W%V-%u") if end else wk.max()

        exp = pd.date_range(start_d, end_d, freq="W-MON")
        iso = exp.isocalendar()
        return (
            iso["year"].astype(str)
            + "-W"
            + iso["week"].astype(int).astype(str).str.zfill(2)
        ).tolist()

    raise ValueError("freq must be 'monthly' or 'weekly'")


def _reindex_to_full_grid(df: pd.DataFrame, *, expected_periods: list[str]) -> pd.DataFrame:
    """
    Materialize full (location × time_period) grid.

    Any missing (location, time_period) rows become real rows with NaN values
    for disease_cases/covariates/etc.
    """
    if df.empty or not expected_periods:
        return df

    locations = df["location"].astype(str).unique().tolist()
    full_index = pd.MultiIndex.from_product(
        [locations, expected_periods],
        names=["location", "time_period"],
    )
    return (
        df.set_index(["location", "time_period"])
        .reindex(full_index)
        .reset_index()
    )

# ---------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------

def find_temporal_gaps(
    df: pd.DataFrame,
    *,
    column_map: dict[str, str],
    freq: str = "monthly",
    start: MaybeString = None,
    end: MaybeString = None,
) -> dict[str, list[str]]:
    """
    Return all missing periods per location over a global time window.

    The window is:
    - defined by start/end if provided
    - otherwise inferred from the dataset (earliest..latest period)

    No truncation, no diagnostics, no imputation.
    """
    for k in ("time_period", "location"):
        if k not in column_map:
            raise KeyError(f"column_map must include '{k}'")

    out = _rename_to_reserved_fields(df.copy(), column_map)
    out["time_period"] = _normalize_time_period(out["time_period"], freq=freq)

    if out.empty:
        return {}

    periods = out["time_period"]

    if freq == "monthly":
        p = pd.PeriodIndex(periods, freq="M")
        start_p = pd.Period(start, freq="M") if start else p.min()
        end_p = pd.Period(end, freq="M") if end else p.max()
        expected = pd.period_range(start_p, end_p, freq="M")

        gaps: dict[str, list[str]] = {}
        for loc, g in out.groupby("location"):
            have = pd.PeriodIndex(g["time_period"], freq="M").unique()
            missing = expected.difference(have)
            if len(missing):
                gaps[str(loc)] = [m.strftime("%Y-%m") for m in missing]
        return gaps

    if freq == "weekly":
        out["_week_start"] = pd.to_datetime(
            periods + "-1", format="%G-W%V-%u", errors="coerce"
        )
        if out["_week_start"].isna().any():
            raise ValueError("Invalid weekly time_period values")

        start_d = pd.to_datetime(start + "-1", format="%G-W%V-%u") if start else out["_week_start"].min()
        end_d = pd.to_datetime(end + "-1", format="%G-W%V-%u") if end else out["_week_start"].max()

        expected = pd.date_range(start_d, end_d, freq="W-MON")

        gaps = {}
        for loc, g in out.groupby("location"):
            have = pd.DatetimeIndex(g["_week_start"].unique())
            missing = expected.difference(have)
            if len(missing):
                iso = missing.isocalendar()
                gaps[str(loc)] = (
                    iso["year"].astype(str)
                    + "-W"
                    + iso["week"].astype(int).astype(str).str.zfill(2)
                ).tolist()

        return gaps
    
    raise ValueError("freq must be 'monthly' or 'weekly'")


# ---------------------------------------------------------------------
# Chap exporter
# ---------------------------------------------------------------------

def dataframe_to_chap_csv(
    df: pd.DataFrame,
    *,
    column_map: dict[str, str],
    freq: str = "monthly",
    start: MaybeString = None,
    end: MaybeString = None,
    continuity_policy: ContinuityPolicy = "error",
    include_other_cols: bool = True,
    value_cols: MaybeStringSequence = None,
    sort: bool = True,
    output_path: MaybeString = None,
) -> MaybeString:
    """
    Convert a harmonized DataFrame to a Chap-compatible CSV.

    This function is intentionally strict and opinionated: if the data is not
    Chap-ready, it fails fast by default.
    """
    _require_columns(df, column_map)

    out = _rename_to_reserved_fields(df.copy(), column_map)
    out["time_period"] = _normalize_time_period(out["time_period"], freq=freq)

    expected_periods = _expected_period_strings(
        out["time_period"], freq=freq, start=start, end=end
    )

    # Handle period continuity
    if continuity_policy != "ignore":
        gaps = find_temporal_gaps(
            df,
            column_map=column_map,
            freq=freq,
            start=start,
            end=end,
        )
        if gaps:
            msg = (
                f"Temporal continuity check failed: {len(gaps)} location(s) "
                f"have missing periods. Use find_temporal_gaps(...) to inspect all gaps."
            )
            if continuity_policy == "error":
                raise ValueError(msg)
            warnings.warn(msg, UserWarning)

    # Always materialize the complete grid for Chap consumption.
    # Missing rows become NaN (including disease_cases/covariates).
    out = _reindex_to_full_grid(out, expected_periods=expected_periods)

    # Reserved fields
    reserved = list(REQUIRED_RESERVED_FIELDS)
    if "population" in column_map and "population" in out.columns:
        reserved.append("population")

    # Build covariates fields
    if value_cols is not None:
        covariates = list(value_cols)
    elif include_other_cols:
        covariates = [c for c in out.columns if c not in reserved]
    else:
        covariates = []

    # Build final reserved + covariates
    cols = reserved + [c for c in covariates if c not in reserved]
    out = out[cols]

    # Sort
    if sort:
        out = out.sort_values(["location", "time_period"])

    # Save to CSV
    if output_path:
        out.to_csv(output_path, index=False)
        return None

    # Return
    return out.to_csv(index=False)
