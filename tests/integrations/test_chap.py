import pandas as pd
import pytest

from dhis2eo.integrations.chap import (
    dataframe_to_chap_csv,
    find_temporal_gaps,
    _normalize_time_period,
)


# -----------------------------------------------------------------------------
# Time normalization
# -----------------------------------------------------------------------------

def test_normalize_monthly_accepts_common_formats():
    s = pd.Series(["199801", "1998-02", "1998-03-15"])
    out = _normalize_time_period(s, freq="monthly")
    assert out.tolist() == ["1998-01", "1998-02", "1998-03"]


def test_normalize_monthly_rejects_invalid_values():
    s = pd.Series(["1998-13"])
    with pytest.raises(ValueError):
        _normalize_time_period(s, freq="monthly")


def test_normalize_weekly_accepts_date_like_input():
    s = pd.Series(["2020-01-01"])
    out = _normalize_time_period(s, freq="weekly")
    assert out.str.match(r"^\d{4}-W\d{2}$").all()


# -----------------------------------------------------------------------------
# Schema enforcement
# -----------------------------------------------------------------------------

def test_dataframe_to_chap_csv_requires_mandatory_chap_fields_in_column_map():
    df = pd.DataFrame(
        {
            "org_id": ["A"],
            "period": ["1998-01"],
            "cases": [1],
        }
    )

    with pytest.raises(KeyError):
        dataframe_to_chap_csv(
            df,
            column_map={
                "time_period": "period",
                "location": "org_id",
                # "disease_cases": "cases"  # missing
            },
            continuity_policy="ignore",  # avoid triggering continuity checks for this test
        )


def test_dataframe_to_chap_csv_requires_mapped_input_columns_exist():
    df = pd.DataFrame(
        {
            "org_id": ["A"],
            "period": ["1998-01"],
            # "cases" missing
        }
    )

    with pytest.raises(KeyError):
        dataframe_to_chap_csv(
            df,
            column_map={
                "time_period": "period",
                "location": "org_id",
                "disease_cases": "cases",
            },
            continuity_policy="ignore",
        )


# -----------------------------------------------------------------------------
# Global continuity gaps
# -----------------------------------------------------------------------------

def test_find_temporal_gaps_global_window_inferred():
    # Global window inferred from dataset: 1998-01 .. 1998-03
    df = pd.DataFrame(
        {
            "org_id": ["A", "A", "B"],
            "period": ["1998-01", "1998-03", "1998-01"],
            "cases": [1, 2, 0],
        }
    )

    gaps = find_temporal_gaps(
        df,
        column_map={
            "time_period": "period",
            "location": "org_id",
            "disease_cases": "cases",
        },
        freq="monthly",
    )

    assert gaps == {"A": ["1998-02"], "B": ["1998-02", "1998-03"]}


def test_find_temporal_gaps_with_explicit_window():
    df = pd.DataFrame(
        {
            "org_id": ["A"],
            "period": ["1998-02"],
            "cases": [1],
        }
    )

    gaps = find_temporal_gaps(
        df,
        column_map={
            "time_period": "period",
            "location": "org_id",
            "disease_cases": "cases",
        },
        start="1998-01",
        end="1998-03",
        freq="monthly",
    )

    assert gaps == {"A": ["1998-01", "1998-03"]}


# -----------------------------------------------------------------------------
# continuity_policy wiring
# -----------------------------------------------------------------------------

def test_dataframe_to_chap_csv_errors_on_gaps_by_default():
    df = pd.DataFrame(
        {
            "org_id": ["A", "A"],
            "period": ["1998-01", "1998-03"],  # missing 1998-02 in inferred window
            "cases": [1, 2],
        }
    )

    with pytest.raises(ValueError):
        dataframe_to_chap_csv(
            df,
            column_map={
                "time_period": "period",
                "location": "org_id",
                "disease_cases": "cases",
            },
        )


def test_dataframe_to_chap_csv_warns_on_gaps_when_configured():
    df = pd.DataFrame(
        {
            "org_id": ["A", "A"],
            "period": ["1998-01", "1998-03"],  # missing 1998-02 in inferred window
            "cases": [1, 2],
        }
    )

    with pytest.warns(UserWarning):
        csv_text = dataframe_to_chap_csv(
            df,
            column_map={
                "time_period": "period",
                "location": "org_id",
                "disease_cases": "cases",
            },
            continuity_policy="warn",
        )
    assert csv_text.startswith("time_period,location,disease_cases")


def test_dataframe_to_chap_csv_ignores_gaps_when_configured():
    df = pd.DataFrame(
        {
            "org_id": ["A", "A"],
            "period": ["1998-01", "1998-03"],  # gap
            "cases": [1, 2],
            "temp": [25.0, 26.0],
        }
    )

    csv_text = dataframe_to_chap_csv(
        df,
        column_map={
            "time_period": "period",
            "location": "org_id",
            "disease_cases": "cases",
        },
        continuity_policy="ignore",
    )

    header = csv_text.splitlines()[0].split(",")
    assert header[:3] == ["time_period", "location", "disease_cases"]
    assert "temp" in header


# -----------------------------------------------------------------------------
# Happy path and file writing
# -----------------------------------------------------------------------------

def test_dataframe_to_chap_csv_output_ok_when_continuous():
    df = pd.DataFrame(
        {
            "org_id": ["A", "A"],
            "period": ["1998-01", "1998-02"],
            "cases": [1, 2],
            "temp": [25.0, 26.0],
        }
    )

    csv_text = dataframe_to_chap_csv(
        df,
        column_map={
            "time_period": "period",
            "location": "org_id",
            "disease_cases": "cases",
        },
    )

    assert csv_text.startswith("time_period,location,disease_cases,temp")
    assert "1998-01,A,1" in csv_text
    assert "1998-02,A,2" in csv_text


def test_dataframe_to_chap_csv_writes_file(tmp_path):
    df = pd.DataFrame(
        {
            "org_id": ["A", "A"],
            "period": ["1998-01", "1998-02"],
            "cases": [1, 2],
        }
    )

    out_path = tmp_path / "chap.csv"
    res = dataframe_to_chap_csv(
        df,
        column_map={
            "time_period": "period",
            "location": "org_id",
            "disease_cases": "cases",
        },
        output_path=str(out_path),
    )

    assert res is None
    assert out_path.exists()
    content = out_path.read_text()
    assert content.splitlines()[0].startswith("time_period,location,disease_cases")
    assert "1998-01,A,1" in content
    assert "1998-02,A,2" in content
