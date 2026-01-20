import pandas as pd
import pytest

from dhis2eo.integrations.chap import dataframe_to_chap_csv, _normalize_time_period


def test_normalize_monthly_accepts_mixed_inputs():
    s = pd.Series(["199801", "1998-02", "1998-03-15"])
    out = _normalize_time_period(s, freq="monthly")
    assert out.tolist() == ["1998-01", "1998-02", "1998-03"]


def test_normalize_monthly_rejects_invalid_values():
    s = pd.Series(["1998-13", "nope"])
    with pytest.raises(ValueError):
        _normalize_time_period(s, freq="monthly")


def test_normalize_weekly_accepts_dates_and_formats_iso_week():
    s = pd.Series(["2020-01-01", "2020-01-08"])
    out = _normalize_time_period(s, freq="weekly")
    assert out.str.match(r"^\d{4}-W\d{2}$").all()
    assert out.iloc[0].startswith("2020-W")


def test_dataframe_to_chap_csv_requires_column_map_keys():
    df = pd.DataFrame(
        {
            "org_id": ["X"],
            "period": ["1998-01"],
            "dengue_cases": [1.0],
        }
    )
    with pytest.raises(KeyError):
        dataframe_to_chap_csv(
            df,
            column_map={
                "time_period": "period",
                "location": "org_id",
                # "disease_cases": "dengue_cases"
            },
        )


def test_dataframe_to_chap_csv_requires_mapped_input_columns_exist():
    df = pd.DataFrame({"org_id": ["X"], "period": ["1998-01"], "dengue_cases": [1.0]})
    with pytest.raises(KeyError):
        dataframe_to_chap_csv(
            df,
            column_map={
                "time_period": "period",
                "location": "org_id",
                "disease_cases": "missing_cases_col",
            },
        )


def test_dataframe_to_chap_csv_maps_reserved_fields_and_drops_metadata_by_default():
    df = pd.DataFrame(
        {
            "org_id": ["LAO_ADM1_0001", "LAO_ADM1_0001"],
            "period": ["1998-01", "1998-02"],
            "org_name": ["ATTAPU", "ATTAPU"],  # should be dropped by default
            "dengue_cases": [0.0, 1.0],        # maps -> disease_cases
            "t2m_mean_c_month": [25.1, 26.7],
            "tp_sum_mm_month": [0.04, 0.54],
            "rh2m_mean_month": [70.0, 71.0],
            "population": [99933.4, 99933.4],  # optional reserved field
            "population_year": [2020, 2020],   # should be dropped by default
        }
    )

    csv_text = dataframe_to_chap_csv(
        df,
        column_map={
            "time_period": "period",
            "location": "org_id",
            "disease_cases": "dengue_cases",
            "population": "population",
        },
        continuity_policy="error",
    )

    lines = csv_text.strip().splitlines()
    header = lines[0].split(",")

    assert header[:4] == ["time_period", "location", "disease_cases", "population"]
    assert "org_name" not in header
    assert "population_year" not in header

    assert "t2m_mean_c_month" in header
    assert "tp_sum_mm_month" in header
    assert "rh2m_mean_month" in header


def test_dataframe_to_chap_csv_population_is_optional():
    df = pd.DataFrame(
        {
            "org_id": ["X"],
            "period": ["1998-01"],
            "dengue_cases": [1.0],
            "temp": [25.0],
        }
    )

    csv_text = dataframe_to_chap_csv(
        df,
        column_map={
            "time_period": "period",
            "location": "org_id",
            "disease_cases": "dengue_cases",
        },
        continuity_policy="error",
    )
    header = csv_text.splitlines()[0].split(",")

    assert header[:3] == ["time_period", "location", "disease_cases"]
    assert "population" not in header
    assert "temp" in header


def test_dataframe_to_chap_csv_value_cols_limits_covariates():
    df = pd.DataFrame(
        {
            "org_id": ["X"],
            "period": ["1998-01"],
            "dengue_cases": [1.0],
            "temp": [25.0],
            "precip": [12.0],
        }
    )

    csv_text = dataframe_to_chap_csv(
        df,
        column_map={
            "time_period": "period",
            "location": "org_id",
            "disease_cases": "dengue_cases",
        },
        value_cols=["precip"],
        continuity_policy="error",
    )

    header = csv_text.splitlines()[0].split(",")
    assert header == ["time_period", "location", "disease_cases", "precip"]
    assert "temp" not in header


def test_dataframe_to_chap_csv_sort_is_deterministic():
    df = pd.DataFrame(
        {
            "org_id": ["B", "A", "A"],
            "period": ["1998-02", "1998-02", "1998-01"],
            "dengue_cases": [1.0, 2.0, 3.0],
            "x": [10, 20, 30],
        }
    )

    csv_text = dataframe_to_chap_csv(
        df,
        column_map={
            "time_period": "period",
            "location": "org_id",
            "disease_cases": "dengue_cases",
        },
        sort=True,
        continuity_policy="error",
    )

    lines = csv_text.strip().splitlines()
    assert lines[1].startswith("1998-01,A,")
    assert lines[2].startswith("1998-02,A,")
    assert lines[3].startswith("1998-02,B,")


def test_dataframe_to_chap_csv_temporal_continuity_error_on_gaps():
    # Missing 1998-02 for location A should trigger an error by default.
    df = pd.DataFrame(
        {
            "org_id": ["A", "A", "B"],
            "period": ["1998-01", "1998-03", "1998-01"],
            "dengue_cases": [1.0, 2.0, 0.0],
        }
    )

    with pytest.raises(ValueError, match="Temporal continuity check failed"):
        dataframe_to_chap_csv(
            df,
            column_map={
                "time_period": "period",
                "location": "org_id",
                "disease_cases": "dengue_cases",
            },
            continuity_policy="error",
        )


def test_dataframe_to_chap_csv_temporal_continuity_warn_on_gaps():
    df = pd.DataFrame(
        {
            "org_id": ["A", "A"],
            "period": ["1998-01", "1998-03"],  # gap
            "dengue_cases": [1.0, 2.0],
            "x": [10, 20],
        }
    )

    with pytest.warns(UserWarning, match="Temporal continuity check failed"):
        csv_text = dataframe_to_chap_csv(
            df,
            column_map={
                "time_period": "period",
                "location": "org_id",
                "disease_cases": "dengue_cases",
            },
            continuity_policy="warn",
        )
    assert "time_period,location,disease_cases" in csv_text


def test_dataframe_to_chap_csv_temporal_continuity_ignore_on_gaps():
    df = pd.DataFrame(
        {
            "org_id": ["A", "A"],
            "period": ["1998-01", "1998-03"],  # gap
            "dengue_cases": [1.0, 2.0],
        }
    )

    csv_text = dataframe_to_chap_csv(
        df,
        column_map={
            "time_period": "period",
            "location": "org_id",
            "disease_cases": "dengue_cases",
        },
        continuity_policy="ignore",
    )
    assert "1998-01" in csv_text
    assert "1998-03" in csv_text


def test_dataframe_to_chap_csv_writes_file(tmp_path):
    df = pd.DataFrame(
        {
            "ou_uid": ["X", "X"],
            "month": ["199801", "199802"],
            "cases_total": [1.0, 2.0],
            "temperature": [25.0, 26.0],
        }
    )

    out_path = tmp_path / "chap.csv"
    res = dataframe_to_chap_csv(
        df,
        column_map={
            "time_period": "month",
            "location": "ou_uid",
            "disease_cases": "cases_total",
        },
        output_path=str(out_path),
        continuity_policy="error",
    )

    assert res is None
    assert out_path.exists()
    content = out_path.read_text()
    assert content.splitlines()[0].startswith("time_period,location,disease_cases")
    assert "1998-01,X,1.0" in content
    assert "1998-02,X,2.0" in content
