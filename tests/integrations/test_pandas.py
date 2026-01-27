"""Tests for pandas integration functions."""

import pandas as pd

from dhis2eo.integrations.pandas import (
    dataframe_to_dhis2_json,
    format_value_for_dhis2,
)


class TestFormatValueForDhis2:
    """Tests for format_value_for_dhis2 function."""

    def test_small_value_no_scientific_notation(self):
        """Small values should be formatted as decimal strings, not scientific notation."""
        value = 8.24148e-05
        result = format_value_for_dhis2(value)
        assert result == "0.0000824148"
        assert "e" not in result.lower()

    def test_very_small_value(self):
        """Very small values should be formatted correctly."""
        value = 1e-10
        result = format_value_for_dhis2(value)
        assert result == "0.0000000001"
        assert "e" not in result.lower()

    def test_normal_value(self):
        """Normal values should be formatted without trailing zeros."""
        value = 123.456
        result = format_value_for_dhis2(value)
        assert result == "123.456"

    def test_integer_value(self):
        """Integer values should not have decimal point."""
        value = 100.0
        result = format_value_for_dhis2(value)
        assert result == "100"

    def test_nan_value(self):
        """NaN values should pass through unchanged."""
        import math

        result = format_value_for_dhis2(float("nan"))
        assert math.isnan(result)

    def test_zero_value(self):
        """Zero should be formatted as '0'."""
        result = format_value_for_dhis2(0.0)
        assert result == "0"


class TestDataframeToDhis2Json:
    """Tests for dataframe_to_dhis2_json function."""

    def test_values_are_strings_not_scientific_notation(self):
        """Values in output should be decimal strings, not scientific notation."""
        df = pd.DataFrame(
            {
                "org_unit": ["OU001", "OU002"],
                "period": ["2024-01-15", "2024-01-16"],
                "value": [8.24148e-05, 1.5e-06],
            }
        )

        result = dataframe_to_dhis2_json(
            df,
            data_element_id="DE001",
            org_unit_col="org_unit",
            period_col="period",
            value_col="value",
        )

        data_values = result["dataValues"]
        for dv in data_values:
            # Value should be a string
            assert isinstance(dv["value"], str)
            # Should not contain scientific notation
            assert "e" not in dv["value"].lower()

    def test_nan_values_are_filtered(self):
        """NaN values should be filtered out."""
        df = pd.DataFrame(
            {
                "org_unit": ["OU001", "OU002", "OU003"],
                "period": ["2024-01-15", "2024-01-16", "2024-01-17"],
                "value": [1.0, float("nan"), 3.0],
            }
        )

        result = dataframe_to_dhis2_json(
            df,
            data_element_id="DE001",
            org_unit_col="org_unit",
            period_col="period",
            value_col="value",
        )

        assert len(result["dataValues"]) == 2

    def test_output_structure(self):
        """Output should have correct DHIS2 structure."""
        df = pd.DataFrame(
            {
                "org_unit": ["OU001"],
                "period": ["2024-01-15"],
                "value": [42.5],
            }
        )

        result = dataframe_to_dhis2_json(
            df,
            data_element_id="DE001",
            org_unit_col="org_unit",
            period_col="period",
            value_col="value",
        )

        assert "dataValues" in result
        assert len(result["dataValues"]) == 1

        dv = result["dataValues"][0]
        assert dv["orgUnit"] == "OU001"
        assert dv["dataElement"] == "DE001"
        assert dv["value"] == "42.5"
        assert "period" in dv
