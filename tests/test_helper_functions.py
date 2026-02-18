"""This module contains tests for the helper_functions.py"""

from ska_sdp_global_sky_model.utilities.helper_functions import (
    calculate_percentage,
)


class TestCalculatePercentage:
    """Tests for the calculate_percentage function"""

    def test_calculate_percentage_valid_inputs(self):
        """Tests the function with valid dividend and divisor values."""
        assert calculate_percentage(25, 100) == 25.00
        assert calculate_percentage(3.14, 12.56) == 25.00
        assert calculate_percentage(1, 1) == 100.00

    def test_calculate_percentage_zero_divisor(self):
        """Tests the function with a zero divisor, expecting a swallowed error and 0.0 returned."""
        assert calculate_percentage(5, 0) == 0.0

    def test_calculate_percentage_negative_values(self):
        """Tests the function with negative dividend and/or divisor."""
        assert calculate_percentage(-20, 100) == -20.00
        assert calculate_percentage(15, -75) == -20.00

    def test_calculate_percentage_rounding(self):
        """Tests the function's rounding behavior."""
        assert calculate_percentage(1.2345, 10) == 12.34
        assert calculate_percentage(5.9999, 10) == 60.00
