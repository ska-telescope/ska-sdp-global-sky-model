"""This module contains tests for the helper_functions.py"""

import pytest
from astropy.coordinates import SkyCoord

from ska_sdp_global_sky_model.utilities.helper_functions import (
    calculate_percentage,
    convert_arcminutes_to_radians,
    convert_ra_dec_to_skycoord,
)


class TestConvertRaDecToSkyCoord:
    """Tests for the convert_ra_dec_to_skycoord function"""

    def test_create_point_icrs(self):
        """Test creating a point in ICRS frame"""
        ra_deg = 10.68458
        dec_deg = 41.26917
        point_icrs = convert_ra_dec_to_skycoord(ra_deg, dec_deg)

        # Check that the frame is ICRS
        assert point_icrs.frame.name == "icrs"

        # Check that the coordinates match the input values
        assert point_icrs.ra.deg == ra_deg
        assert point_icrs.dec.deg == dec_deg
        assert point_icrs.to_string("decimal") == "10.6846 41.2692"
        assert point_icrs.to_string("dms") == "10d41m04.488s 41d16m09.012s"
        assert point_icrs.to_string("hmsdms") == "00h42m44.2992s +41d16m09.012s"

    def test_valid_input(self):
        """Test valid input"""
        ra_deg = 10.0
        dec_deg = 20.0
        skycoord = convert_ra_dec_to_skycoord(ra_deg, dec_deg)
        assert isinstance(skycoord, SkyCoord)

    def test_invalid_ra(self):
        """Test invalid ra input"""
        with pytest.raises(ValueError, match="RA must be between 0 and 360 degrees"):
            convert_ra_dec_to_skycoord(400.0, 30.0)

    def test_invalid_dec(self):
        """Test invalid dec input"""
        with pytest.raises(ValueError, match="Dec must be between -90 and 90 degrees"):
            convert_ra_dec_to_skycoord(20.0, 100.0)

    def test_edge_cases(self):
        """Test with edge cases (e.g., RA = 0, RA = 360, Dec = -90, Dec = 90)"""
        assert isinstance(convert_ra_dec_to_skycoord(0.0, 0.0), SkyCoord)
        assert isinstance(convert_ra_dec_to_skycoord(360.0, 0.0), SkyCoord)
        assert isinstance(convert_ra_dec_to_skycoord(180.0, -90.0), SkyCoord)
        assert isinstance(convert_ra_dec_to_skycoord(270.0, 90.0), SkyCoord)


class TestConvertArchminutesToRadians:
    """Tests for the convert_arcminutes_to_radians function"""

    def test_valid_input(self):
        """Valid numeric input."""
        test_data = [
            (120.0, 0.03490658503988659),
            (0.0, 0.0),
            (-60.0, -0.017453292519943295),
        ]
        for arcmin, expected_rad in test_data:
            actual_rad = convert_arcminutes_to_radians(arcmin)
            assert actual_rad == expected_rad

    def test_invalid_input(self):
        """Non-numeric input (raises TypeError)"""
        non_numeric_values = ["hello", True, None]
        for value in non_numeric_values:
            with pytest.raises(TypeError):
                convert_arcminutes_to_radians(value)


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
