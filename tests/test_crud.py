"""This module contains tests for the crud.py"""

# pylint: disable=duplicate-code

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from ska_sdp_global_sky_model.api.app.crud import (
    get_coverage_range,
    get_sky_components_by_criteria,
)
from ska_sdp_global_sky_model.configuration.config import Base

# Test database setup
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture
def test_db():
    """Create test database."""
    Base.metadata.create_all(bind=engine)  # pylint: disable=no-member
    db = TestingSessionLocal()
    yield db
    db.close()
    Base.metadata.drop_all(bind=engine)  # pylint: disable=no-member


class TestGetCoverageRange:
    """Tests for the get_coverage_range function"""

    def test_get_coverage_range_valid_input(self):
        """Tests the function with valid input values."""
        ra = 120.5
        dec = 30.2
        fov = 10.0
        ra_min, ra_max, dec_min, dec_max = get_coverage_range(ra, dec, fov)

        assert ra_min == 115.5
        assert ra_max == 125.5
        assert dec_min == 25.2
        assert dec_max == 35.2

    def test_get_coverage_range_zero_fov(self):
        """Tests the function with zero field of view."""
        ra = 120.5
        dec = 30.2
        fov = 0.0

        with pytest.raises(ValueError) as excinfo:
            get_coverage_range(ra, dec, fov)
        assert str(excinfo.value) == "Field of view must be a positive value."

    def test_get_coverage_range_negative_fov(self):
        """Tests the function with negative field of view."""
        ra = 120.5
        dec = 30.2
        fov = -10.0

        with pytest.raises(ValueError) as excinfo:
            get_coverage_range(ra, dec, fov)
        assert str(excinfo.value) == "Field of view must be a positive value."

    def test_get_coverage_range_ra_out_of_bounds_low(self):
        """Tests the function with RA less than zero."""
        ra = -10.0
        dec = 30.2
        fov = 10.0

        with pytest.raises(ValueError) as excinfo:
            get_coverage_range(ra, dec, fov)
        assert str(excinfo.value) == "Right Ascension (RA) must be between 0 and 360 degrees."

    def test_get_coverage_range_ra_out_of_bounds_high(self):
        """Tests the function with RA greater than 360."""
        ra = 370.0
        dec = 30.2
        fov = 10.0

        with pytest.raises(ValueError) as excinfo:
            get_coverage_range(ra, dec, fov)
        assert str(excinfo.value) == "Right Ascension (RA) must be between 0 and 360 degrees."

    def test_get_coverage_range_dec_out_of_bounds_low(self):
        """Tests the function with Declination less than -90."""
        ra = 120.5
        dec = -100.0
        fov = 10.0

        with pytest.raises(ValueError) as excinfo:
            get_coverage_range(ra, dec, fov)
        assert str(excinfo.value) == "Declination (Dec) must be between -90 and 90 degrees."

    def test_get_coverage_range_dec_out_of_bounds_high(self):
        """Tests the function with Declination greater than 90."""
        ra = 120.5
        dec = 100.0
        fov = 10.0

        with pytest.raises(ValueError) as excinfo:
            get_coverage_range(ra, dec, fov)
        assert str(excinfo.value) == "Declination (Dec) must be between -90 and 90 degrees."


class TestGetSkyComponentsByCriteria:  # pylint: disable=too-few-public-methods
    """Tests for the get_sky_components_by_criteria function"""

    def test_get_sky_components_by_criteria_no_filters(
        self, test_db
    ):  # pylint: disable=redefined-outer-name
        """Test getting components without filters."""
        result = get_sky_components_by_criteria(test_db)
        # Should return empty list or all components
        assert isinstance(result, list)
