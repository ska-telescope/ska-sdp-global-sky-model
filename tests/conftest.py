# pylint: disable=no-member

"""Conftest.py"""

from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from ska_sdp_global_sky_model.api.app.main import app
from ska_sdp_global_sky_model.api.app.models import SkyComponent
from ska_sdp_global_sky_model.configuration.config import Base
from tests.utils import engine, override_get_db


@pytest.fixture(scope="module", name="myclient")
def fixture_client():
    """Create test client with mocked database startup and Q3C function."""
    # Mock the database connection check and startup functions
    with (
        patch("ska_sdp_global_sky_model.api.app.main.wait_for_db"),
        patch("ska_sdp_global_sky_model.api.app.main.start_thread"),
        patch("ska_sdp_global_sky_model.api.app.main.engine", engine),
    ):

        # Create tables once for all tests
        Base.metadata.create_all(bind=engine)

        with TestClient(app) as client:
            yield client
            Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def test_db():
    """
    Database for test purposes.
    """
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(name="set_up_db")
def set_up_db_data_fxt():
    """
    Set up database with components
    """
    components = [
        SkyComponent(
            component_id="J030420+022029", healpix_index=12345, ra=90, dec=2, version="1.0.2"
        ),
        SkyComponent(
            component_id="J031020+042029", healpix_index=12340, ra=92, dec=4, version="1.1.0"
        ),
    ]

    # Add a component directly to the test database
    db = next(override_get_db())
    try:
        # Add a component in the query region (RA ~45, Dec ~4)
        for component in components:
            db.add(component)
            db.commit()
    finally:
        db.close()
