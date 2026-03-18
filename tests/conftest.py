# pylint: disable=no-member

"""Conftest.py"""

import hashlib
import logging
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from ska_sdp_global_sky_model.api.app.ingest import compute_hpx_healpy
from ska_sdp_global_sky_model.api.app.main import app
from ska_sdp_global_sky_model.api.app.models import GlobalSkyModelMetadata, SkyComponent
from ska_sdp_global_sky_model.configuration.config import Base
from tests.utils import clean_all_tables, engine, override_get_db

logger = logging.getLogger(__name__)


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


def _generate_catalogue(name: str, version: str, mid: tuple[float, float], count: int):
    upload_id = hashlib.sha1(f"{name}-{version}".encode("utf-8")).hexdigest()
    code = chr(65 + sum(ord(x) for x in upload_id) % 26)
    logger.info(
        "%s - %s - %s - %s - (%f, %f) - %d", name, version, upload_id, code, mid[0], mid[1], count
    )
    objs = []
    objs.append(
        GlobalSkyModelMetadata(
            version=version,
            catalogue_name=name,
            upload_id=upload_id,
        )
    )

    mr, md = mid[0] - count / 10, mid[1] - count / 10

    for i in range(count):
        r, d = mr + i / 5, md + i / 5
        objs.append(
            SkyComponent(
                component_id=f"{code}{version.replace('.',''):0>6}+{i:0>6}",
                healpix_index=compute_hpx_healpy(r, d),
                ra_deg=r,
                dec_deg=d,
                version=version,
                catalogue_name=name,
            )
        )
    return objs


@pytest.fixture(name="set_up_db")
def set_up_db_data_fxt():
    """
    Set up database with components
    """

    objs = _generate_catalogue("catalogue1", "0.1.0", (90, 2), 20)
    objs.extend(_generate_catalogue("catalogue1", "0.2.0", (90, 4), 10))
    objs.extend(_generate_catalogue("catalogue2", "1.0.0", (70, 4), 200))
    objs.extend(_generate_catalogue("catalogue3", "1.0.0", (80, 4), 20))

    # Add a component directly to the test database
    # pylint: disable-next=stop-iteration-return
    db = next(override_get_db())
    try:
        # Add a component in the query region (RA ~45, Dec ~4)
        for obj in objs:
            db.add(obj)
        db.commit()
    finally:
        db.close()

    yield

    clean_all_tables()
