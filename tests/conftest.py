"""Conftest.py"""

import logging
import pathlib
from unittest.mock import patch

import pytest
from ska_sdp_config.entity import Flow
from ska_sdp_config.entity.common import PVCPath
from ska_sdp_config.entity.flow import DataProduct, FlowSource
from sqlalchemy.orm import sessionmaker
from starlette.testclient import TestClient

from ska_sdp_global_sky_model.api.app.main import app
from ska_sdp_global_sky_model.api.app.models import GlobalSkyModelMetadata
from ska_sdp_global_sky_model.api.app.request_responder import QueryParameters
from ska_sdp_global_sky_model.configuration.config import SHARED_VOLUME_MOUNT, Base, get_db
from tests.utils import engine, override_get_db

logger = logging.getLogger(__name__)

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="session", autouse=True)
def create_db():
    """Create a new database at the start of the testing session."""
    # pylint: disable-next=no-member
    Base.metadata.create_all(bind=engine)
    yield
    # pylint: disable-next=no-member
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="session", name="myclient")
def fixture_client():
    """Create test client connecting to the in-memory SQLite database."""
    # Mock the database connection check and startup functions
    app.dependency_overrides[get_db] = override_get_db
    with (
        patch("ska_sdp_global_sky_model.api.app.main.wait_for_db"),
        patch("ska_sdp_global_sky_model.api.app.main.start_lsm_response_thread"),
        patch("ska_sdp_global_sky_model.api.app.main.engine", engine),
    ):
        with TestClient(app) as client:
            yield client


@pytest.fixture(name="gsm_metadata")
def fake_gsm_metadata():
    """Create an example metadata record"""
    return GlobalSkyModelMetadata(
        version="1.0.0",
        catalogue_name="TEST",
        upload_id="test-upload-1",
    )


@pytest.fixture(name="valid_flow")
def fixture_valid_flow():
    """Fixture for a valid flow"""

    pb_id = "pb-test-20260108-1234"
    eb_id = "eb-test-20260108-1234"
    return Flow(
        key=Flow.Key(pb_id=pb_id, name="local-sky-model-field1"),
        sink=DataProduct(
            data_dir=PVCPath(
                k8s_namespaces=[],
                k8s_pvc_name="",
                pvc_mount_path=str(SHARED_VOLUME_MOUNT),
                pvc_subpath=pathlib.Path(f"product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/sky/field1"),
            ),
            paths=[],
        ),
        sources=[
            FlowSource(
                uri="gsm://request/lsm",
                function="GlobalSkyModel.RequestLocalSkyModel",
                parameters={
                    "ra_deg": 2.9670,
                    "dec_deg": -0.1745,
                    "fov_deg": 0.0873,
                    "catalogue_name": "catalogue",
                    "sub_path": "test/lsm.csv",
                },
            ),
        ],
        data_model="CsvNamedColumns",
        expiry_time=-1,
    )


@pytest.fixture(name="expected_query_parameters")
def expected_query_params_fixt(valid_flow):
    """Expected query parameters"""
    query_params = valid_flow.sources[0].parameters
    return QueryParameters(**query_params)
