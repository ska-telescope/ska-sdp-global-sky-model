# pylint: disable=no-member
"""
Basic testing of the API
"""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import JSON, create_engine, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from ska_sdp_global_sky_model.api.app.main import Base, app, get_db

# Use in-memory SQLite for testing
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,  # Use StaticPool to keep a single connection
)


# Register Q3C mock function for all SQLite connections
@event.listens_for(engine, "connect")
def register_q3c_mock(dbapi_conn, connection_record):  # pylint: disable=unused-argument
    """Register a mock Q3C function for SQLite."""

    def q3c_radial_query_mock(ra1, dec1, ra2, dec2, radius):
        """Mock Q3C function that does a simple box check instead of proper spherical distance."""
        # Simple box check - not accurate but sufficient for testing
        ra_diff = abs(ra1 - ra2)
        dec_diff = abs(dec1 - dec2)
        # Treat radius as degrees and check if point is within box
        return 1 if (ra_diff <= radius and dec_diff <= radius) else 0

    dbapi_conn.create_function("q3c_radial_query", 5, q3c_radial_query_mock)


# Make JSONB compatible with SQLite for tests
# pylint: disable=duplicate-code
@event.listens_for(Base.metadata, "before_create")
def replace_jsonb_sqlite(target, connection, **kw):  # pylint: disable=unused-argument
    """Replace JSONB with JSON and remove schema for SQLite."""
    if connection.dialect.name == "sqlite":
        for table in target.tables.values():
            table.schema = None
            for column in table.columns:
                if isinstance(column.type, JSONB):
                    column.type = JSON()


TESTING_SESSION_LOCAL = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    """
    Create a local testing session.
    """
    try:
        db = TESTING_SESSION_LOCAL()
        yield db
    finally:
        db.close()


@pytest.fixture()
def test_db():
    """
    Database for test purposes.
    """
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


app.dependency_overrides[get_db] = override_get_db


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


def test_read_main(myclient):
    """Unit test for the root path "/" """
    response = myclient.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "live"}


def test_upload_rcal(myclient):
    """Unit test for the /upload_rcal path"""
    file_path = "tests/data/rcal.csv"
    # Open the file in binary mode
    with open(file_path, "rb") as file:
        # Create a dictionary with the file
        files = {"file": ("rcal.csv", file, "text/csv")}

        # Send a POST request to the FastAPI endpoint
        response = myclient.post("/upload-rcal", files=files)

    assert response.status_code == 200
    assert response.json() == {"message": "RCAL uploaded and ingested successfully"}


def test_upload_sky_survey_batch(myclient):
    """Unit test for the /upload-sky-survey-batch path"""
    file_paths = [
        "tests/data/survey1.csv",
        "tests/data/survey2.csv",
    ]

    files = []
    for path in file_paths:
        files.append(("files", (path.split("/")[-1], open(path, "rb"), "text/csv")))

    response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 200

    data = response.json()
    assert "upload_id" in data
    assert data["status"] == "completed"


def test_sources(myclient):
    """Unit test for the /local_sky_model path"""
    file_path = "tests/data/rcal.csv"
    # Open the file in binary mode
    with open(file_path, "rb") as file:
        # Create a dictionary with the file
        files = {"file": file}

        # Send a POST request to the FastAPI endpoint
        myclient.post("/upload-rcal/", files=files)
    response = myclient.get("/sources")
    assert response.status_code == 200
    assert response.json()[0][0] == "J235613-743047"


def test_local_sky_model(myclient):
    """Unit test for the /local_sky_model path"""
    file_path = "tests/data/rcal.csv"
    # Open the file in binary mode
    with open(file_path, "rb") as file:
        # Create a dictionary with the file
        files = {"file": file}

        # Send a POST request to the FastAPI endpoint
        myclient.post("/upload-rcal/", files=files)

    local_sky_model = myclient.get(
        "/local_sky_model/",
        params={"ra": 359, "dec": -74, "telescope": "MWA", "flux_wide": 0, "fov": 1},
    )

    assert local_sky_model.status_code == 200
    assert len(local_sky_model.json()) >= 1
