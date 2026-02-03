# pylint: disable=no-member
"""
Basic testing of the API
"""

from pathlib import Path
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


def test_upload_sky_survey_batch(myclient, monkeypatch):
    """Unit test for the /upload-sky-survey-batch path"""
    first_file = Path("tests/data/survey1.csv")
    second_file = Path("tests/data/survey2.csv")

    # Patch DEFAULT_CATALOG_CONFIG
    test_config = {
        "ingest": {
            "wideband": True,
            "agent": "file",
            "file_location": [
                {
                    "key": "unset",
                    "heading_alias": {},
                    "heading_missing": [],
                    "bands": [],
                }
            ],
        },
        "name": "Test Sky Survey",
        "catalog_name": "TEST_SURVEY",
        "frequency_min": 80,
        "frequency_max": 300,
        "source": "GLEAM",  # Column name for source identifier in test CSV
        "bands": [],
    }

    # Patch the config in main module where it's imported
    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.DEFAULT_CATALOG_CONFIG", test_config
    )

    # Mock the ingest function to always return True (testing upload mechanism, not ingestion)
    def mock_ingest(db, config):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr("ska_sdp_global_sky_model.api.app.main.ingest", mock_ingest)

    with first_file.open("rb") as f1, second_file.open("rb") as f2:
        files = [
            ("files", (first_file.name, f1, "text/csv")),
            ("files", (second_file.name, f2, "text/csv")),
        ]

        response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "completed"
    assert "upload_id" in response_data

    # Test status endpoint
    upload_id = response_data["upload_id"]
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["state"] == "completed"
    assert status_data["total_files"] == 2
    assert status_data["uploaded_files"] == 2
    assert status_data["remaining_files"] == 0


def test_upload_sky_survey_batch_invalid_file_type(myclient, monkeypatch):
    """Test batch upload with invalid file type"""
    # Patch DEFAULT_CATALOG_CONFIG
    test_config = {
        "ingest": {
            "wideband": True,
            "agent": "file",
            "file_location": [
                {"key": "unset", "heading_alias": {}, "heading_missing": [], "bands": []}
            ],
        },
        "name": "Test",
        "catalog_name": "TEST",
        "frequency_min": 80,
        "frequency_max": 300,
        "source": "GLEAM",
        "bands": [],
    }

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.DEFAULT_CATALOG_CONFIG", test_config
    )

    # Create a fake non-CSV file
    fake_file_content = b"This is not a CSV"

    files = [
        ("files", ("test.txt", fake_file_content, "text/plain")),
    ]

    response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 400
    assert "Invalid file type" in response.json()["detail"]


def test_upload_sky_survey_batch_no_files(myclient):
    """Test batch upload with no files"""
    response = myclient.post("/upload-sky-survey-batch", files=[])

    assert response.status_code == 422  # FastAPI validation error for empty list


def test_upload_sky_survey_status_not_found(myclient):
    """Test status endpoint with non-existent upload ID"""
    response = myclient.get("/upload-sky-survey-status/non-existent-id")

    assert response.status_code == 404
    assert "Upload ID not found" in response.json()["detail"]


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


def test_upload_batch_gleam_catalog(myclient, monkeypatch):
    """Unit test for batch upload with GLEAM catalog"""
    file_path = Path("tests/data/gleam.csv")

    # Mock the ingest function
    def mock_ingest(db, config):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr("ska_sdp_global_sky_model.api.app.main.ingest", mock_ingest)

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use catalog parameter to select GLEAM configuration
        response = myclient.post(
            "/upload-sky-survey-batch", files=files, data={"catalog": "GLEAM"}
        )

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "completed"
    assert "upload_id" in response_data

    # Verify status
    upload_id = response_data["upload_id"]
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["state"] == "completed"
    assert status_data["total_files"] == 1
    assert status_data["uploaded_files"] == 1


def test_upload_batch_racs_catalog(myclient, monkeypatch):
    """Unit test for batch upload with RACS catalog"""
    file_path = Path("tests/data/racs.csv")

    # Mock the ingest function
    def mock_ingest(db, config):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr("ska_sdp_global_sky_model.api.app.main.ingest", mock_ingest)

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use catalog parameter to select RACS configuration
        response = myclient.post("/upload-sky-survey-batch", files=files, data={"catalog": "RACS"})

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "completed"
    assert "upload_id" in response_data

    # Verify status
    upload_id = response_data["upload_id"]
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["state"] == "completed"
    assert status_data["total_files"] == 1
    assert status_data["uploaded_files"] == 1


def test_upload_batch_rcal_catalog(myclient, monkeypatch):
    """Unit test for batch upload with RCAL catalog"""
    file_path = Path("tests/data/rcal.csv")

    # Mock the ingest function
    def mock_ingest(db, config):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr("ska_sdp_global_sky_model.api.app.main.ingest", mock_ingest)

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use catalog parameter to select RCAL configuration
        response = myclient.post("/upload-sky-survey-batch", files=files, data={"catalog": "RCAL"})

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "completed"
    assert "upload_id" in response_data

    # Verify status
    upload_id = response_data["upload_id"]
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["state"] == "completed"
    assert status_data["total_files"] == 1
    assert status_data["uploaded_files"] == 1


def test_upload_batch_generic_catalog(myclient, monkeypatch):
    """Unit test for batch upload with GENERIC catalog"""
    file_path = Path("tests/data/generic.csv")

    # Mock the ingest function
    def mock_ingest(db, config):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr("ska_sdp_global_sky_model.api.app.main.ingest", mock_ingest)

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use catalog parameter to select GENERIC configuration
        response = myclient.post(
            "/upload-sky-survey-batch", files=files, data={"catalog": "GENERIC"}
        )

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "completed"
    assert "upload_id" in response_data

    # Verify status
    upload_id = response_data["upload_id"]
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["state"] == "completed"
    assert status_data["total_files"] == 1
    assert status_data["uploaded_files"] == 1


def test_upload_batch_mixed_catalogs(myclient, monkeypatch):
    """Unit test for batch upload with multiple catalog types"""
    gleam_file = Path("tests/data/gleam.csv")
    racs_file = Path("tests/data/racs.csv")
    generic_file = Path("tests/data/generic.csv")

    # Mock the ingest function
    def mock_ingest(db, config):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr("ska_sdp_global_sky_model.api.app.main.ingest", mock_ingest)

    # Test uploading multiple files with GLEAM catalog
    with gleam_file.open("rb") as f1, racs_file.open("rb") as f2, generic_file.open("rb") as f3:
        files = [
            ("files", (gleam_file.name, f1, "text/csv")),
            ("files", (racs_file.name, f2, "text/csv")),
            ("files", (generic_file.name, f3, "text/csv")),
        ]
        # All files will use GLEAM catalog config
        response = myclient.post(
            "/upload-sky-survey-batch", files=files, data={"catalog": "GLEAM"}
        )

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "completed"
    assert "upload_id" in response_data

    # Verify status
    upload_id = response_data["upload_id"]
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["state"] == "completed"
    assert status_data["total_files"] == 3
    assert status_data["uploaded_files"] == 3


def test_upload_batch_invalid_catalog_name(myclient):
    """Unit test for batch upload with invalid catalog name"""
    file_path = Path("tests/data/generic.csv")

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use invalid catalog name - should fall back to GENERIC
        response = myclient.post(
            "/upload-sky-survey-batch", files=files, data={"catalog": "INVALID"}
        )

    # Should succeed by falling back to GENERIC catalog
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "completed"


def test_upload_batch_default_catalog(myclient, monkeypatch):
    """Unit test for batch upload with default catalog (no catalog specified)"""
    file_path = Path("tests/data/generic.csv")

    # Mock the ingest function
    def mock_ingest(db, config):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr("ska_sdp_global_sky_model.api.app.main.ingest", mock_ingest)

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # No catalog parameter - should default to GENERIC
        response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "completed"
    assert "upload_id" in response_data
