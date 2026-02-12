# pylint: disable=no-member
"""
Basic testing of the API
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import JSON, create_engine, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from ska_sdp_global_sky_model.api.app.main import app, get_db, wait_for_db
from ska_sdp_global_sky_model.api.app.models import SkyComponent
from ska_sdp_global_sky_model.configuration.config import Base

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


def test_upload_rcal(myclient, monkeypatch):
    """Unit test for batch upload with test catalog"""
    file_path = Path("tests/data/test_catalog_1.csv")

    # Mock the ingest function
    def mock_ingest_catalog(db, metadata):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalog", mock_ingest_catalog
    )

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "uploading"
    assert "upload_id" in response_data


def test_upload_sky_survey_batch(myclient, monkeypatch):
    """Unit test for the /upload-sky-survey-batch path"""
    first_file = Path("tests/data/test_catalog_1.csv")
    second_file = Path("tests/data/test_catalog_2.csv")

    # Patch STANDARD_CATALOG_METADATA
    test_metadata = {
        "version": "1.0.0",
        "description": "Test metadata",
        "name": "Test Sky Survey",
        "catalog_name": "TEST_SURVEY",
        "ingest": {
            "file_location": [
                {
                    "content": None,
                }
            ],
        },
    }

    # Patch the metadata in main module where it's imported
    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.STANDARD_CATALOG_METADATA", test_metadata
    )

    # Mock the ingest_catalog function to always return True
    def mock_ingest_catalog(db, metadata):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalog", mock_ingest_catalog
    )

    with first_file.open("rb") as f1, second_file.open("rb") as f2:
        files = [
            ("files", (first_file.name, f1, "text/csv")),
            ("files", (second_file.name, f2, "text/csv")),
        ]

        response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 200
    response_data = response.json()
    # With background tasks, status will be "uploading" not "completed"
    assert response_data["status"] == "uploading"
    assert "upload_id" in response_data

    # Test status endpoint - note: in test environment background task runs synchronously
    upload_id = response_data["upload_id"]
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    # Check that upload was tracked
    assert status_data["total_files"] == 2


def test_upload_sky_survey_batch_invalid_file_type(myclient, monkeypatch):
    """Test batch upload with invalid file type"""
    # Patch STANDARD_CATALOG_METADATA
    test_metadata = {
        "version": "1.0.0",
        "description": "Test metadata",
        "name": "Test",
        "catalog_name": "TEST",
        "ingest": {
            "file_location": [{"content": None}],
        },
    }

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.STANDARD_CATALOG_METADATA", test_metadata
    )

    # Create a fake non-CSV file
    fake_file_content = b"This is not a CSV"

    files = [
        ("files", ("test.txt", fake_file_content, "text/plain")),
    ]

    response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 400
    # Now validates actual content structure rather than file extension
    assert "data rows" in response.json()["detail"] or "not valid CSV" in response.json()["detail"]


def test_upload_sky_survey_batch_no_files(myclient):
    """Test batch upload with no files"""
    response = myclient.post("/upload-sky-survey-batch", files=[])

    assert response.status_code == 422  # FastAPI validation error for empty list


def test_upload_sky_survey_status_not_found(myclient):
    """Test status endpoint with non-existent upload ID"""
    response = myclient.get("/upload-sky-survey-status/non-existent-id")

    assert response.status_code == 404
    assert "Upload ID not found" in response.json()["detail"]


def test_components(myclient):  # pylint: disable=unused-argument,redefined-outer-name
    """Unit test for the /components endpoint"""

    # Add a test component directly to the test database using override_get_db
    # Use the overridden database session
    db = next(override_get_db())
    try:
        component = SkyComponent(
            component_id="J030853+053903",
            healpix_index=12345,
            ra=47.222569,
            dec=5.650958,
            i_pol=0.098383,
        )
        db.add(component)
        db.commit()
    finally:
        db.close()

    response = myclient.get("/components")
    assert response.status_code == 200
    # Verify we have components
    assert len(response.json()) > 0
    assert response.json()[0][0].startswith("J")


def test_local_sky_model(myclient):  # pylint: disable=unused-argument
    """Unit test for the /local_sky_model path"""

    # Add a component directly to the test database
    db = next(override_get_db())
    try:
        # Add a component in the query region (RA ~45, Dec ~4)
        component = SkyComponent(
            component_id="J030420+022029",
            healpix_index=12345,
            ra=46.084633,
            dec=2.341634,
            i_pol=0.29086,
        )
        db.add(component)
        db.commit()
    finally:
        db.close()

    # Query in the region covered by test data (RA ~42-50, Dec ~0-7)
    local_sky_model = myclient.get(
        "/local_sky_model/",
        params={"ra": "45", "dec": "4", "telescope": "MWA", "flux_wide": 0, "fov": 5},
    )

    assert local_sky_model.status_code == 200
    assert len(local_sky_model.json()) >= 1


def test_upload_batch_gleam_catalog(myclient, monkeypatch):
    """Unit test for batch upload with GLEAM catalog"""
    file_path = Path("tests/data/test_catalog_1.csv")

    # Mock the ingest function
    def mock_ingest_catalog(db, metadata):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalog", mock_ingest_catalog
    )

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use catalog parameter to select GLEAM metadata
        response = myclient.post(
            "/upload-sky-survey-batch", files=files, data={"catalog": "GLEAM"}
        )

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "uploading"
    assert "upload_id" in response_data

    # Verify status
    upload_id = response_data["upload_id"]
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["state"] == "completed"
    assert status_data["total_files"] == 1


def test_upload_batch_racs_catalog(myclient, monkeypatch):
    """Unit test for batch upload with RACS catalog"""
    file_path = Path("tests/data/test_catalog_1.csv")

    # Mock the ingest function
    def mock_ingest_catalog(db, metadata):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalog", mock_ingest_catalog
    )

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use catalog parameter to select RACS metadata
        response = myclient.post("/upload-sky-survey-batch", files=files, data={"catalog": "RACS"})

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "uploading"
    assert "upload_id" in response_data

    # Verify status
    upload_id = response_data["upload_id"]
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["state"] == "completed"
    assert status_data["total_files"] == 1


def test_upload_batch_rcal_catalog(myclient, monkeypatch):
    """Unit test for batch upload with RCAL catalog"""
    file_path = Path("tests/data/test_catalog_1.csv")

    # Mock the ingest function
    def mock_ingest_catalog(db, metadata):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalog", mock_ingest_catalog
    )

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use catalog parameter to select RCAL metadata
        response = myclient.post("/upload-sky-survey-batch", files=files, data={"catalog": "RCAL"})

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "uploading"
    assert "upload_id" in response_data

    # Verify status
    upload_id = response_data["upload_id"]
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["state"] == "completed"
    assert status_data["total_files"] == 1


def test_upload_batch_generic_catalog(myclient, monkeypatch):
    """Unit test for batch upload with GENERIC catalog"""
    file_path = Path("tests/data/test_catalog_1.csv")

    # Mock the ingest function
    def mock_ingest_catalog(db, metadata):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalog", mock_ingest_catalog
    )

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use catalog parameter to select GENERIC metadata
        response = myclient.post(
            "/upload-sky-survey-batch", files=files, data={"catalog": "GENERIC"}
        )

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "uploading"
    assert "upload_id" in response_data

    # Verify status
    upload_id = response_data["upload_id"]
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["state"] == "completed"
    assert status_data["total_files"] == 1


def test_upload_batch_mixed_catalogs(myclient, monkeypatch):
    """Unit test for batch upload with multiple files"""
    first_file = Path("tests/data/test_catalog_1.csv")
    second_file = Path("tests/data/test_catalog_2.csv")
    third_file = Path("tests/data/test_catalog_1.csv")

    # Mock the ingest function
    def mock_ingest_catalog(db, metadata):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalog", mock_ingest_catalog
    )

    # Test uploading multiple files with GLEAM catalog
    with first_file.open("rb") as f1, second_file.open("rb") as f2, third_file.open("rb") as f3:
        files = [
            ("files", (first_file.name, f1, "text/csv")),
            ("files", (second_file.name, f2, "text/csv")),
            ("files", (third_file.name, f3, "text/csv")),
        ]
        # All files will use GLEAM catalog metadata
        response = myclient.post(
            "/upload-sky-survey-batch", files=files, data={"catalog": "GLEAM"}
        )

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "uploading"
    assert "upload_id" in response_data

    # Verify status
    upload_id = response_data["upload_id"]
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["state"] == "completed"
    assert status_data["total_files"] == 3


def test_upload_batch_default_catalog(myclient, monkeypatch):
    """Unit test for batch upload with standard catalog metadata"""
    file_path = Path("tests/data/test_catalog_1.csv")

    # Mock the ingest function
    def mock_ingest_catalog(db, metadata):  # pylint: disable=unused-argument
        return True

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalog", mock_ingest_catalog
    )

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Uses standard catalog metadata automatically
        response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["status"] == "uploading"
    assert "upload_id" in response_data


def test_wait_for_db_success():
    """Test wait_for_db succeeds on first try."""
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_connection

    with patch("ska_sdp_global_sky_model.api.app.main.engine", mock_engine):
        wait_for_db()

    # Verify connection was attempted
    mock_engine.connect.assert_called_once()


def test_wait_for_db_retry():
    """Test wait_for_db retries on failure then succeeds."""
    mock_engine = MagicMock()
    # Fail once, then succeed
    mock_engine.connect.side_effect = [
        Exception("Connection failed"),
        MagicMock(),
    ]

    with (
        patch("ska_sdp_global_sky_model.api.app.main.engine", mock_engine),
        patch("time.sleep") as mock_sleep,
    ):
        wait_for_db()

    # Verify retry occurred
    assert mock_engine.connect.call_count == 2
    mock_sleep.assert_called_once_with(5)


def test_upload_batch_ingest_failure(myclient):
    """Test batch upload when catalog ingest fails."""
    file_path = Path("tests/data/test_catalog_1.csv")

    with (
        file_path.open("rb") as f,
        patch("ska_sdp_global_sky_model.api.app.main.ingest_catalog", return_value=False),
    ):
        files = [("files", (file_path.name, f, "text/csv"))]
        response = myclient.post("/upload-sky-survey-batch", files=files)

        # With background tasks, upload starts successfully
        assert response.status_code == 200
        upload_id = response.json()["upload_id"]

        # Check status shows failure
        status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
        assert status_response.status_code == 200
        status_data = status_response.json()
        assert status_data["state"] == "failed"


def test_upload_sky_survey_batch_empty_file(myclient):
    """Test uploading an empty file."""
    empty_content = b""
    files = [("files", ("empty.csv", empty_content, "text/csv"))]
    response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 400
    assert "empty" in response.json()["detail"].lower()
    assert "empty.csv" in response.json()["detail"]


def test_upload_sky_survey_batch_header_only(myclient):
    """Test uploading a CSV with only header row."""
    header_only = b"name,ra,dec\n"
    files = [("files", ("header_only.csv", header_only, "text/csv"))]
    response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 400
    assert "no data rows" in response.json()["detail"].lower()
    assert "header_only.csv" in response.json()["detail"]


def test_upload_sky_survey_batch_empty_header(myclient):
    """Test uploading a CSV with empty header row."""
    empty_header = b",,\ndata1,data2,data3\n"
    files = [("files", ("empty_header.csv", empty_header, "text/csv"))]
    response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 400
    assert "empty header" in response.json()["detail"].lower()
    assert "empty_header.csv" in response.json()["detail"]


def test_upload_sky_survey_batch_invalid_utf8(myclient):
    """Test uploading a file with invalid UTF-8 encoding."""
    invalid_utf8 = b"\x80\x81\x82\x83"  # Invalid UTF-8 bytes
    files = [("files", ("binary.dat", invalid_utf8, "application/octet-stream"))]
    response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 400
    assert "not valid UTF-8" in response.json()["detail"]
    assert "binary.dat" in response.json()["detail"]


def test_upload_sky_survey_batch_malformed_csv(myclient):
    """Test uploading a file with malformed CSV structure."""
    malformed_csv = b'name,ra,dec\n"unclosed quote,10.5,45.2\n'
    files = [("files", ("malformed.csv", malformed_csv, "text/csv"))]
    response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 400
    assert "not valid CSV" in response.json()["detail"]
    assert "malformed.csv" in response.json()["detail"]


def test_upload_sky_survey_batch_valid_without_csv_extension(myclient):
    """Test uploading a valid CSV file without .csv extension."""
    file_path = Path("tests/data/test_catalog_1.csv")

    with (
        file_path.open("rb") as f,
        patch("ska_sdp_global_sky_model.api.app.main.ingest_catalog", return_value=True),
    ):
        # Use .txt extension but valid CSV content
        files = [("files", ("data.txt", f, "text/plain"))]
        response = myclient.post("/upload-sky-survey-batch", files=files)

        # Should succeed because validation is content-based, not extension-based
        assert response.status_code == 200
        assert "upload_id" in response.json()
