# pylint: disable=no-member, stop-iteration-return
"""
Basic testing of the API
"""

import csv
import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import JSON, create_engine, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from ska_sdp_global_sky_model.api.app.main import app, get_db, upload_manager, wait_for_db
from ska_sdp_global_sky_model.api.app.models import SkyComponent, SkyComponentStaging
from ska_sdp_global_sky_model.api.app.upload_manager import UploadStatus
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


def _clean_staging_table():
    """Clean staging table for test isolation."""
    db = next(override_get_db())
    try:
        db.query(SkyComponentStaging).delete()
        db.commit()
    finally:
        db.close()


def _clean_all_tables():
    """Clean both staging and main tables for test isolation."""
    db = next(override_get_db())
    try:
        db.query(SkyComponentStaging).delete()
        db.query(SkyComponent).delete()
        db.commit()
    finally:
        db.close()


def _mock_ingest_catalogue(db, metadata):  # pylint: disable=unused-argument
    """Simple mock that returns True without doing anything."""
    return True


def _fake_ingest_catalogue(db, metadata):
    """Fake ingest that fails when `ra` or `dec` are missing."""
    content = metadata["ingest"]["file_location"][0]["content"]
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        if not row.get("ra") or not row.get("dec"):
            return False
    return True


def _make_bad_csv(file_path: Path, n_missing: int = 2) -> bytes:
    """Create a bad CSV bytes object by removing `ra` and `dec` from first n rows."""
    with file_path.open("r", newline="", encoding="utf-8") as f:
        reader = list(csv.reader(f))
    if not reader:
        return b""
    header = reader[0]
    rows = reader[1:]

    for i in range(min(n_missing, len(rows))):
        if "ra" in header:
            ra_idx = header.index("ra")
            if ra_idx < len(rows[i]):
                rows[i][ra_idx] = ""
        if "dec" in header:
            dec_idx = header.index("dec")
            if dec_idx < len(rows[i]):
                rows[i][dec_idx] = ""

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def test_read_main(myclient):
    """Unit test for the root path "/" """
    response = myclient.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "live"}


def test_upload_rcal(myclient, monkeypatch):
    """Unit test for batch upload with test catalogue"""
    file_path = Path("tests/data/test_catalogue_1.csv")

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalogue", _mock_ingest_catalogue
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
    first_file = Path("tests/data/test_catalogue_1.csv")
    second_file = Path("tests/data/test_catalogue_2.csv")

    # Patch STANDARD_CATALOGUE_METADATA
    test_metadata = {
        "version": "1.0.0",
        "description": "Test metadata",
        "name": "Test Sky Survey",
        "catalogue_name": "TEST_SURVEY",
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
        "ska_sdp_global_sky_model.api.app.main.STANDARD_CATALOGUE_METADATA", test_metadata
    )

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalogue", _mock_ingest_catalogue
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
    # Patch STANDARD_CATALOGUE_METADATA
    test_metadata = {
        "version": "1.0.0",
        "description": "Test metadata",
        "name": "Test",
        "catalogue_name": "TEST",
        "ingest": {
            "file_location": [{"content": None}],
        },
    }

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.STANDARD_CATALOGUE_METADATA", test_metadata
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
            version="0.1.0",
        )
        db.add(component)
        db.commit()
    finally:
        db.close()

    response = myclient.get("/components")
    assert response.status_code == 200
    # Verify we have components
    assert "J030853+053903" in response.text


def test_local_sky_model(myclient):  # pylint: disable=unused-argument
    """Unit test for the /local_sky_model path"""

    # Add a component directly to the test database
    db = next(override_get_db())
    try:
        # Add a component in the query region (RA ~45, Dec ~4)
        component = SkyComponent(
            component_id="J030420+022029", healpix_index=12345, ra=45, dec=4, version="1.0.2"
        )
        db.add(component)
        db.commit()
    finally:
        db.close()

    # Query in the region covered by test data (RA ~42-50, Dec ~0-7)
    local_sky_model = myclient.get(
        "/local_sky_model/",
        params={"ra": "45", "dec": "4", "fov": 5, "version": "0.1.0"},
    )

    assert local_sky_model.status_code == 200
    assert "J030420+022029" in local_sky_model.text


def test_upload_batch_gleam_catalogue(myclient, monkeypatch):
    """Unit test for batch upload with GLEAM catalogue"""
    file_path = Path("tests/data/test_catalogue_1.csv")

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalogue", _mock_ingest_catalogue
    )

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use catalogue parameter to select GLEAM metadata
        response = myclient.post(
            "/upload-sky-survey-batch", files=files, data={"catalogue": "GLEAM"}
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


def test_upload_batch_racs_catalogue(myclient, monkeypatch):
    """Unit test for batch upload with RACS catalogue"""
    file_path = Path("tests/data/test_catalogue_1.csv")

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalogue", _mock_ingest_catalogue
    )

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use catalogue parameter to select RACS metadata
        response = myclient.post(
            "/upload-sky-survey-batch", files=files, data={"catalogue": "RACS"}
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


def test_upload_batch_rcal_catalogue(myclient, monkeypatch):
    """Unit test for batch upload with RCAL catalogue"""
    file_path = Path("tests/data/test_catalogue_1.csv")

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalogue", _mock_ingest_catalogue
    )

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use catalogue parameter to select RCAL metadata
        response = myclient.post(
            "/upload-sky-survey-batch", files=files, data={"catalogue": "RCAL"}
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


def test_upload_batch_generic_catalogue(myclient, monkeypatch):
    """Unit test for batch upload with GENERIC catalogue"""
    file_path = Path("tests/data/test_catalogue_1.csv")

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalogue", _mock_ingest_catalogue
    )

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Use catalogue parameter to select GENERIC metadata
        response = myclient.post(
            "/upload-sky-survey-batch", files=files, data={"catalogue": "GENERIC"}
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


def test_upload_batch_mixed_catalogues(myclient, monkeypatch):
    """Unit test for batch upload with multiple files"""
    first_file = Path("tests/data/test_catalogue_1.csv")
    second_file = Path("tests/data/test_catalogue_2.csv")
    third_file = Path("tests/data/test_catalogue_1.csv")

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalogue", _mock_ingest_catalogue
    )

    # Test uploading multiple files with GLEAM catalogue
    with first_file.open("rb") as f1, second_file.open("rb") as f2, third_file.open("rb") as f3:
        files = [
            ("files", (first_file.name, f1, "text/csv")),
            ("files", (second_file.name, f2, "text/csv")),
            ("files", (third_file.name, f3, "text/csv")),
        ]
        # All files will use GLEAM catalogue metadata
        response = myclient.post(
            "/upload-sky-survey-batch", files=files, data={"catalogue": "GLEAM"}
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


def test_upload_batch_default_catalogue(myclient, monkeypatch):
    """Unit test for batch upload with standard catalogue metadata"""
    file_path = Path("tests/data/test_catalogue_1.csv")

    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalogue", _mock_ingest_catalogue
    )

    with file_path.open("rb") as f:
        files = [("files", (file_path.name, f, "text/csv"))]
        # Uses standard catalogue metadata automatically
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
    """Test batch upload when catalogue ingest fails."""
    file_path = Path("tests/data/test_catalogue_1.csv")

    with (
        file_path.open("rb") as f,
        patch("ska_sdp_global_sky_model.api.app.main.ingest_catalogue", return_value=False),
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
    file_path = Path("tests/data/test_catalogue_1.csv")

    with (
        file_path.open("rb") as f,
        patch("ska_sdp_global_sky_model.api.app.main.ingest_catalogue", return_value=True),
    ):
        # Use .txt extension but valid CSV content
        files = [("files", ("data.txt", f, "text/plain"))]
        response = myclient.post("/upload-sky-survey-batch", files=files)

        # Should succeed because validation is content-based, not extension-based
        assert response.status_code == 200
        assert "upload_id" in response.json()


def test_review_upload_success(myclient):
    """Test successful review of staged upload."""
    _clean_staging_table()

    # Create a fake upload ID
    upload_id = "test-upload-review-123"

    # Directly insert test data into staging table
    db = next(override_get_db())
    try:
        for i in range(15):
            component = SkyComponentStaging(
                component_id=f"TEST{i:05d}",
                upload_id=upload_id,
                ra=10.0 + i,
                dec=20.0 + i,
                i_pol=0.5 + i * 0.1,
                healpix_index=12345,
            )
            db.add(component)
        db.commit()
    finally:
        db.close()

    # Create upload status
    upload_status = UploadStatus(upload_id=upload_id, total=1)
    upload_status.state = "completed"
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    # Review the upload
    review_response = myclient.get(f"/review-upload/{upload_id}")
    assert review_response.status_code == 200
    review_data = review_response.json()
    assert review_data["upload_id"] == upload_id
    assert review_data["total_records"] == 15
    assert review_data["sample_range"] == "6-15"

    # Last 10 records
    assert len(review_data["sample"]) == 10
    assert "TEST" in review_data["sample"][0]["component_id"]


def test_review_upload_not_completed(myclient):
    """Test review of upload that hasn't completed yet."""
    _clean_staging_table()

    # Create upload in non-completed state
    upload_id = "test-upload-not-completed-123"
    upload_status = UploadStatus(upload_id=upload_id, total=1)
    upload_status.state = "uploading"  # Not completed
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    # Review before completion
    review_response = myclient.get(f"/review-upload/{upload_id}")
    assert review_response.status_code == 400
    assert "not ready for review" in review_response.json()["detail"].lower()


def test_commit_upload_success(myclient):
    """Test successful commit of staged upload with versioning."""
    _clean_all_tables()

    # Create test data directly in staging table
    upload_id = "test-upload-commit-123"
    db = next(override_get_db())
    try:
        for i in range(5):
            component = SkyComponentStaging(
                component_id=f"COMMIT_TEST{i:05d}",
                upload_id=upload_id,
                ra=10.0 + i,
                dec=20.0 + i,
                i_pol=0.5,
                healpix_index=12345,
            )
            db.add(component)
        db.commit()
    finally:
        db.close()

    # Create upload status
    upload_status = UploadStatus(upload_id=upload_id, total=1)
    upload_status.state = "completed"
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    # Commit the upload
    commit_response = myclient.post(f"/commit-upload/{upload_id}")
    assert commit_response.status_code == 200
    commit_data = commit_response.json()
    assert commit_data["status"] == "success"
    assert commit_data["records_committed"] == 5

    # Verify data moved to main table with version
    db = next(override_get_db())
    try:
        main_records = (
            db.query(SkyComponent).filter(SkyComponent.component_id.like("COMMIT_TEST%")).all()
        )
        assert len(main_records) == 5

        # Check all have same version
        versions = {r.version for r in main_records}
        assert len(versions) == 1
        assert list(versions)[0] == "0.1.0"

        # Verify staging table is cleared
        staging_records = (
            db.query(SkyComponentStaging).filter(SkyComponentStaging.upload_id == upload_id).all()
        )
        assert len(staging_records) == 0
    finally:
        db.close()


def test_commit_upload_increments_version(myclient):
    """Test that second commit increments version to 0.2.0."""
    _clean_all_tables()

    # Add existing data at version 0.1.0
    db = next(override_get_db())
    try:
        for i in range(3):
            component = SkyComponent(
                component_id=f"EXISTING{i:05d}",
                ra=5.0 + i,
                dec=15.0 + i,
                i_pol=0.3,
                healpix_index=11111,
                version="0.1.0",
            )
            db.add(component)
        db.commit()
    finally:
        db.close()

    # Create new staging data
    upload_id = "test-upload-increment-123"
    db = next(override_get_db())
    try:
        for i in range(5):
            component = SkyComponentStaging(
                component_id=f"NEW{i:05d}",
                upload_id=upload_id,
                ra=10.0 + i,
                dec=20.0 + i,
                i_pol=0.5,
                healpix_index=12345,
            )
            db.add(component)
        db.commit()
    finally:
        db.close()

    # Create upload status
    upload_status = UploadStatus(upload_id=upload_id, total=1)
    upload_status.state = "completed"
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    commit_response = myclient.post(f"/commit-upload/{upload_id}")

    assert commit_response.status_code == 200

    # Verify new records have version 0.2.0
    db = next(override_get_db())
    try:
        new_records = db.query(SkyComponent).filter(SkyComponent.component_id.like("NEW%")).all()
        assert len(new_records) == 5
        for record in new_records:
            assert record.version == "0.2.0"  # Incremented from 0.1.0
    finally:
        db.close()


def test_commit_upload_not_completed(myclient):
    """Test commit fails if upload not completed."""
    _clean_staging_table()

    # Create upload in non-completed state
    upload_id = "test-upload-commit-not-done-123"
    upload_status = UploadStatus(upload_id=upload_id, total=1)
    upload_status.state = "uploading"  # Not completed
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    commit_response = myclient.post(f"/commit-upload/{upload_id}")
    assert commit_response.status_code == 400
    assert "not ready for commit" in commit_response.json()["detail"].lower()


def test_reject_upload_success(myclient):
    """Test successful rejection of staged upload."""
    _clean_staging_table()

    # Directly insert staging records
    upload_id = "test-upload-reject-123"
    db = next(override_get_db())
    try:
        for i in range(5):
            component = SkyComponentStaging(
                component_id=f"REJECT_TEST{i:03d}",
                upload_id=upload_id,
                ra=10.0 + i,
                dec=20.0 + i,
                i_pol=0.5,
                healpix_index=12345,
            )
            db.add(component)
        db.commit()
    finally:
        db.close()

    # Create completed upload status
    upload_status = UploadStatus(upload_id=upload_id, total=1)
    upload_status.state = "completed"
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    # Reject the upload
    reject_response = myclient.delete(f"/reject-upload/{upload_id}")
    assert reject_response.status_code == 200
    reject_data = reject_response.json()
    assert reject_data["status"] == "success"
    assert reject_data["records_deleted"] == 5

    # Verify staging table is cleared
    db = next(override_get_db())
    try:
        staging_records = (
            db.query(SkyComponentStaging).filter(SkyComponentStaging.upload_id == upload_id).all()
        )
        assert len(staging_records) == 0
    finally:
        db.close()


def test_reject_upload_not_completed(myclient):
    """Test reject fails if upload not completed."""
    _clean_staging_table()

    # Create incomplete upload status
    upload_id = "test-upload-reject-incomplete-456"
    upload_status = UploadStatus(upload_id=upload_id, total=1)
    upload_status.state = "uploading"
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    # Reject before completion
    reject_response = myclient.delete(f"/reject-upload/{upload_id}")

    assert reject_response.status_code == 400
    assert "not ready for rejection" in reject_response.json()["detail"].lower()


def test_upload_batch_partial_fail_clears_staging(myclient, monkeypatch):
    """Test that if one good and one bad file are uploaded, staging is cleared on failure."""
    _clean_staging_table()
    good_file = Path("tests/data/test_catalogue_1.csv")

    # Create bad CSV bytes from the good file (removes ra/dec in first rows)
    bad_csv_bytes = _make_bad_csv(good_file, n_missing=2)

    # Fake to simulate failure when ra/dec missing
    monkeypatch.setattr(
        "ska_sdp_global_sky_model.api.app.main.ingest_catalogue", _fake_ingest_catalogue
    )

    with good_file.open("rb") as f1:
        files = [
            ("files", (good_file.name, f1, "text/csv")),
            ("files", ("bad.csv", bad_csv_bytes, "text/csv")),
        ]
        response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 200
    upload_id = response.json()["upload_id"]

    # Status should be failed
    status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["state"] == "failed"

    # Staging table should be empty for this upload_id
    db = next(override_get_db())
    try:
        count = (
            db.query(SkyComponentStaging)
            .filter(SkyComponentStaging.upload_id == upload_id)
            .count()
        )
        assert count == 0
    finally:
        db.close()
