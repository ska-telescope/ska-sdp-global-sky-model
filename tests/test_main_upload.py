"""
Basic testing of the upload functionality of the API.
"""

import csv
import io
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from ska_sdp_global_sky_model.api.app.main import upload_manager
from ska_sdp_global_sky_model.api.app.models import (
    GlobalSkyModelMetadata,
    SkyComponent,
    SkyComponentStaging,
)
from ska_sdp_global_sky_model.api.app.upload_manager import UploadStatus
from tests.utils import clean_all_tables


@pytest.fixture(scope="function", autouse=True)
def clean_up_database():
    """
    Clean tables after each test run.
    Specific to this module. Do not move.
    """
    yield
    clean_all_tables()


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


def _assert_upload_response(my_client, response, num_csv):
    """Assert response for upload tests."""

    assert response.status_code == 200
    response_data = response.json()
    # With background tasks, status will be "uploading" not "completed"
    assert response_data["status"] == "uploading"
    assert "upload_id" in response_data

    # Test status endpoint - note: in test environment background task runs synchronously
    upload_id = response_data["upload_id"]
    status_response = my_client.get(f"/upload-sky-survey-status/{upload_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    # Check that upload was tracked
    assert status_data["total_csv_files"] == num_csv


@patch("ska_sdp_global_sky_model.api.app.main.ingest_catalogue", Mock(return_value=True))
def test_upload_sky_survey_batch(myclient):
    """Unit test for the /upload-sky-survey-batch path"""
    first_file = Path("tests/data/test_catalogue_1.csv")
    second_file = Path("tests/data/test_catalogue_2.csv")
    metadata_file = Path("tests/data/metadata_test.json")

    with (
        metadata_file.open("rb") as metadata_f,
        first_file.open("rb") as f1,
        second_file.open("rb") as f2,
    ):
        files = [
            ("metadata_file", (metadata_file.name, metadata_f, "application/json")),
            ("csv_files", (first_file.name, f1, "text/csv")),
            ("csv_files", (second_file.name, f2, "text/csv")),
        ]

        response = myclient.post("/upload-sky-survey-batch", files=files)

    _assert_upload_response(myclient, response, 2)


@patch("ska_sdp_global_sky_model.api.app.main.ingest_catalogue", Mock(return_value=True))
@pytest.mark.parametrize(
    "metadata_file",
    [
        Path("tests/data/metadata_gleam.json"),
        Path("tests/data/metadata_rcal.json"),
        Path("tests/data/metadata_test.json"),
    ],
)
def test_upload_sky_survey_batch_metadata(metadata_file, myclient):
    """
    Test that metadata files can be uploaded even
    when not all information is present in the file.
    """

    first_file = Path("tests/data/test_catalogue_1.csv")

    with (
        metadata_file.open("rb") as metadata_f,
        first_file.open("rb") as f1,
    ):
        files = [
            ("metadata_file", (metadata_file.name, metadata_f, "application/json")),
            ("csv_files", (first_file.name, f1, "text/csv")),
        ]

        response = myclient.post("/upload-sky-survey-batch", files=files)

    _assert_upload_response(myclient, response, 1)


def test_upload_sky_survey_batch_invalid_file_type(myclient):
    """Test batch upload with invalid file type"""
    metadata_file = Path("tests/data/metadata_test.json")

    # Create a fake non-CSV file
    file_content = b"This is not a CSV"

    with metadata_file.open("rb") as metadata_f:
        files = [
            ("metadata_file", (metadata_file.name, metadata_f, "application/json")),
            ("csv_files", ("test.txt", file_content, "text/plain")),
        ]

        response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 400
    # Now validates actual content structure rather than file extension
    assert "has no data rows" in response.json()["detail"]


def test_upload_sky_survey_batch_no_files(myclient):
    """Test batch upload with no files"""
    response = myclient.post("/upload-sky-survey-batch", files=[])

    assert response.status_code == 422  # FastAPI validation error for empty list


def test_upload_sky_survey_status_not_found(myclient):
    """Test status endpoint with non-existent upload ID"""
    response = myclient.get("/upload-sky-survey-status/non-existent-id")

    assert response.status_code == 404
    assert "Upload ID not found" in response.json()["detail"]


def test_upload_batch_ingest_failure(myclient):
    """
    Test batch upload when catalogue ingest fails.

    Failure is caused by the ingest_catalogue function
    being patched to return False (i.e. ingest failed)
    """

    file_path = Path("tests/data/test_catalogue_1.csv")
    metadata_file = Path("tests/data/metadata_test.json")

    with (
        metadata_file.open("rb") as metadata_f,
        file_path.open("rb") as f,
        patch("ska_sdp_global_sky_model.api.app.main.ingest_catalogue", return_value=False),
    ):
        files = [
            ("metadata_file", (metadata_file.name, metadata_f, "application/json")),
            ("csv_files", (file_path.name, f, "text/csv")),
        ]
        response = myclient.post("/upload-sky-survey-batch", files=files)

        # With background tasks, upload starts successfully
        assert response.status_code == 200
        upload_id = response.json()["upload_id"]

        # Check status shows failure
        status_response = myclient.get(f"/upload-sky-survey-status/{upload_id}")
        assert status_response.status_code == 200
        status_data = status_response.json()
        assert status_data["state"] == "failed"


@pytest.mark.parametrize(
    "content, response_string",
    [
        (b"", "empty"),
        (b"name,ra,dec\n", "no data rows"),
        (b",,\ndata1,data2,data3\n", "empty header"),
        (b"\x80\x81\x82\x83", "not valid UTF-8"),
        (b'name,ra,dec\n"unclosed quote,10.5,45.2', "not valid CSV at line 2"),
        (b'name,ra,dec\n"unclosed quote,10.5,45.2\n', "not valid CSV at line 2"),
        (
            b'name,ra,dec\n"unclosed quote,10.5,45.2\n"another line",10.5,45.2\n',
            "not valid CSV at line 2",
        ),
        (b'name,ra,dec,i_pol_jy\n"s1",10.5,45.2\n', "inconsistent number of fields at line 2"),
        (
            b'name,ra,dec,i_pol_jy\n"s1",10.5,45.2,12.3\n"s2",10.5,45.2,23.4,34.5',
            "inconsistent number of fields at line 3",
        ),
    ],
)
def test_upload_sky_survey_batch_file_variations(content, response_string, myclient):
    """
    Test how /upload-sky-survey-batch handles files with various issues:

    - empty csv
    - header only in csv
    - empty header csv
    - invalid UTF-8 bytes
    - malformed csv
    """
    metadata_file = Path("tests/data/metadata_test.json")

    with metadata_file.open("rb") as metadata_f:
        files = [
            ("metadata_file", (metadata_file.name, metadata_f, "application/json")),
            ("csv_files", ("test.csv", content, "text/csv")),
        ]
        response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 400
    assert response_string in response.json()["detail"]


def test_upload_sky_survey_batch_valid_without_csv_extension(myclient):
    """Test uploading a valid CSV file without .csv extension."""

    file_path = Path("tests/data/test_catalogue_1.csv")
    metadata_file = Path("tests/data/metadata_test.json")

    with (
        metadata_file.open("rb") as metadata_f,
        file_path.open("rb") as f,
        patch("ska_sdp_global_sky_model.api.app.main.ingest_catalogue", return_value=True),
    ):
        # Use .txt extension but valid CSV content
        files = [
            ("metadata_file", (metadata_file.name, metadata_f, "application/json")),
            ("csv_files", ("data.txt", f, "text/plain")),
        ]
        response = myclient.post("/upload-sky-survey-batch", files=files)

        # Should succeed because validation is content-based, not extension-based
        assert response.status_code == 200
        assert "upload_id" in response.json()


def test_upload_sky_survey_batch_too_many_metadata_files(myclient):
    """Test trying to upload too many metadata files - should fail."""
    first_file = Path("tests/data/test_catalogue_1.csv")
    second_file = Path("tests/data/test_catalogue_2.csv")
    metadata_file1 = Path("tests/data/metadata_test.json")
    metadata_file2 = Path("tests/data/metadata_gleam.json")

    with (
        metadata_file1.open("rb") as metadata_f1,
        metadata_file2.open("rb") as metadata_f2,
        first_file.open("rb") as f1,
        second_file.open("rb") as f2,
    ):
        files = [
            ("metadata_file", (metadata_file1.name, metadata_f1, "application/json")),
            ("metadata_file", (metadata_file2.name, metadata_f2, "application/json")),
            ("csv_files", (first_file.name, f1, "text/csv")),
            ("csv_files", (second_file.name, f2, "text/csv")),
        ]
        response = myclient.post("/upload-sky-survey-batch", files=files)

    assert response.status_code == 400
    assert "must be one metadata JSON file" in response.json()["detail"]


def test_review_upload_success(myclient, gsm_metadata, db_session):
    """Test successful review of staged upload."""
    # Create a fake upload ID
    upload_id = "test-upload-review-123"

    # Directly insert test data into staging table
    for i in range(15):
        component = SkyComponentStaging(
            component_id=f"TEST{i:05d}",
            upload_id=upload_id,
            ra_deg=10.0 + i,
            dec_deg=20.0 + i,
            i_pol_jy=0.5 + i * 0.1,
            gsm_id=gsm_metadata.id,
        )
        db_session.add(component)
    db_session.commit()

    # Create upload status
    upload_status = UploadStatus(upload_id=upload_id, total_csv_files=1, metadata=None)
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
    # Create upload in non-completed state
    upload_id = "test-upload-not-completed-123"
    upload_status = UploadStatus(upload_id=upload_id, total_csv_files=1, metadata=None)
    upload_status.state = "uploading"  # Not completed
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    # Review before completion
    review_response = myclient.get(f"/review-upload/{upload_id}")
    assert review_response.status_code == 400
    assert "not ready for review" in review_response.json()["detail"].lower()


def test_commit_upload_success(myclient, gsm_metadata, db_session):
    """Test successful first commit auto-assigns version 0.1.0."""
    # Create test data directly in staging table (no version - assigned at commit)
    upload_id = "test-upload-commit-123"
    gsm_metadata.upload_id = upload_id

    db_session.add(gsm_metadata)
    db_session.commit()
    for i in range(5):
        component = SkyComponentStaging(
            component_id=f"COMMIT_TEST{i:05d}",
            upload_id=upload_id,
            ra_deg=10.0 + i,
            dec_deg=20.0 + i,
            i_pol_jy=0.5,
            gsm_id=gsm_metadata.id,
        )
        db_session.add(component)
    db_session.commit()

    # Create and attach metadata with no version (auto-assigned at commit time)
    metadata = GlobalSkyModelMetadata(
        version=None,
        catalogue_name=gsm_metadata.catalogue_name,
        description=gsm_metadata.description,
        upload_id=upload_id,
        staging=True,
    )
    upload_status = UploadStatus(upload_id=upload_id, total_csv_files=1, metadata=metadata)
    upload_status.state = "completed"
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    # Commit the upload
    commit_response = myclient.post(f"/commit-upload/{upload_id}")
    assert commit_response.status_code == 200
    commit_data = commit_response.json()
    assert commit_data["status"] == "success"
    assert commit_data["records_committed"] == 5
    # First commit for this catalogue gets version 0.1.0
    assert commit_data["version"] == "1.1.0"

    # Verify data moved to main table with auto-assigned version
    main_records = (
        db_session.query(SkyComponent).filter(SkyComponent.component_id.like("COMMIT_TEST%")).all()
    )
    assert len(main_records) == 5

    # All records should share the same auto-assigned version
    versions = {r.gsm_id for r in main_records}
    assert len(versions) == 1
    assert list(versions)[0] == gsm_metadata.id

    # Verify staging table is cleared
    staging_records = (
        db_session.query(SkyComponentStaging)
        .filter(SkyComponentStaging.upload_id == upload_id)
        .all()
    )
    assert len(staging_records) == 0


def test_commit_upload_increments_version(myclient, gsm_metadata, db_session):
    """Test that second commit for the same catalogue auto-increments to 0.2.0."""
    # Create new staging data for the same catalogue (no version - assigned at commit)
    upload_id = "test-upload-increment-123"
    gsm_metadata.upload_id = upload_id
    db_session.add(gsm_metadata)
    db_session.commit()
    for i in range(5):
        component = SkyComponentStaging(
            component_id=f"NEW{i:05d}",
            upload_id=upload_id,
            ra_deg=10.0 + i,
            dec_deg=20.0 + i,
            i_pol_jy=0.5,
            gsm_id=gsm_metadata.id,
        )
        db_session.add(component)
    db_session.commit()

    # Metadata with no version - commit endpoint auto-assigns the next version
    metadata = GlobalSkyModelMetadata(
        version=None,
        catalogue_name=gsm_metadata.catalogue_name,
        description=gsm_metadata.description,
        upload_id=upload_id,
        staging=True,
    )
    upload_status = UploadStatus(upload_id=upload_id, total_csv_files=1, metadata=metadata)
    upload_status.state = "completed"
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    commit_response = myclient.post(f"/commit-upload/{upload_id}")
    assert commit_response.status_code == 200
    commit_data = commit_response.json()
    # Minor version incremented from 0.1.0 to 0.2.0 for this catalogue
    assert commit_data["version"] == "1.1.0"

    # Verify new records have version 0.2.0
    new_records = (
        db_session.query(SkyComponent).filter(SkyComponent.component_id.like("NEW%")).all()
    )
    assert len(new_records) == 5
    for record in new_records:
        assert record.gsm_id == gsm_metadata.id


def test_commit_upload_per_catalogue_versioning(myclient, db_session):
    """Test that versioning is independent per catalogue name."""
    upload_id = "test-cat-b-upload-123"

    # Simulate catalogue A already at version 0.3.0
    gsm_metadata = GlobalSkyModelMetadata(
        version="0.3.0",
        catalogue_name="CAT_A",
        description="Catalogue A",
        upload_id="cat-a-upload-id",
        staging=False,
    )
    db_session.add(gsm_metadata)
    metadata = GlobalSkyModelMetadata(
        version=None,
        catalogue_name="CAT_B",
        description="Catalogue B first upload",
        upload_id=upload_id,
        staging=True,
    )
    db_session.add(metadata)

    # Upload for catalogue B (independent - should start at 0.1.0)
    component = SkyComponentStaging(
        component_id="CAT_B_COMP_001",
        upload_id=upload_id,
        ra_deg=15.0,
        dec_deg=25.0,
        i_pol_jy=0.5,
        gsm_id=metadata.id,
    )
    db_session.add(component)
    db_session.commit()

    upload_status = UploadStatus(upload_id=upload_id, total_csv_files=1, metadata=metadata)
    upload_status.state = "completed"
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    commit_response = myclient.post(f"/commit-upload/{upload_id}")
    assert commit_response.status_code == 200
    # CAT_B has no prior versions; first commit should be 0.1.0 regardless of CAT_A
    assert commit_response.json()["version"] == "0.1.0"


def test_commit_upload_not_completed(myclient):
    """Test commit fails if upload not completed."""
    # Create upload in non-completed state
    upload_id = "test-upload-commit-not-done-123"
    upload_status = UploadStatus(upload_id=upload_id, total_csv_files=1, metadata=None)
    upload_status.state = "uploading"  # Not completed
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    commit_response = myclient.post(f"/commit-upload/{upload_id}")
    assert commit_response.status_code == 400
    assert "not ready for commit" in commit_response.json()["detail"].lower()


def test_reject_upload_success(myclient, db_session):
    """Test successful rejection of staged upload."""
    # Directly insert staging records
    upload_id = "test-upload-reject-123"
    for i in range(5):
        component = SkyComponentStaging(
            component_id=f"REJECT_TEST{i:03d}",
            upload_id=upload_id,
            ra_deg=10.0 + i,
            dec_deg=20.0 + i,
            i_pol_jy=0.5,
            gsm_id=None,
        )
        db_session.add(component)
    db_session.commit()

    # Create completed upload status
    upload_status = UploadStatus(upload_id=upload_id, total_csv_files=1, metadata=None)
    upload_status.state = "completed"
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    # Reject the upload
    reject_response = myclient.delete(f"/reject-upload/{upload_id}")
    assert reject_response.status_code == 200
    reject_data = reject_response.json()
    assert reject_data["status"] == "success"
    assert reject_data["records_deleted"] == 5

    # Verify staging table is cleared
    staging_records = (
        db_session.query(SkyComponentStaging)
        .filter(SkyComponentStaging.upload_id == upload_id)
        .all()
    )
    assert len(staging_records) == 0


def test_reject_upload_not_completed(myclient):
    """Test reject fails if upload not completed."""
    # Create incomplete upload status
    upload_id = "test-upload-reject-incomplete-456"
    upload_status = UploadStatus(upload_id=upload_id, total_csv_files=1, metadata=None)
    upload_status.state = "uploading"
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    # Reject before completion
    reject_response = myclient.delete(f"/reject-upload/{upload_id}")

    assert reject_response.status_code == 400
    assert "not ready for rejection" in reject_response.json()["detail"].lower()


@patch("ska_sdp_global_sky_model.api.app.main.ingest_catalogue", Mock(side_effect=[True, False]))
def test_upload_batch_partial_fail_clears_staging(myclient, db_session):
    """Test that if one good and one bad file are uploaded, staging is cleared on failure."""
    good_file = Path("tests/data/test_catalogue_1.csv")

    # Create bad CSV bytes from the good file (removes ra/dec in first rows)
    bad_csv_bytes = _make_bad_csv(good_file, n_missing=2)

    with good_file.open("rb") as f1:
        metadata_file = Path("tests/data/metadata_test.json")
        with metadata_file.open("rb") as meta_f:
            files = [
                ("metadata_file", (metadata_file.name, meta_f, "application/json")),
                ("csv_files", (good_file.name, f1, "text/csv")),
                ("csv_files", ("bad.csv", io.BytesIO(bad_csv_bytes), "text/csv")),
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
    count = (
        db_session.query(SkyComponentStaging)
        .filter(SkyComponentStaging.upload_id == upload_id)
        .count()
    )
    assert count == 0
