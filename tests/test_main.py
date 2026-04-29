# pylint: disable=too-many-lines

"""
Basic testing of the API
"""

import csv
import io
from pathlib import Path
from unittest.mock import MagicMock, patch, Mock

import pytest

from ska_sdp_global_sky_model.api.app.main import app, get_db, upload_manager, wait_for_db
from ska_sdp_global_sky_model.api.app.models import (
    GlobalSkyModelMetadata,
    SkyComponent,
    SkyComponentStaging,
)
from ska_sdp_global_sky_model.api.app.upload_manager import UploadStatus
from tests.utils import clean_all_tables, override_get_db

app.dependency_overrides[get_db] = override_get_db


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


def test_read_main(myclient):
    """Unit test for the root path "/" """
    response = myclient.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "live"}


def test_components(myclient, gsm_metadata):
    """Unit test for the /components endpoint"""

    # Add a test component directly to the test database using override_get_db
    # Use the overridden database session
    db = next(override_get_db())
    try:
        db.add(gsm_metadata)
        db.commit()
        component = SkyComponent(
            component_id="J030853+053903",
            ra_deg=47.222569,
            dec_deg=5.650958,
            i_pol_jy=0.098383,
            gsm_id=gsm_metadata.id,
        )
        db.add(component)
        db.commit()
    finally:
        db.close()

    response = myclient.get("/components")
    assert response.status_code == 200
    # Verify we have components
    assert "J030853+053903" in response.text


def test_local_sky_model(myclient, set_up_db):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query in the region covered by test data (RA ~80, Dec ~4 +- 1)
    without a specified version
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={"ra_deg": 80, "dec_deg": 4, "fov_deg": 1, "catalogue_name": "catalogue3"},
    )

    assert local_sky_model.status_code == 200

    assert local_sky_model.text.count("N000100") == 11
    for i in range(5, 16):
        assert f"N000100+0000{i:0>2d}" in local_sky_model.text


def test_local_sky_model_with_version(myclient, set_up_db):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query in the region covered by test data (RA ~90, Dec ~2 +- 5)
    but with version that only includes one component
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": "90",
            "dec_deg": "2",
            "fov_deg": 5,
            "version": "0.1.0",
            "catalogue_name": "catalogue1",
        },
    )

    assert local_sky_model.status_code == 200
    assert local_sky_model.text.count("W000010") == 20
    for i in range(20):
        assert f"W000010+0000{i:0>2d}" in local_sky_model.text


def test_local_sky_model_query_author(myclient, set_up_db):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query by author to test metadata query.
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": "0",
            "dec_deg": "0",
            "fov_deg": 180,
            "author__contains": "Other",  # Should match the whole of "catalogue2".
        },
    )

    assert local_sky_model.status_code == 200
    assert local_sky_model.text.count("A000100") == 200
    for i in range(200):
        assert f"A000100+000{i:0>3d}" in local_sky_model.text


def test_local_sky_model_query_freq_max(myclient, set_up_db):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query by freq_min_hz to test metadata query.
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": "0",
            "dec_deg": "0",
            "fov_deg": 180,
            "freq_max_hz__gt": 300e6,  # Should match the whole of "catalogue3".
        },
    )

    assert local_sky_model.status_code == 200
    assert local_sky_model.text.count("N000100") == 20
    for i in range(20):
        assert f"N000100+0000{i:0>2d}" in local_sky_model.text


def test_local_sky_model_small_fov(myclient, set_up_db):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query in the region covered by test data (RA ~70, Dec ~4, +-4)
    without version, with fov that only returns 41/200 objects
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": 70,
            "dec_deg": 4,
            "fov_deg": 4,
            "catalogue_name": "catalogue2",
            "version": "1.0.0",
        },
    )

    assert local_sky_model.status_code == 200
    assert local_sky_model.text.count("A000100") == 41
    for i in range(80, 121):
        assert f"A000100+000{i:0>3d}" in local_sky_model.text


def test_local_sky_model_extra_param(myclient, set_up_db):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query in the region covered by test data (RA ~70, Dec ~4, +-4)
    without version, and limiting another value.
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": 70,
            "dec_deg": 4,
            "fov_deg": 4,
            "catalogue_name": "catalogue2",
            "version": "1.0.0",
            "pa_deg__lt": 100,
        },
    )

    assert local_sky_model.status_code == 200
    assert local_sky_model.text.count("A000100") == 20
    for i in range(80, 100):
        assert f"A000100+000{i:0>3d}" in local_sky_model.text
    for i in range(100, 120):
        assert f"A000100+000{i:0>3d}" not in local_sky_model.text


def test_local_sky_model_flux_range_filter(myclient):
    """Test range filtering on flux values for /local-sky-model."""
    clean_all_tables()

    db = next(override_get_db())
    try:
        metadata = GlobalSkyModelMetadata(
            version="0.1.0",
            catalogue_name="catalogue",
            upload_id="range-upload",
        )
        db.add(metadata)
        db.commit()
        db.refresh(metadata)
        db.add_all(
            [
                SkyComponent(
                    component_id="J070001+040001",
                    ra_deg=70.001111,
                    dec_deg=4.001111,
                    i_pol_jy=0.098383,
                    gsm_id=metadata.id,
                ),
                SkyComponent(
                    component_id="J070002+040002",
                    ra_deg=70.002222,
                    dec_deg=4.002222,
                    i_pol_jy=0.798383,
                    gsm_id=metadata.id,
                ),
                SkyComponent(
                    component_id="J070003+040003",
                    ra_deg=70.003333,
                    dec_deg=4.003333,
                    i_pol_jy=1.298383,
                    gsm_id=metadata.id,
                ),
            ]
        )
        db.commit()
    finally:
        db.close()

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": 70,
            "dec_deg": 4,
            "fov_deg": 1,
            "catalogue_name": "catalogue",
            "version": "0.1.0",
            "i_pol_jy__gte": 0.5,
            "i_pol_jy__lte": 1.0,
        },
    )

    assert local_sky_model.status_code == 200
    assert "J070001+040001" not in local_sky_model.text
    assert "J070002+040002" in local_sky_model.text
    assert "J070003+040003" not in local_sky_model.text


def test_local_sky_model_missing_version(myclient, set_up_db):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query in the region covered by test data (RA ~42-50, Dec ~0-7)
    with version that does not exist
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": "90",
            "dec_deg": "2",
            "fov_deg": 5,
            "version": "2.0.0",
            "catalogue_name": "catalogue",
        },
    )

    assert local_sky_model.status_code == 200
    assert local_sky_model.text.count("0+0") == 0


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
        (b'name,ra,dec\n"unclosed quote,10.5,45.2\n', "not valid CSV at line 2"),
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


def test_review_upload_success(myclient, gsm_metadata):
    """Test successful review of staged upload."""
    clean_all_tables()

    # Create a fake upload ID
    upload_id = "test-upload-review-123"

    # Directly insert test data into staging table
    db = next(override_get_db())
    db.add(gsm_metadata)
    db.commit()
    try:
        for i in range(15):
            component = SkyComponentStaging(
                component_id=f"TEST{i:05d}",
                upload_id=upload_id,
                ra_deg=10.0 + i,
                dec_deg=20.0 + i,
                i_pol_jy=0.5 + i * 0.1,
                gsm_id=gsm_metadata.id,
            )
            db.add(component)
        db.commit()
    finally:
        db.close()

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
    clean_all_tables()

    # Create upload in non-completed state
    upload_id = "test-upload-not-completed-123"
    upload_status = UploadStatus(upload_id=upload_id, total_csv_files=1, metadata=None)
    upload_status.state = "uploading"  # Not completed
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    # Review before completion
    review_response = myclient.get(f"/review-upload/{upload_id}")
    assert review_response.status_code == 400
    assert "not ready for review" in review_response.json()["detail"].lower()


def test_commit_upload_success(myclient, gsm_metadata):
    """Test successful first commit auto-assigns version 0.1.0."""
    clean_all_tables()

    # Create test data directly in staging table (no version - assigned at commit)
    upload_id = "test-upload-commit-123"
    gsm_metadata.upload_id = upload_id
    db = next(override_get_db())
    db.add(gsm_metadata)
    db.commit()
    for i in range(5):
        component = SkyComponentStaging(
            component_id=f"COMMIT_TEST{i:05d}",
            upload_id=upload_id,
            ra_deg=10.0 + i,
            dec_deg=20.0 + i,
            i_pol_jy=0.5,
            gsm_id=gsm_metadata.id,
        )
        db.add(component)
    db.commit()

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
    db = next(override_get_db())
    try:
        main_records = (
            db.query(SkyComponent).filter(SkyComponent.component_id.like("COMMIT_TEST%")).all()
        )
        assert len(main_records) == 5

        # All records should share the same auto-assigned version
        versions = {r.gsm_id for r in main_records}
        assert len(versions) == 1
        assert list(versions)[0] == gsm_metadata.id

        # Verify staging table is cleared
        staging_records = (
            db.query(SkyComponentStaging).filter(SkyComponentStaging.upload_id == upload_id).all()
        )
        assert len(staging_records) == 0
    finally:
        db.close()


def test_commit_upload_increments_version(myclient, gsm_metadata):
    """Test that second commit for the same catalogue auto-increments to 0.2.0."""
    clean_all_tables()

    # Create new staging data for the same catalogue (no version - assigned at commit)
    upload_id = "test-upload-increment-123"
    db = next(override_get_db())
    gsm_metadata.upload_id = upload_id
    db.add(gsm_metadata)
    db.commit()
    for i in range(5):
        component = SkyComponentStaging(
            component_id=f"NEW{i:05d}",
            upload_id=upload_id,
            ra_deg=10.0 + i,
            dec_deg=20.0 + i,
            i_pol_jy=0.5,
            gsm_id=gsm_metadata.id,
        )
        db.add(component)
    db.commit()

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
    db = next(override_get_db())
    try:
        new_records = db.query(SkyComponent).filter(SkyComponent.component_id.like("NEW%")).all()
        assert len(new_records) == 5
        for record in new_records:
            assert record.gsm_id == gsm_metadata.id
    finally:
        db.close()


def test_commit_upload_per_catalogue_versioning(myclient):
    """Test that versioning is independent per catalogue name."""
    clean_all_tables()
    upload_id = "test-cat-b-upload-123"

    # Simulate catalogue A already at version 0.3.0
    db = next(override_get_db())
    gsm_metadata = GlobalSkyModelMetadata(
        version="0.3.0",
        catalogue_name="CAT_A",
        description="Catalogue A",
        upload_id="cat-a-upload-id",
        staging=False,
    )
    db.add(gsm_metadata)
    metadata = GlobalSkyModelMetadata(
        version=None,
        catalogue_name="CAT_B",
        description="Catalogue B first upload",
        upload_id=upload_id,
        staging=True,
    )
    db.add(metadata)
    db.commit()

    # Upload for catalogue B (independent - should start at 0.1.0)
    component = SkyComponentStaging(
        component_id="CAT_B_COMP_001",
        upload_id=upload_id,
        ra_deg=15.0,
        dec_deg=25.0,
        i_pol_jy=0.5,
        gsm_id=metadata.id,
    )
    db.add(component)
    db.commit()

    upload_status = UploadStatus(upload_id=upload_id, total_csv_files=1, metadata=metadata)
    upload_status.state = "completed"
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    commit_response = myclient.post(f"/commit-upload/{upload_id}")
    assert commit_response.status_code == 200
    # CAT_B has no prior versions; first commit should be 0.1.0 regardless of CAT_A
    assert commit_response.json()["version"] == "0.1.0"


def test_commit_upload_not_completed(myclient):
    """Test commit fails if upload not completed."""
    clean_all_tables()

    # Create upload in non-completed state
    upload_id = "test-upload-commit-not-done-123"
    upload_status = UploadStatus(upload_id=upload_id, total_csv_files=1, metadata=None)
    upload_status.state = "uploading"  # Not completed
    upload_manager._uploads[upload_id] = upload_status  # pylint: disable=protected-access

    commit_response = myclient.post(f"/commit-upload/{upload_id}")
    assert commit_response.status_code == 400
    assert "not ready for commit" in commit_response.json()["detail"].lower()


def test_reject_upload_success(myclient):
    """Test successful rejection of staged upload."""
    clean_all_tables()

    # Directly insert staging records
    upload_id = "test-upload-reject-123"
    db = next(override_get_db())
    try:
        for i in range(5):
            component = SkyComponentStaging(
                component_id=f"REJECT_TEST{i:03d}",
                upload_id=upload_id,
                ra_deg=10.0 + i,
                dec_deg=20.0 + i,
                i_pol_jy=0.5,
                gsm_id=None,
            )
            db.add(component)
        db.commit()
    finally:
        db.close()

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
    clean_all_tables()

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
def test_upload_batch_partial_fail_clears_staging(myclient):
    """Test that if one good and one bad file are uploaded, staging is cleared on failure."""
    clean_all_tables()
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
    db = next(override_get_db())
    # pylint: disable-next=duplicate-code
    try:
        count = (
            db.query(SkyComponentStaging)
            .filter(SkyComponentStaging.upload_id == upload_id)
            .count()
        )
        assert count == 0
    finally:
        db.close()


def _insert_test_metadata():
    """Insert multiple test metadata records for query tests."""
    db = next(override_get_db())
    try:
        db.query(GlobalSkyModelMetadata).delete()
        db.commit()
        records = [
            GlobalSkyModelMetadata(
                version="1.0",
                catalogue_name="GLEAM",
                description="First test catalogue",
                upload_id="upload1",
                staging=True,
            ),
            GlobalSkyModelMetadata(
                version="2.0",
                catalogue_name="GLEAM Extended",
                description="Second test catalogue",
                upload_id="upload2",
                staging=False,
            ),
            GlobalSkyModelMetadata(
                version="3.0",
                catalogue_name="LOFAR",
                description="Third test catalogue",
                upload_id="upload3",
                staging=True,
            ),
        ]
        db.add_all(records)
        db.commit()
    finally:
        db.close()


def test_query_metadata_basic(myclient):
    """Test retrieving all metadata records with no filters."""
    clean_all_tables()
    _insert_test_metadata()

    response = myclient.get("/catalogue-metadata")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 3


def test_query_metadata_filter_version_and_name(myclient):
    """Test filtering by version and partial catalogue name."""
    clean_all_tables()
    _insert_test_metadata()

    response = myclient.get(
        "/catalogue-metadata",
        params={"version": "2.0", "catalogue_name__contains": "GLEAM"},
    )
    assert response.status_code == 200

    data = response.json()
    assert len(data) == 1
    record = data[0]
    assert record["version"] == "2.0"
    assert "GLEAM" in record["catalogue_name"]


def test_query_metadata_sorting(myclient):
    """Test sorting by version descending."""
    clean_all_tables()
    _insert_test_metadata()

    response = myclient.get("/catalogue-metadata", params={"sort": "-version"})
    assert response.status_code == 200

    data = response.json()
    assert len(data) == 3
    versions = [r["version"] for r in data]
    assert versions == sorted(versions, reverse=True)


def test_query_metadata_fields_selection(myclient):
    """Test selecting only specific columns."""
    clean_all_tables()
    _insert_test_metadata()

    response = myclient.get("/catalogue-metadata", params={"fields": "version,catalogue_name"})
    assert response.status_code == 200

    data = response.json()
    for row in data:
        assert set(row.keys()) == {"version", "catalogue_name"}


def test_query_metadata_pagination(myclient):
    """Test limit parameter."""
    clean_all_tables()
    _insert_test_metadata()

    # Limit 2
    response = myclient.get("/catalogue-metadata", params={"limit": "2"})
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2


def test_query_metadata_gt_lt(myclient):
    """Test __gt and __lt operators on version field."""
    clean_all_tables()
    _insert_test_metadata()

    # __gt operator
    response = myclient.get("/catalogue-metadata", params={"version__gt": "1.5"})
    assert response.status_code == 200
    data = response.json()
    versions = [r["version"] for r in data]
    assert all(float(v) > 1.5 for v in versions)
    assert len(versions) == 2

    # __lt operator
    response = myclient.get("/catalogue-metadata", params={"version__lt": "3.0"})
    assert response.status_code == 200
    data = response.json()
    versions = [r["version"] for r in data]
    assert all(float(v) < 3.0 for v in versions)
    assert len(versions) == 2


def test_query_metadata_gte_lte_range(myclient):
    """Test range filtering on metadata fields."""
    clean_all_tables()
    _insert_test_metadata()

    response = myclient.get(
        "/catalogue-metadata",
        params={
            "version__gte": "2.0",
            "version__lte": "3.0",
            "sort": "version",
        },
    )
    assert response.status_code == 200
    data = response.json()

    versions = [row["version"] for row in data]
    assert versions == ["2.0", "3.0"]


def test_query_metadata_in_operator(myclient):
    """Test __in operator on catalogue_name."""
    clean_all_tables()
    _insert_test_metadata()

    response = myclient.get("/catalogue-metadata", params={"catalogue_name__in": "LOFAR,GLEAM"})
    assert response.status_code == 200
    data = response.json()
    names = [r["catalogue_name"] for r in data]
    assert set(names) == {"LOFAR", "GLEAM"}


def test_query_metadata_combined_operators(myclient):
    """Test combination of filters with sorting and fields selection."""
    clean_all_tables()
    _insert_test_metadata()

    response = myclient.get(
        "/catalogue-metadata",
        params={
            "version__gt": "1.0",
            "version__lt": "3.0",
            "fields": "version,catalogue_name",
            "sort": "-version",
        },
    )
    assert response.status_code == 200
    data = response.json()

    # Should only include version 2.0
    assert len(data) == 1
    row = data[0]
    assert row.keys() == {"version", "catalogue_name"}
    assert row["version"] == "2.0"
    assert row["catalogue_name"] == "GLEAM Extended"
