"""
Tests for upload_manager module.

This module tests the UploadManager class including CSV validation,
file handling, and upload state tracking.
"""

import json
from datetime import datetime, timedelta
from io import BytesIO
from uuid import uuid4

import pytest
from fastapi import HTTPException, UploadFile

from ska_sdp_global_sky_model.api.app.models import (
    GlobalSkyModelMetadata,
    SkyComponentStaging,
    UploadTaskState,
)
from ska_sdp_global_sky_model.api.app.upload_manager import (
    UploadTask,
    load_metadata_from_json,
    run_db_cleanup,
)
from ska_sdp_global_sky_model.configuration.config import CATALOGUE_CLEANUP_AGE
from tests.utils import clean_all_tables


@pytest.fixture(scope="function", autouse=True)
def clean_up_database():
    """
    Clean tables after each test run.
    Specific to this module. Do not move.
    """
    yield
    clean_all_tables()


@pytest.fixture(name="upload_task")
def _fxt_upload_task():
    u_id = str(uuid4())
    task = UploadTaskState(upload_id=u_id, status="pending")
    metadata = GlobalSkyModelMetadata(upload_id=u_id, catalogue_name="cat-name")

    return UploadTask(metadata, task)


def test_task_fetch_from_db_not_found(db_session):
    """Check that exception is thrown when task isn't found"""
    with pytest.raises(HTTPException) as err:
        UploadTask.fetch_from_db(db_session, "some-bad-id")

    assert err.value.status_code == 404
    assert err.value.detail == "Upload ID not found"


def test_task_fetch_from_db(db_session, upload_task):
    """Check that a task and catalogue can be retrieved from the DB"""
    db_session.add(upload_task.task_status)
    db_session.add(upload_task.catalogue_metadata)
    db_session.commit()

    task = UploadTask.fetch_from_db(db_session, upload_task.task_status.upload_id)

    assert task.catalogue_metadata.id == upload_task.catalogue_metadata.id
    assert task.name == upload_task.name
    assert task.task_status.id == upload_task.task_status.id


@pytest.mark.parametrize(
    "staging",
    [True, False],
)
def test_task_create_new(db_session, staging: bool):
    """Check that a task can be created from metadata"""
    metadata = GlobalSkyModelMetadata(catalogue_name="a-new-name", staging=staging)

    task = UploadTask.create(db_session, metadata)

    assert task.catalogue_metadata == metadata
    assert task.task_status.status == "pending" if staging else "released"
    assert task.task_status.upload_id == task.catalogue_metadata.upload_id


def test_to_dict(db_session):
    """Test conversion to dictionary."""
    u_id = "upload-id-from-test"
    task = UploadTaskState(upload_id=u_id, status="pending")
    metadata = GlobalSkyModelMetadata(upload_id=u_id, catalogue_name="cat-name")
    db_session.add(task)
    db_session.add(metadata)
    db_session.commit()

    status = UploadTask.fetch_from_db(db_session, u_id)

    result = status.to_dict()

    assert result["upload_id"] == u_id
    assert result["state"] == "pending"
    assert result["total_csv_files"] == 0
    assert result["uploaded_csv_files"] == 0
    assert result["remaining_csv_files"] == 0
    assert result["errors"] == [""]
    assert result["has_metadata"] is True
    assert result["metadata"]["catalogue_name"] == "cat-name"
    assert result["metadata"]["upload_id"] == u_id


def test_properties(upload_task):
    """Test that known properties do what is required"""
    assert upload_task.upload_id == upload_task.catalogue_metadata.upload_id
    assert upload_task.name == "cat-name"
    assert upload_task.status == "pending"
    assert upload_task.is_uploaded is False


def test_update_status(upload_task):
    """When we run update status the correct fields are updated"""
    upload_task.update_status("uploading", "update message")
    assert upload_task.task_status.status == "uploading"
    assert upload_task.task_status.reason == "update message"
    assert upload_task.task_status.last_update > datetime.now() - timedelta(seconds=1)


def test_update_status_existing_reason(upload_task):
    """Test that a message gets appended to the reason"""
    upload_task.task_status.reason = "existing reason"
    upload_task.update_status("uploading", "update message")
    assert upload_task.task_status.status == "uploading"
    assert upload_task.task_status.reason == "existing reason, update message"
    assert upload_task.task_status.last_update > datetime.now() - timedelta(seconds=1)


def test_update_status_no_message(upload_task):
    """Test that the reason is not updated when there is no message"""
    upload_task.update_status(
        "uploading",
    )
    assert upload_task.task_status.status == "uploading"
    # The default is either None or "" depending on how the object gets created
    assert upload_task.task_status.reason in [None, ""]
    assert upload_task.task_status.last_update > datetime.now() - timedelta(seconds=1)


def test_mark_released(upload_task):
    """Test that the correct fields are updated when marking an upload as released"""
    upload_task.mark_released()

    assert upload_task.task_status.status == "completed"
    assert upload_task.task_status.last_update > datetime.now() - timedelta(seconds=1)


def test_mark_uploading(upload_task):
    """Test that the correct fields are updated when marking an upload as uploading"""
    upload_task.mark_uploading()

    assert upload_task.task_status.status == "uploading"
    assert upload_task.task_status.last_update > datetime.now() - timedelta(seconds=1)


def test_mark_uploaded(upload_task):
    """Test that the correct fields are updated when marking an upload as uploaded"""
    upload_task.mark_uploaded()

    assert upload_task.task_status.status == "staged"
    assert upload_task.task_status.last_update > datetime.now() - timedelta(seconds=1)


def test_mark_failed(upload_task):
    """Test that the correct fields are updated when marking an upload as failed"""
    upload_task.mark_failed("user aborted")

    assert upload_task.task_status.status == "failed"
    assert upload_task.task_status.reason == "user aborted"
    assert upload_task.task_status.last_update > datetime.now() - timedelta(seconds=1)


def test_get_files(upload_task):
    """Test retrieving files from an upload."""
    upload_task.files.append(("file1.csv", "data1"))
    upload_task.files.append(("file2.csv", "data2"))

    files = upload_task.files

    assert len(files) == 2
    assert files[0] == ("file1.csv", "data1")
    assert files[1] == ("file2.csv", "data2")


@pytest.fixture(name="catalogue_metadata")
def _fxt_catalogue_metadata():
    return GlobalSkyModelMetadata(
        version="1.0.0",
        catalogue_name="TestCatalogue",
        upload_id="test-upload-1",
    )


@pytest.mark.parametrize(
    "delete",
    [True, False],
)
def test_cleanup_old_catalogues(db_session, delete: bool):
    """Test removal of catalogues that were created a long time ago"""

    catalogue_old = GlobalSkyModelMetadata(
        catalogue_name="Old Catalogue",
        upload_id="1234-abcd-a",
        staging=True,
        uploaded_at=(datetime.now() - timedelta(hours=CATALOGUE_CLEANUP_AGE + 1)),
    )
    catalogue_fine = GlobalSkyModelMetadata(
        catalogue_name="Old Catalogue",
        upload_id="1234-abcd-b",
        staging=True,
        uploaded_at=(datetime.now() - timedelta(hours=CATALOGUE_CLEANUP_AGE - 1)),
    )
    catalogue_complete = GlobalSkyModelMetadata(
        catalogue_name="Old Catalogue",
        upload_id="1234-abcd-c",
        staging=False,
        uploaded_at=(datetime.now() - timedelta(hours=CATALOGUE_CLEANUP_AGE + 1)),
    )

    db_session.add(catalogue_old)
    db_session.add(catalogue_fine)
    db_session.add(catalogue_complete)
    db_session.commit()

    run_db_cleanup(db_session, do_delete=delete)

    catalogues = db_session.query(GlobalSkyModelMetadata).all()

    expected = {catalogue_fine.upload_id, catalogue_complete.upload_id}
    if not delete:
        expected.add(catalogue_old.upload_id)

    assert len(catalogues) == len(expected)

    assert {cat.upload_id for cat in catalogues} == expected


@pytest.mark.parametrize(
    "delete",
    [True, False],
)
def test_cleanup_component_without_catalogues(db_session, delete: bool):
    """Test removal of staging components that have no link to a catalogue"""

    component = SkyComponentStaging(
        component_id="C1",
        ra_deg=1,
        dec_deg=1,
        upload_id="1234-abcd",
    )
    db_session.add(component)
    db_session.commit()

    assert db_session.query(SkyComponentStaging).count() == 1
    run_db_cleanup(db_session, do_delete=delete)

    assert db_session.query(SkyComponentStaging).count() == (0 if delete else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name_input, expected_name",
    [
        ("TEST_CAT", "TEST_CAT"),
        (" TEST_CAT", "TEST_CAT"),
        ("TEST_CAT ", "TEST_CAT"),
        (" TEST_CAT ", "TEST_CAT"),
    ],
)
async def test_load_metadata_from_json_correct_cat_name(name_input, expected_name):
    """
    Function correctly strips white spaces around catalogue_name.
    """
    metadata = {
        "catalogue_name": name_input,
    }
    metadata_f = BytesIO(json.dumps(metadata).encode("utf-8"))
    met_file = UploadFile(
        filename="metadata_test.json",
        file=metadata_f,
        headers={"content-type": "application/json"},
    )

    result = await load_metadata_from_json(met_file)

    assert result.catalogue_name == expected_name


@pytest.mark.asyncio
async def test_load_metadata_from_json_defaults():
    """
    Test the default metadata that is loaded.

    From the ones tested below "version" and "staging"
    are always the same, they cannot be modified via the function.
    """
    metadata = {}
    metadata_f = BytesIO(json.dumps(metadata).encode("utf-8"))
    met_file = UploadFile(
        filename="metadata_test.json",
        file=metadata_f,
        headers={"content-type": "application/json"},
    )

    result = await load_metadata_from_json(met_file)

    assert result.catalogue_name == "UPLOAD"
    assert result.version is None
    assert result.staging is True
    assert result.author is None
    assert result.freq_min_hz is None


@pytest.mark.asyncio
async def test_load_metadata_from_json_incorrect_json():
    """Raise error when data cannot be loaded as JSON."""
    metadata = "not a json".encode("utf-8")
    metadata_f = BytesIO(metadata)
    met_file = UploadFile(
        filename="metadata_test.json",
        file=metadata_f,
        headers={"content-type": "application/json"},
    )

    with pytest.raises(HTTPException) as exc:
        await load_metadata_from_json(met_file)

    assert "Metadata file metadata_test.json is not valid JSON" in exc.value.detail


@pytest.mark.asyncio
async def test_load_metadata_from_json_incorrect_encoding():
    """Raise error when encoding is not utf-8."""
    metadata = "not a json".encode("utf-16")
    metadata_f = BytesIO(metadata)
    met_file = UploadFile(
        filename="metadata_test.json",
        file=metadata_f,
        headers={"content-type": "application/json"},
    )

    with pytest.raises(HTTPException) as exc:
        await load_metadata_from_json(met_file)

    assert "Metadata file metadata_test.json is not valid UTF-8 text" in exc.value.detail


@pytest.mark.asyncio
async def test_load_metadata_from_json_freq_data():
    """
    Function correctly loads supplied frequency information.
    """
    metadata = {
        "freq_min_hz": "1000.0",
        "freq_max_hz": "5000.0",
    }
    metadata_f = BytesIO(json.dumps(metadata).encode("utf-8"))
    met_file = UploadFile(
        filename="metadata_test.json",
        file=metadata_f,
        headers={"content-type": "application/json"},
    )

    result = await load_metadata_from_json(met_file)

    assert result.freq_min_hz == 1000.0
    assert result.freq_max_hz == 5000.0
