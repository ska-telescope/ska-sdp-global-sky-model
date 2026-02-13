"""
Tests for upload_manager module.

This module tests the UploadManager class including CSV validation,
file handling, and upload state tracking.
"""

import pytest
from fastapi import HTTPException

from ska_sdp_global_sky_model.api.app.upload_manager import (
    UploadManager,
    UploadState,
    UploadStatus,
)


class TestUploadStatus:  # pylint: disable=too-few-public-methods
    """Tests for UploadStatus dataclass."""

    def test_to_dict(self):
        """Test conversion to dictionary."""
        status = UploadStatus(
            upload_id="test-id",
            total_csv_files=5,
            uploaded_csv_files=2,
            state=UploadState.UPLOADING,
            errors=["error1"],
            csv_files=[],
            metadata=None,
        )

        result = status.to_dict()

        assert result["upload_id"] == "test-id"
        assert result["state"] == "uploading"
        assert result["total_csv_files"] == 5
        assert result["uploaded_csv_files"] == 2
        assert result["remaining_csv_files"] == 3
        assert result["errors"] == ["error1"]
        assert result["has_metadata"] is False


class TestUploadManager:
    """Tests for UploadManager class."""

    @pytest.fixture
    def manager(self):
        """Create an UploadManager instance."""
        return UploadManager()

    def test_create_upload(self, manager):
        """Test creating a new upload."""
        status = manager.create_upload(csv_file_count=3)

        assert status.upload_id is not None
        assert status.total_csv_files == 3
        assert status.uploaded_csv_files == 0
        assert status.state == UploadState.UPLOADING
        assert status.errors == []
        assert status.csv_files == []
        assert status.metadata is None

    def test_get_status_success(self, manager):
        """Test retrieving upload status."""
        created = manager.create_upload(csv_file_count=2)
        retrieved = manager.get_status(created.upload_id)

        assert retrieved.upload_id == created.upload_id
        assert retrieved.total_csv_files == 2

    def test_get_status_not_found(self, manager):
        """Test retrieving non-existent upload."""
        with pytest.raises(HTTPException) as exc_info:
            manager.get_status("non-existent-id")

        assert exc_info.value.status_code == 404
        assert "Upload ID not found" in exc_info.value.detail

    def test_mark_completed(self, manager):
        """Test marking an upload as completed."""
        status = manager.create_upload(csv_file_count=1)
        manager.mark_completed(status.upload_id)

        retrieved = manager.get_status(status.upload_id)
        assert retrieved.state == UploadState.COMPLETED

    def test_mark_failed(self, manager):
        """Test marking an upload as failed."""
        status = manager.create_upload(csv_file_count=1)
        error_msg = "Test error"
        manager.mark_failed(status.upload_id, error_msg)

        retrieved = manager.get_status(status.upload_id)
        assert retrieved.state == UploadState.FAILED
        assert error_msg in retrieved.errors

    def test_cleanup(self, manager):
        """Test cleaning up upload data."""
        status = manager.create_upload(csv_file_count=1)
        status.csv_files.append(("test.csv", "data"))

        manager.cleanup(status.upload_id)

        # Files should be cleared but status should still exist
        retrieved = manager.get_status(status.upload_id)
        assert len(retrieved.csv_files) == 0

    def test_get_files(self, manager):
        """Test retrieving files from an upload."""
        status = manager.create_upload(csv_file_count=2)
        status.csv_files.append(("file1.csv", "data1"))
        status.csv_files.append(("file2.csv", "data2"))

        files = manager.get_files(status.upload_id)

        assert len(files) == 2
        assert files[0] == ("file1.csv", "data1")
        assert files[1] == ("file2.csv", "data2")
