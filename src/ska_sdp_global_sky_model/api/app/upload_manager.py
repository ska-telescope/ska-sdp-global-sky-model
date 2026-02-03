"""
Upload manager for handling batch file uploads and tracking.

This module provides functionality for managing batch uploads of sky survey data,
including file validation, temporary storage, and upload state tracking.
"""

import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional
from uuid import uuid4

from fastapi import HTTPException, UploadFile

logger = logging.getLogger(__name__)


class UploadState(str, Enum):
    """Upload states for a sky survey batch upload."""

    PENDING = "pending"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class UploadStatus:
    """Track the status of a batch upload."""

    upload_id: str
    total: int
    uploaded: int = 0
    state: UploadState = UploadState.PENDING
    errors: list[str] = field(default_factory=list)
    temp_dir: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert status to dictionary."""
        return {
            "upload_id": self.upload_id,
            "state": self.state.value,
            "total_files": self.total,
            "uploaded_files": self.uploaded,
            "remaining_files": self.total - self.uploaded,
            "errors": self.errors,
        }


class UploadManager:
    """Manages batch uploads of sky survey files."""

    def __init__(self):
        """Initialize the upload manager."""
        self._uploads: dict[str, UploadStatus] = {}

    def create_upload(self, file_count: int) -> UploadStatus:
        """
        Create a new upload tracking entry.

        Parameters
        ----------
        file_count : int
            Number of files in the batch

        Returns
        -------
        UploadStatus
            The created upload status object
        """
        upload_id = str(uuid4())
        temp_dir = tempfile.mkdtemp(prefix="sky_survey_")

        status = UploadStatus(
            upload_id=upload_id,
            total=file_count,
            state=UploadState.UPLOADING,
            temp_dir=temp_dir,
        )

        self._uploads[upload_id] = status
        logger.info("Created upload %s for %d files in %s", upload_id, file_count, temp_dir)

        return status

    def get_status(self, upload_id: str) -> UploadStatus:
        """
        Retrieve the status of an upload.

        Parameters
        ----------
        upload_id : str
            Unique identifier of the upload

        Returns
        -------
        UploadStatus
            The upload status object

        Raises
        ------
        HTTPException
            If the upload ID does not exist
        """
        if upload_id not in self._uploads:
            raise HTTPException(status_code=404, detail="Upload ID not found")

        return self._uploads[upload_id]

    def validate_file(self, file: UploadFile) -> None:
        """
        Validate that a file is a CSV.

        Parameters
        ----------
        file : UploadFile
            File to validate

        Raises
        ------
        HTTPException
            If the file is not a CSV
        """
        # Check filename extension
        if not file.filename.endswith(".csv"):
            raise HTTPException(
                status_code=400, detail=f"Invalid file type for {file.filename}. Must be CSV."
            )

        # Check content type (allow common CSV MIME types)
        allowed_types = ["text/csv", "application/csv", "text/plain", "application/vnd.ms-excel"]
        if file.content_type not in allowed_types:
            raise HTTPException(
                status_code=400, detail=f"Invalid file type for {file.filename}. Must be CSV."
            )

    async def save_file(self, file: UploadFile, upload_status: UploadStatus) -> Path:
        """
        Save an uploaded file to the temporary directory.

        Parameters
        ----------
        file : UploadFile
            File to save
        upload_status : UploadStatus
            Upload status tracking object

        Returns
        -------
        Path
            Path where the file was saved

        Raises
        ------
        HTTPException
            If there's insufficient disk space
        """
        if not upload_status.temp_dir:
            raise HTTPException(status_code=500, detail="Upload temporary directory not set")

        # Check disk space
        statvfs = os.statvfs(upload_status.temp_dir)
        free_space = statvfs.f_frsize * statvfs.f_bavail

        contents = await file.read()
        file_size = len(contents)

        if file_size > free_space:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient disk space for {file.filename}",
            )

        file_path = Path(upload_status.temp_dir) / file.filename
        with open(file_path, "wb") as f:
            f.write(contents)

        upload_status.uploaded += 1
        logger.info(
            "Saved file %s (%d bytes) to %s",
            file.filename,
            file_size,
            file_path,
        )

        return file_path

    def mark_completed(self, upload_id: str) -> None:
        """
        Mark an upload as completed.

        Parameters
        ----------
        upload_id : str
            Upload identifier
        """
        status = self.get_status(upload_id)
        status.state = UploadState.COMPLETED
        logger.info("Upload %s completed successfully", upload_id)

    def mark_failed(self, upload_id: str, error: str) -> None:
        """
        Mark an upload as failed and record the error.

        Parameters
        ----------
        upload_id : str
            Upload identifier
        error : str
            Error message
        """
        status = self.get_status(upload_id)
        status.state = UploadState.FAILED
        status.errors.append(error)
        logger.error("Upload %s failed: %s", upload_id, error)

    def cleanup(self, upload_id: str) -> None:
        """
        Clean up temporary files for an upload.

        Parameters
        ----------
        upload_id : str
            Upload identifier
        """
        status = self.get_status(upload_id)
        if status.temp_dir and os.path.exists(status.temp_dir):
            shutil.rmtree(status.temp_dir, ignore_errors=True)
            logger.info("Cleaned up temporary directory for upload %s", upload_id)

    def list_files(self, upload_id: str) -> list[Path]:
        """
        List all files in an upload's temporary directory.

        Parameters
        ----------
        upload_id : str
            Upload identifier

        Returns
        -------
        list[Path]
            List of file paths
        """
        status = self.get_status(upload_id)
        if not status.temp_dir:
            return []

        temp_path = Path(status.temp_dir)
        if not temp_path.exists():
            return []

        return list(temp_path.iterdir())
