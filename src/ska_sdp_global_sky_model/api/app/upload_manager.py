"""
Upload manager for handling batch file uploads and tracking.

This module provides functionality for managing batch uploads of sky survey data,
including file validation, in-memory storage, and upload state tracking.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
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
    files: list[tuple[str, bytes]] = field(default_factory=list)  # (filename, content)

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

        status = UploadStatus(
            upload_id=upload_id,
            total=file_count,
            state=UploadState.UPLOADING,
        )

        self._uploads[upload_id] = status
        logger.info("Created upload %s for %d files", upload_id, file_count)

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

    async def save_file(self, file: UploadFile, upload_status: UploadStatus) -> None:
        """
        Save an uploaded file to memory.

        Parameters
        ----------
        file : UploadFile
            File to save
        upload_status : UploadStatus
            Upload status tracking object
        """
        contents = await file.read()
        file_size = len(contents)

        upload_status.files.append((file.filename, contents))
        upload_status.uploaded += 1

        logger.info(
            "Stored file %s (%d bytes) in memory",
            file.filename,
            file_size,
        )

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
        Clean up memory and remove upload tracking.

        Parameters
        ----------
        upload_id : str
            Upload identifier
        """
        if upload_id in self._uploads:
            # Clear file contents from memory
            self._uploads[upload_id].files.clear()
            logger.info("Cleaned up upload %s from memory", upload_id)

    def get_files(self, upload_id: str) -> list[tuple[str, bytes]]:
        """
        Get all files for an upload.

        Parameters
        ----------
        upload_id : str
            Upload identifier

        Returns
        -------
        list[tuple[str, bytes]]
            List of (filename, content) tuples
        """
        status = self.get_status(upload_id)
        return status.files
