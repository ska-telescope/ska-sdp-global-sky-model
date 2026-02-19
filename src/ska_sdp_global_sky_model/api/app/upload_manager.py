"""
Upload manager for handling batch file uploads and tracking.

This module provides functionality for managing batch uploads of sky survey data,
including file validation, in-memory storage, metadata parsing, and upload state tracking.
"""

import csv
import io
import json
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
    total_csv_files: int
    uploaded_csv_files: int = 0
    state: UploadState = UploadState.PENDING
    errors: list[str] = field(default_factory=list)
    csv_files: list[tuple[str, str]] = field(default_factory=list)  # (filename, content as str)
    metadata: dict | None = None  # Parsed catalogue metadata

    def to_dict(self) -> dict:
        """Convert status to dictionary."""
        return {
            "upload_id": self.upload_id,
            "state": self.state.value,
            "total_csv_files": self.total_csv_files,
            "uploaded_csv_files": self.uploaded_csv_files,
            "remaining_csv_files": self.total_csv_files - self.uploaded_csv_files,
            "errors": self.errors,
            "has_metadata": self.metadata is not None,
            "metadata": self.metadata,
        }


class UploadManager:
    """Manages batch uploads of sky survey files."""

    def __init__(self):
        """Initialize the upload manager."""
        self._uploads: dict[str, UploadStatus] = {}

    def create_upload(self, csv_file_count: int) -> UploadStatus:
        """
        Create a new upload tracking entry.

        Parameters
        ----------
        csv_file_count : int
            Number of CSV files in the batch

        Returns
        -------
        UploadStatus
            The created upload status object
        """
        upload_id = str(uuid4())

        status = UploadStatus(
            upload_id=upload_id,
            total_csv_files=csv_file_count,
            state=UploadState.UPLOADING,
        )

        self._uploads[upload_id] = status
        logger.info("Created upload %s for %d CSV files", upload_id, csv_file_count)

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

    async def save_metadata_file(self, file: UploadFile, upload_status: UploadStatus) -> None:
        """
        Parse and validate metadata JSON file.

        Parameters
        ----------
        file : UploadFile
            Metadata file to parse
        upload_status : UploadStatus
            Upload status tracking object

        Raises
        ------
        HTTPException
            If file is not valid JSON or missing required fields
        """
        contents = await file.read()

        # Parse JSON
        try:
            text_content = contents.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Metadata file {file.filename} is not valid UTF-8 text.",
            ) from exc

        try:
            metadata = json.loads(text_content)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Metadata file {file.filename} is not valid JSON: {str(exc)}",
            ) from exc

        # Validate required fields
        required_fields = ["version", "catalogue_name", "description", "ref_freq", "epoch"]
        missing_fields = [field for field in required_fields if field not in metadata]

        if missing_fields:
            raise HTTPException(
                status_code=400,
                detail=f"Metadata file missing required fields: {', '.join(missing_fields)}. "
                f"Required: {', '.join(required_fields)}",
            )

        # Validate ref_freq is numeric
        try:
            float(metadata["ref_freq"])
        except (ValueError, TypeError) as exc:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid ref_freq value: {metadata.get('ref_freq')}. " "Must be numeric (Hz)."
                ),
            ) from exc

        # Store parsed metadata
        upload_status.metadata = metadata
        logger.info(
            "Parsed and validated metadata file %s (version: %s, catalogue: %s)",
            file.filename,
            metadata["version"],
            metadata["catalogue_name"],
        )

    async def save_csv_file(self, file: UploadFile, upload_status: UploadStatus) -> None:
        """
        Save a CSV file to memory after validating structure.

        Parameters
        ----------
        file : UploadFile
            CSV file to save
        upload_status : UploadStatus
            Upload status tracking object

        Raises
        ------
        HTTPException
            If file content is not valid CSV
        """
        contents = await file.read()
        file_size = len(contents)

        # Validate CSV structure
        try:
            text_content = contents.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"File {file.filename} is not valid UTF-8 text. "
                f"CSV files must be text-based.",
            ) from exc

        try:
            csv_reader = csv.reader(io.StringIO(text_content), strict=True)
            rows = list(csv_reader)

            if not rows:
                raise HTTPException(
                    status_code=400,
                    detail=f"File {file.filename} is empty. Must contain header and data rows.",
                )

            if len(rows) < 2:
                raise HTTPException(
                    status_code=400,
                    detail=f"File {file.filename} has no data rows. "
                    f"Must contain at least one header row and one data row.",
                )

            # Validate header row is not empty
            header = rows[0]
            if not header or all(cell.strip() == "" for cell in header):
                raise HTTPException(
                    status_code=400,
                    detail=f"File {file.filename} has empty header row. "
                    f"First row must contain column names.",
                )

        except csv.Error as e:
            raise HTTPException(
                status_code=400,
                detail=f"File {file.filename} is not valid CSV: {str(e)}",
            ) from e

        upload_status.csv_files.append((file.filename, text_content))
        upload_status.uploaded_csv_files += 1

        logger.info(
            "Validated and stored CSV file %s (%d bytes, %d rows) in memory",
            file.filename,
            file_size,
            len(rows),
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
            self._uploads[upload_id].csv_files.clear()
            # self._uploads[upload_id].metadata = None
            logger.info("Cleaned up upload %s from memory", upload_id)

    def get_files(self, upload_id: str) -> list[tuple[str, str]]:
        """
        Get all CSV files for an upload.

        Parameters
        ----------
        upload_id : str
            Upload identifier

        Returns
        -------
        list[tuple[str, str]]
            List of (filename, content) tuples where content is string
        """
        status = self.get_status(upload_id)
        return status.csv_files
