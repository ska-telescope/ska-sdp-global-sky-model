"""
Upload manager for handling batch file uploads and tracking.

This module provides functionality for managing batch uploads of sky survey data,
including file validation, in-memory storage, metadata parsing, and upload state tracking.
"""

import csv
import io
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from uuid import uuid4

import sqlalchemy
from fastapi import HTTPException, UploadFile

from ska_sdp_global_sky_model.api.app.models import (
    GlobalSkyModelMetadata,
    SkyComponent,
    SkyComponentStaging,
)
from ska_sdp_global_sky_model.configuration.config import CATALOGUE_CLEANUP_AGE

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
    metadata: GlobalSkyModelMetadata
    uploaded_csv_files: int = 0
    state: UploadState = UploadState.PENDING
    errors: list[str] = field(default_factory=list)
    csv_files: list[tuple[str, str]] = field(default_factory=list)  # (filename, content as str)

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

    def create_upload(self, csv_file_count: int, metadata: GlobalSkyModelMetadata) -> UploadStatus:
        """
        Create a new upload tracking entry.

        Parameters
        ----------
        csv_file_count : int
            Number of CSV files in the batch
        metadata : GlobalSkyModelMetadata
            Metadata for the upload

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
            metadata=metadata,
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

    def get_all_statuses(self):
        """Get all upload statuses"""
        return [upload.to_dict() for upload_id, upload in self._uploads.items()]

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
            rows = []
            header = []

            # Parse each line and check they have the same number of fields.
            for row_index, row in enumerate(csv_reader):
                if row_index == 0:
                    header = row
                if len(row) != len(header):
                    raise HTTPException(
                        status_code=400,
                        detail=f"File {file.filename} has an inconsistent "
                        f"number of fields at line {csv_reader.line_num}: "
                        f"expected {len(header)} fields, "
                        f"found {len(row)} fields "
                        f"(header={header}, record={row}.",
                    )
                rows.append(row)

            # Check for empty file.
            if not rows:
                raise HTTPException(
                    status_code=400,
                    detail=f"File {file.filename} is empty. "
                    f"Must contain header and data rows.",
                )

            # Check for missing data.
            if len(rows) < 2:
                raise HTTPException(
                    status_code=400,
                    detail=f"File {file.filename} has no data rows. "
                    f"Must contain at least one header row and one data row.",
                )

            # Validate header row is not empty
            if not header or all(cell.strip() == "" for cell in header):
                raise HTTPException(
                    status_code=400,
                    detail=f"File {file.filename} has empty header row. "
                    f"First row must contain column names.",
                )

        except csv.Error as e:
            # Get the line number where CSV parsing failed, adjusted if needed.
            line_num = csv_reader.line_num
            error_msg = str(e)
            if "unexpected end of data" not in error_msg.lower():
                line_num -= 1

            # Raise HTTP exception with detailed error message.
            raise HTTPException(
                status_code=400,
                detail=f"File {file.filename} is not valid CSV "
                f"at line {line_num}: {error_msg}. Check closing quotes.",
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

    def run_db_cleanup(self, db: sqlalchemy.orm.Session):
        """Do a cleanup of old data in the DB, and cleanup manager"""

        self._cleanup_old_uploads(db)
        self._cleanup_partial_migrations(db)
        db.commit()

    def _cleanup_old_uploads(self, db: sqlalchemy.orm.Session):
        # Get all catalogues where the upload time is old and staging is true
        catalogues = (
            db.query(GlobalSkyModelMetadata)
            .filter(
                GlobalSkyModelMetadata.uploaded_at
                < datetime.now() - timedelta(hours=CATALOGUE_CLEANUP_AGE)
            )
            .filter(GlobalSkyModelMetadata.staging.is_(True))
            .all()
        )
        # -> remove catalogue and components
        for catalogue in catalogues:
            logger.info(
                "Found old catalogue: '%s/%s' (uploaded @ '%s')",
                catalogue.upload_id,
                catalogue.catalogue_name,
                catalogue.uploaded_at,
            )
            self.cleanup(catalogue.upload_id)
            db.query(SkyComponentStaging).filter(
                SkyComponentStaging.gsm_id == catalogue.id
            ).delete()
            db.delete(catalogue)

    def _cleanup_partial_migrations(self, db: sqlalchemy.orm.Session):
        # Get all unique catalogues from staging
        upload_ids = db.query(SkyComponentStaging.upload_id).distinct().all()
        for upload_id_row in upload_ids:
            upload_id = upload_id_row[0]
            logger.debug("Found upload ID: '%s'", upload_id)
            catalogues = (
                db.query(GlobalSkyModelMetadata)
                .filter(GlobalSkyModelMetadata.upload_id == upload_id)
                .all()
            )
            if len(catalogues) == 0:
                # -> if it doesn't exist in catalogue, delete it
                logger.info("Upload ID has no catalogue: '%s'", upload_id)
                self.cleanup(upload_id)
                db.query(SkyComponentStaging).filter(
                    SkyComponentStaging.upload_id == upload_id
                ).delete()
            else:
                for catalogue in catalogues:
                    component_count = (
                        db.query(SkyComponent).filter(SkyComponent.gsm_id == catalogue.id).count()
                    )
                    if component_count > 0:
                        if catalogue.staging:
                            logger.warning(
                                "Partial migration of incomplete upload: '%s'", upload_id
                            )
                        elif not catalogue.staging:
                            logger.warning("Partial migration has left over data: '%s'", upload_id)
