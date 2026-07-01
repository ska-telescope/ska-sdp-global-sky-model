"""
Upload manager for handling batch file uploads and tracking.

This module provides functionality for managing batch uploads of sky survey data,
including file validation, in-memory storage, metadata parsing, and upload state tracking.
"""

import csv
import io
import json
import logging
from datetime import datetime, timedelta
from enum import Enum
from uuid import uuid4

import sqlalchemy
from fastapi import HTTPException, UploadFile
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from ska_sdp_global_sky_model.api.app.models import (
    GlobalSkyModelMetadata,
    SkyComponent,
    SkyComponentStaging,
    UploadTaskState,
)
from ska_sdp_global_sky_model.configuration.config import CATALOGUE_CLEANUP_AGE

logger = logging.getLogger(__name__)


class UploadState(str, Enum):
    """Upload states for a sky survey batch upload."""

    PENDING = "pending"
    UPLOADING = "uploading"
    STAGED = "staged"
    FAILED = "failed"
    COMPLETED = "completed"


class UploadTask:
    """A wrapper for some of the objects that are required for an upload"""

    def __init__(self, catalogue_metadata: GlobalSkyModelMetadata, task_status: UploadTaskState):
        self.catalogue_metadata = catalogue_metadata
        self.task_status = task_status
        self.files = []

    @classmethod
    def fetch_from_db(cls, db: Session, upload_id: str) -> "UploadTask":
        """Attempt to fetch the task from the database"""

        task_status = db.scalars(
            select(UploadTaskState).where(UploadTaskState.upload_id == upload_id)
        ).first()

        catalogue_metadata = db.scalars(
            select(GlobalSkyModelMetadata).where(GlobalSkyModelMetadata.upload_id == upload_id)
        ).first()

        if task_status is None and catalogue_metadata is None:
            raise HTTPException(status_code=404, detail="Upload ID not found")

        return UploadTask.create(db, catalogue_metadata, task_status, set_upload_id=False)

    @classmethod
    def create(
        cls,
        db: Session,
        catalogue_metadata: GlobalSkyModelMetadata | None,
        task_status: UploadTaskState | None = None,
        set_upload_id: bool = True,
    ) -> "UploadTask":
        """Create the helper object instance"""
        if catalogue_metadata is not None:
            if set_upload_id:
                catalogue_metadata.upload_id = str(uuid4())
            if task_status is None:
                task_status = UploadTaskState(
                    upload_id=catalogue_metadata.upload_id,
                    status=(
                        UploadState.PENDING
                        if catalogue_metadata.staging
                        else UploadState.COMPLETED
                    ),
                )
                db.add(task_status)

        return UploadTask(catalogue_metadata, task_status)

    @property
    def upload_id(self):
        """Get the upload ID of the task"""
        return self.task_status.upload_id

    @property
    def name(self):
        """Get the catalogue name"""
        return self.catalogue_metadata.catalogue_name

    @property
    def status(self) -> tuple[str, str | None]:
        """Get the current status of the upload"""
        return self.task_status.status

    @property
    def is_uploaded(self) -> bool:
        """Check if the status is completely uploaded"""
        return self.task_status.status == UploadState.STAGED.value

    def to_dict(self) -> dict:
        """Return this object as a dictionary"""
        file_count = self.task_status.files_uploaded or 0
        return {
            "upload_id": self.upload_id,
            "state": self.task_status.status,
            "errors": (self.task_status.reason or "").split(","),
            "total_csv_files": file_count,
            "uploaded_csv_files": 0,
            "remaining_csv_files": file_count,
            "has_metadata": True,
            "metadata": (
                self.catalogue_metadata.columns_to_dict()
                if self.catalogue_metadata is not None
                else None
            ),
            "last_update": self.task_status.last_update,
        }

    async def add_file(self, file: UploadFile):
        """
        Save a CSV file to memory after validating structure.

        Parameters
        ----------
        file : UploadFile
            CSV file to save

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

        self.files.append((file.filename, text_content))
        if self.task_status.files_uploaded is None:
            self.task_status.files_uploaded = 0
        self.task_status.files_uploaded += 1

        logger.info(
            "Validated and stored CSV file %s (%d bytes, %d rows) in memory",
            file.filename,
            file_size,
            len(rows),
        )

    def update_status(self, status, message: str | None = None):
        """Update task status and time"""
        self.task_status.status = status
        if message is not None:
            if self.task_status.reason is None or len(self.task_status.reason) < 2:
                self.task_status.reason = message
            else:
                self.task_status.reason = f"{self.task_status.reason}, {message}"
        self.task_status.last_update = datetime.now()

    def mark_released(self):
        """Mark this catalogue as released"""
        self.update_status(UploadState.COMPLETED)

    def mark_uploading(self):
        """Mark this catalogue as uploading"""
        self.update_status(UploadState.UPLOADING)

    def mark_uploaded(self):
        """Mark this catalogue as uploaded"""
        self.update_status(UploadState.STAGED)

    def mark_failed(self, error_message: str):
        """Mark this catalogue as failed"""
        self.update_status(UploadState.FAILED, error_message)


async def load_metadata_from_json(metadata_file: UploadFile) -> GlobalSkyModelMetadata:
    """
    Validate the provided metadata JSON file and
    load content into the GlobalSkyModelMetadata object.

    The following are hardcode in this function, becasue they
    are updated later in other parts of the code:

    staging=True
    upload_id="upload_id_placeholder"
    version=None

    Parameters
    ----------
    metadata_file: single UploadFile-type object with metadata content
        expecetd: JSON format, utf-8 encoding

    Returns
    -------
    catalogue_metadat: GlobalSkyModelMetadata object with metadata
        loaded from input file
    """

    try:
        metadata_content = await metadata_file.read()
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to read metadata file {metadata_file.filename}: {exc}",
        ) from exc

    try:
        metadata = json.loads(metadata_content.decode("utf-8"))
    except UnicodeDecodeError as exc:
        logger.error("Metadata file %s is not valid UTF-8 text: %s", metadata_file.filename, exc)
        raise HTTPException(
            status_code=400,
            detail=f"Metadata file {metadata_file.filename} is not valid UTF-8 text: {exc}",
        ) from exc
    except json.JSONDecodeError as exc:
        logger.error("Metadata file %s is not valid JSON: %s", metadata_file.filename, exc)
        raise HTTPException(
            status_code=400,
            detail=f"Metadata file {metadata_file.filename} is not valid JSON: {exc}",
        ) from exc

    # Version is not accepted from metadata - it is auto-assigned per catalogue at commit time.
    catalogue_metadata = GlobalSkyModelMetadata(
        version=None,
        catalogue_name=metadata.get("catalogue_name", "UPLOAD").strip(),
        description=metadata.get("description", ""),
        upload_id="upload_id_placeholder",  # Will be set after creating upload status
        author=metadata.get("author"),
        reference=metadata.get("reference"),
        notes=metadata.get("notes"),
        staging=True,
    )
    return catalogue_metadata


def run_db_cleanup(
    db: sqlalchemy.orm.Session, do_delete: bool = False, override_cleanup_age: int | None = None
):
    """Do a cleanup of old data in the DB, and attempt to cleanup this manager.

    This will remove only catalogues that are staging=True, or
    components in the SkyComponentStaging table without a catalogue.

    There are also logs for partially migrated catalogues.

    `override_cleanup_age` - when set the function will use that number of hours
    instead of the default of `CATALOGUE_CLEANUP_AGE`. This is in hours"""

    _cleanup_old_uploads(db, override_cleanup_age)
    _cleanup_partial_migrations_and_orphaned_staging_components(db)
    if do_delete:
        logger.info("Committing any changes")
        db.commit()
    else:
        logger.info("Rolling back any changes")
        db.rollback()


def _cleanup_old_uploads(db: sqlalchemy.orm.Session, override_cleanup_age: int | None = None):
    """Remove catalogues and their components if they are still in staging
    and if the upload time is more than CATALOGUE_CLEANUP_AGE hours ago."""
    hours = override_cleanup_age or CATALOGUE_CLEANUP_AGE
    catalogues = db.scalars(
        select(GlobalSkyModelMetadata)
        .where(GlobalSkyModelMetadata.uploaded_at < datetime.now() - timedelta(hours=hours))
        .where(GlobalSkyModelMetadata.staging.is_(True))
    ).all()
    logger.info("Found %d catalogues to clean", len(catalogues))
    for catalogue in catalogues:
        logger.info(
            "Remove old catalogue: '%s/%s' (uploaded @ '%s')",
            catalogue.upload_id,
            catalogue.catalogue_name,
            catalogue.uploaded_at,
        )
        db.query(SkyComponentStaging).filter(SkyComponentStaging.gsm_id == catalogue.id).delete()
        db.delete(catalogue)


def _cleanup_partial_migrations_and_orphaned_staging_components(db: sqlalchemy.orm.Session):
    """Cleanup components in staging that are orphaned, and log
    partial migrations."""
    # Get all unique catalogues from staging
    upload_ids = db.scalars(select(SkyComponentStaging.upload_id).distinct()).all()
    logger.info("Found %d unique catalogue(s) in staging", len(upload_ids))
    for upload_id in upload_ids:
        logger.info("Checking upload ID: '%s'", upload_id)
        catalogues = db.scalars(
            select(GlobalSkyModelMetadata).filter(GlobalSkyModelMetadata.upload_id == upload_id)
        ).all()
        if len(catalogues) == 0:
            # -> if it doesn't exist in catalogue, delete it
            logger.info(" -> Has no catalogue (removing)")
            db.execute(
                delete(SkyComponentStaging).where(SkyComponentStaging.upload_id == upload_id)
            )
        else:
            logger.info(" -> Has an existing catalogue")
            for catalogue in catalogues:
                logger.info(
                    " -> Catalogue: %d:'%s' (uploaded @ '%s') (Staging:%s)",
                    catalogue.id,
                    catalogue.catalogue_name,
                    catalogue.uploaded_at,
                    "yes" if catalogue.staging else "no",
                )
                component_count = db.scalars(
                    # pylint: disable-next=not-callable
                    select(func.count(SkyComponent.id)).where(SkyComponent.gsm_id == catalogue.id)
                ).first()
                if component_count > 0:
                    if catalogue.staging:
                        logger.warning(" -> Partial migration of incomplete upload")
                    elif not catalogue.staging:
                        logger.warning(" -> Partial migration has left over data")
                else:
                    logger.info(
                        " -> there are no partially transferred components "
                        "(can be ignored for now)"
                    )
