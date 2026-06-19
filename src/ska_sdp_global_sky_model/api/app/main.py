"""
A simple fastAPI to ingest data into the global sky model database and to obtain a local sky
model from it.
"""

# pylint: disable=broad-exception-caught

import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    File,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    RedirectResponse,
    Response,
)
from sqlalchemy import func, text
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from ska_sdp_global_sky_model.api.app.ingest import ingest_catalogue
from ska_sdp_global_sky_model.api.app.models import (
    GlobalSkyModelMetadata,
    SkyComponent,
    SkyComponentStaging,
    UploadTaskState,
)
from ska_sdp_global_sky_model.api.app.request_responder import (
    QueryParameters,
    lsm_to_csv_lines,
    lsm_to_ecsv_lines,
    sky_components_to_single_file,
    sky_components_to_tar,
    start_lsm_response_thread,
)
from ska_sdp_global_sky_model.api.app.upload_manager import UploadTask
from ska_sdp_global_sky_model.configuration.config import (
    API_URL,
    engine,
    get_db,
    templates,
)
from ska_sdp_global_sky_model.utilities.query_helpers import QueryBuilder
from ska_sdp_global_sky_model.utilities.version_utils import (
    get_latest_version,
    increment_minor_version,
)

logger = logging.getLogger(__name__)


def wait_for_db():
    """Await DB connection."""
    while True:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            logger.info("Database is up and running!")
            break
        except Exception as e:
            logger.info("Database connection failed: %s", e)
            time.sleep(5)  # Wait before retrying


@asynccontextmanager
async def lifespan(fast_api_app: FastAPI):  # pylint: disable=unused-argument
    """
    Lifespan context manager for FastAPI application startup and shutdown.
    """
    # Startup
    logger.info("Starting application...")
    wait_for_db()
    start_lsm_response_thread()
    logger.info("Application startup complete")

    yield

    # Shutdown
    logger.info("Shutting down application...")


app = FastAPI(lifespan=lifespan, root_path=API_URL)

app.add_middleware(GZipMiddleware, minimum_size=1000)

origins = []

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def home_docs():
    """Redirect from / to /docs."""
    return RedirectResponse(url=f"{API_URL}/docs")


@app.get(
    "/ping",
    summary="Ping the API",
    responses={200: {"content": {"application/json": {"example": {"ping": "live"}}}}},
)
def ping():
    """Returns {"ping": "live"} when called"""
    logger.debug("Ping: alive")
    return {"ping": "live"}


@app.get(
    "/upload",
    response_class=HTMLResponse,
    summary="Browser upload interface",
    responses={200: {"content": {"text/html": {"example": "Upload Form."}}}},
)
def upload_interface():
    """Serve the HTML upload interface"""
    upload_page = Path(__file__).parent / "static" / "upload.html"
    if upload_page.exists():
        return FileResponse(upload_page)
    return {"message": "Upload interface not available. Use API endpoints directly."}


@app.get(
    "/components",
    response_class=HTMLResponse,
    summary="See all components",
    responses={200: {"content": {"text/html": {"example": "Id | Version |Catalogue_name |..."}}}},
)
def get_point_components(request: Request, db: Session = Depends(get_db)):
    """Retrieve all components from database."""
    logger.info("Retrieving all components...")
    components = (
        db.query(SkyComponent, GlobalSkyModelMetadata)
        .filter(SkyComponent.gsm_id == GlobalSkyModelMetadata.id)
        .all()
    )
    output_rows = [r[1].columns_to_dict() | r[0].columns_to_dict() for r in components]
    for row in output_rows:
        del row["gsm_id"]
        del row["upload_id"]
        del row["staging"]
    logger.info("Retrieved all data for all %d components", len(output_rows))
    return templates.TemplateResponse(
        request=request,
        name="table.html",
        context={"items": list(output_rows), "title": "Component list"},
    )


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
@app.get(
    "/local-sky-model",
    summary="Retrieve a local sky model",
    description="Retrieve a sub-set of the global sky model in the form of a local sky model.",
    responses={
        200: {
            "content": {
                "text/html": {"example": "Id | Version | Catalogue_name |..."},
                "text/csv": {"example": "component_id,ra_deg,dec_deg,..."},
            }
        }
    },
)
async def get_local_sky_model_endpoint(
    request: Request,
    ra_deg: float,
    dec_deg: float,
    fov_deg: float,
    page: int = Query(default=1, ge=1, description="Page number"),
    page_size: int = Query(default=50, ge=1, le=10000, description="Results per page"),
    output_format: str = Query(
        default="html", alias="format", description="Output format: 'html', 'csv', or 'ecsv'"
    ),
    db: Session = Depends(get_db),
):
    """
    Get the local sky model from a global sky model.

    Args:
        request (Request): HTTP request object.
        ra_deg (float): Right ascension of the observation point in degrees.
        dec_deg (float): Declination of the observation point in degrees.
        fov_deg (float): Field of view of the telescope in degrees.
        page (int): Page number for pagination (1-indexed).
        page_size (int): Number of results per page.
        output_format (str): Response format, 'html' (default), 'csv', or 'ecsv'.
        db (Session): Database session object.

    Returns:
        HTML table or CSV file of the local sky model.
    """
    query_parameters = dict(request.query_params)
    for param in ["ra_deg", "dec_deg", "fov_deg", "page", "page_size", "format"]:
        query_parameters.pop(param, None)
    logger.info(
        "Requesting local sky model: ra:%s, dec:%s, fov:%s, page:%s, page_size:%s, "
        "format:%s, other:%s",
        ra_deg,
        dec_deg,
        fov_deg,
        page,
        page_size,
        output_format,
        query_parameters,
    )
    query_params = QueryParameters(ra_deg, dec_deg, fov_deg, **query_parameters)
    catalogues = query_params.sky_components(db)

    if output_format == "csv":
        if len(catalogues) == 1:
            filename, content = sky_components_to_single_file(
                catalogues, query_params, "csv", lsm_to_csv_lines
            )
            return Response(
                content=content,
                media_type="text/csv",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )
        return Response(
            content=sky_components_to_tar(catalogues, query_params, "csv", lsm_to_csv_lines),
            media_type="application/x-tar",
            headers={"Content-Disposition": 'attachment; filename="local_sky_model_csv.tar"'},
        )
    if output_format == "ecsv":
        if len(catalogues) == 1:
            filename, content = sky_components_to_single_file(
                catalogues, query_params, "ecsv", lsm_to_ecsv_lines
            )
            return Response(
                content=content,
                media_type="text/plain",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )
        return Response(
            content=sky_components_to_tar(catalogues, query_params, "ecsv", lsm_to_ecsv_lines),
            media_type="application/x-tar",
            headers={"Content-Disposition": 'attachment; filename="local_sky_model_ecsv.tar"'},
        )
    all_rows = []
    for catalogue, components in catalogues:
        catalogue_dict = catalogue.columns_to_dict()
        all_rows.extend([catalogue_dict | component.columns_to_dict() for component in components])
    for row in all_rows:
        del row["gsm_id"]
        del row["id"]
        del row["staging"]

    total_count = len(all_rows)
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    offset = (page - 1) * page_size
    output_rows = all_rows[offset : offset + page_size]  # noqa: E203

    return templates.TemplateResponse(
        request=request,
        name="table.html",
        context={
            "items": output_rows,
            "title": "Local Sky Model Search",
            "page": page,
            "page_size": page_size,
            "total_count": total_count,
            "total_pages": total_pages,
        },
    )


def _run_ingestion_task(upload_task: UploadTask, db: Session = None):
    """
    Run ingestion task in background to staging table.

    This function runs in a separate thread/task to keep the API responsive
    while processing large file uploads. Data is ingested to staging table
    and requires manual commit to move to main table.

    Parameters
    ----------
    upload_id : str
        Upload identifier for tracking
    catalogue_metadata : GlobalSkyModelMetadata
        Catalogue metadata for ingestion
    """
    upload_id = upload_task.upload_id
    try:
        upload_task.catalogue_metadata.staging = True
        upload_task.mark_uploading()
        db.add(upload_task.catalogue_metadata)
        db.commit()

        # Get files from memory
        files_data = upload_task.files

        # Ingest all files from memory to staging table
        for file, content in files_data:
            logger.info("Processing file: '%s'", file)
            # Deep copy to avoid modifying shared metadata
            # Pass content directly
            catalogue_content_files = {"ingest": {"file_location": [{"content": content}]}}
            # Set staging flag and upload_id for tracking

            if not ingest_catalogue(db, upload_task.catalogue_metadata, catalogue_content_files):
                raise RuntimeError(f"Ingest failed for {file}")

        # Mark as completed
        upload_task.mark_uploaded()
        logger.info("Background ingestion to staging completed for upload %s", upload_id)

    except Exception as e:
        error_msg = str(e)
        logger.error("Background ingestion failed for upload %s: %s", upload_id, error_msg)
        logger.exception("Full traceback for upload %s:", upload_id)

        if db:
            try:
                db.rollback()
                db.query(SkyComponentStaging).filter(
                    SkyComponentStaging.upload_id == upload_id
                ).delete()
                db.query(GlobalSkyModelMetadata).filter(
                    GlobalSkyModelMetadata.upload_id == upload_id
                ).delete()
                logger.info("Cleared staged records for failed upload %s", upload_id)
            except Exception as cleanup_error:
                db.rollback()
                logger.error(
                    "Failed to clear staged records for upload %s: %s",
                    upload_id,
                    cleanup_error,
                )
            upload_task.mark_failed(error_msg)

    finally:
        if db:
            db.commit()
            db.close()


@app.post(
    "/upload-sky-survey-batch",
    summary="Upload sky survey CSV files with catalogue metadata",
    description="Upload catalogue metadata file and CSV files for staging. "
    "Ingestion runs asynchronously - use the status endpoint to monitor progress.",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "upload_id": "catalogue_metadata.upload_id",
                        "status": "uploading",
                        "catalogue_name": "catalogue_metadata.catalogue_name",
                        "message": (
                            "Uploaded {count} CSV file(s) with metadata. "
                            "Data is being ingested into the staging table, prior to "
                            "being  committed to the main table."
                        ),
                        "next_action": "poll_status",
                    }
                }
            }
        }
    },
)
async def upload_sky_survey_batch(
    background_tasks: BackgroundTasks,
    metadata_file: list[UploadFile] = File(..., description="One catalogue metadata JSON file"),
    csv_files: list[UploadFile] = File(..., description="One or more CSV files"),
    db: Session = Depends(get_db),
):
    """
    Upload catalogue metadata and CSV files for staging.

    Requires a metadata.json file containing catalogue information, plus one or more CSV
    files with component data.

    Parameters
    ----------
    background_tasks : BackgroundTasks
        FastAPI background task manager
    metadata_file : list[UploadFile]
        A JSON file with catalogue metadata (catalogue_name, description, etc.)
    csv_files : list[UploadFile]
        One or more CSV files containing component data
    db: database Session (automatically generated)

    Raises
    ------
    HTTPException
        If validation fails

    Returns
    -------
    dict
        Upload identifier and status
    """
    # Check we have a single metadata JSON file, and at least one CSV file.
    if not csv_files:
        raise HTTPException(status_code=400, detail="No CSV files provided")
    if len(metadata_file) != 1:
        raise HTTPException(
            status_code=400,
            detail="There must be one metadata JSON file",
        )

    try:
        metadata_file_contents = await metadata_file[0].read()
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to read metadata file {metadata_file[0].filename}: {exc}",
        ) from exc

    # Validate CSV structure
    try:
        metadata = json.loads(metadata_file_contents.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"File {metadata_file[0].filename} is not valid UTF-8 text. "
            f"CSV files must be text-based.",
        ) from exc
    except json.JSONDecodeError as exc:
        logger.error("Metadata file %s is not valid JSON: %s", metadata_file[0].filename, exc)
        raise HTTPException(
            status_code=400,
            detail=f"Metadata file {metadata_file[0].filename} is not valid JSON: {exc}",
        ) from exc

    # Version is not accepted from metadata - it is auto-assigned per catalogue at commit time.
    catalogue_metadata = GlobalSkyModelMetadata(
        version=None,
        catalogue_name=metadata.get("catalogue_name", "UPLOAD"),
        description=metadata.get("description", ""),
        upload_id="upload_id_placeholder",  # Will be set after creating upload status
        author=metadata.get("author"),
        reference=metadata.get("reference"),
        notes=metadata.get("notes"),
        staging=True,
    )

    # Create upload tracking
    upload_task = UploadTask.create(db, catalogue_metadata)

    try:
        logger.info(
            "Received upload with metadata: catalogue_name=%s, upload_id=%s",
            catalogue_metadata.catalogue_name,
            catalogue_metadata.upload_id,
        )

        # Validate and save CSV files
        for file in csv_files:
            await upload_task.add_file(file)

        # Schedule ingestion to run in background
        background_tasks.add_task(_run_ingestion_task, upload_task, db)

        logger.info(
            "Upload %s: metadata and %d CSV files saved, ingestion scheduled",
            catalogue_metadata.upload_id,
            len(csv_files),
        )

        return {
            "upload_id": catalogue_metadata.upload_id,
            "status": "uploading",
            "catalogue_name": catalogue_metadata.catalogue_name,
            "message": (
                f"Uploaded {len(csv_files)} CSV file(s) with metadata. "
                "Data is being ingested into the staging table, prior to "
                "being  committed to the main table."
            ),
            "next_action": "poll_status",
        }

    except HTTPException as exc:
        upload_task.mark_failed(str(exc.detail))
        raise

    except Exception as e:
        error_msg = str(e)
        upload_task.mark_failed(error_msg)
        raise HTTPException(
            status_code=500,
            detail=f"Sky survey upload failed: {error_msg}",
        ) from e


@app.get(
    "/upload-sky-survey-status/{upload_id}",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "upload_id": "upload_id",
                        "state": "state.value",
                        "total_csv_files": "total_csv_files",
                        "uploaded_csv_files": "uploaded_csv_files",
                        "remaining_csv_files": "total_csv_files - uploaded_csv_files",
                        "errors": "errors",
                        "has_metadata": True,
                        "metadata": "metadata",
                    }
                }
            }
        }
    },
)
def upload_sky_survey_status(upload_id: str, db: Session = Depends(get_db)):
    """
    Retrieve the current status of a sky survey upload.

    Parameters
    ----------
    upload_id : str
        Unique identifier returned when the upload was initiated.

    Raises
    ------
    HTTPException
        If the upload ID does not exist.

    Returns
    -------
    dict
        Upload progress, completion state, and error information.
    """
    status = UploadTask.fetch_from_db(db, upload_id)
    return status.to_dict()


@app.get(
    "/review-upload/{upload_id}",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "upload_id": "catalogue_metadata.upload_id",
                        "total_records": "count",
                        "sample_range": "sample_start-sample_end",
                        "sample": "sample",
                        "message": (
                            "Review complete. Data is still in staging and not visible in the "
                            "GSM until committed."
                        ),
                        "next_action": "commit",
                    }
                }
            }
        }
    },
)
def review_upload(upload_id: str, db: Session = Depends(get_db)):
    """
    Review staged data before committing to main database.

    Parameters
    ----------
    upload_id : str
        Upload identifier
    db : Session
        Database session

    Returns
    -------
    dict
        Sample of staged data and statistics
    """
    task = UploadTask.fetch_from_db(db, upload_id)

    if not task.is_uploaded:
        raise HTTPException(
            status_code=400, detail=f"Upload not ready for review. Current state: {task.status}"
        )

    # Get count and sample of staged data
    count = (
        # pylint: disable-next=not-callable
        db.query(func.count(SkyComponentStaging.id))
        .filter(SkyComponentStaging.upload_id == upload_id)
        .scalar()
    )

    # Get last 10 rows as sample to confirm all data loaded
    sample = (
        db.query(SkyComponentStaging)
        .filter(SkyComponentStaging.upload_id == upload_id)
        .order_by(SkyComponentStaging.id.desc())
        .limit(10)
        .all()
    )

    # Reverse to show in ascending order and calculate positions
    sample.reverse()

    # Calculate the starting position
    sample_start = max(1, count - len(sample) + 1)
    sample_end = count

    response = {
        "upload_id": upload_id,
        "total_records": count,
        "sample_range": f"{sample_start}-{sample_end}",
        "sample": [row.columns_to_dict() for row in sample],
        "message": (
            "Review complete. Data is still in staging and not visible in the "
            "GSM until committed."
        ),
        "next_action": "commit",
    }
    # Add metadata details if available
    response["metadata"] = task.catalogue_metadata
    return response


@app.post(
    "/commit-upload/{upload_id}",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "message": (
                            "Committed {record count} components from catalogue {catalogue_name}"
                        ),
                        "records_committed": "count",
                        "version": "catalogue_version",
                        "catalogue_name": "catalogue_name",
                    }
                }
            }
        }
    },
)
def commit_upload(upload_id: str, db: Session = Depends(get_db)):
    """
    Commit staged data to main database with catalogue-level versioning.

    Creates a GlobalSkyModelMetadata record and copies all components from staging
    to the main table with the catalogue version.

    Parameters
    ----------
    upload_id : str
        Upload identifier
    db : Session
        Database session

    Returns
    -------
    dict
        Result of commit operation including version and catalogue info

    Raises
    ------
    HTTPException
        If upload not ready, no metadata, or commit fails
    """
    task = UploadTask.fetch_from_db(db, upload_id)

    if not task.is_uploaded:
        raise HTTPException(
            status_code=400, detail=f"Upload not ready for commit. Current state: {task.status}"
        )

    try:
        # Get all staged records
        staged_records = (
            db.query(SkyComponentStaging).filter(SkyComponentStaging.upload_id == upload_id).all()
        )

        if not staged_records:
            raise HTTPException(status_code=404, detail="No staged data found")

        # Commit any possible changes, so that we can start a new transaction
        task.update_status("finalising")

        db.commit()
        # Auto-compute the next version for this catalogue by incrementing the minor version
        # of the current latest committed version, independently per catalogue name.
        # We only commit new version here
        while True:
            try:
                # Transaction starting in loop, in case of version conflicts
                with db.begin():
                    existing_versions = [
                        versions[0]
                        for versions in db.query(GlobalSkyModelMetadata.version)
                        .filter(GlobalSkyModelMetadata.catalogue_name == task.name)
                        .filter(GlobalSkyModelMetadata.version.isnot(None))
                        .all()
                    ]
                    latest_version = get_latest_version(existing_versions)
                    catalogue_version = increment_minor_version(latest_version)
                    task.catalogue_metadata.version = catalogue_version

                    logger.debug("Attempting to set the version to %s", catalogue_version)

                    db.commit()

                break
            except IntegrityError:
                logger.error("Version update failed, trying again...")

        logger.info(
            "Auto-assigned version %s to upload %s for catalogue '%s' (previous latest: %s)",
            catalogue_version,
            upload_id,
            task.name,
            latest_version or "none",
        )

        # Copy from staging to main table with catalogue version
        for staged in staged_records:
            # Create main table record from staged data
            # Exclude 'id' and 'upload_id' from staging table fields
            record_data = {
                k: v for k, v in staged.columns_to_dict().items() if k not in ["id", "upload_id"]
            }

            main_record = SkyComponent(**record_data)
            db.add(main_record)

        task.catalogue_metadata.staging = False
        # Delete from staging
        db.query(SkyComponentStaging).filter(SkyComponentStaging.upload_id == upload_id).delete()

        task.mark_released()
        db.commit()

        logger.info(
            "Successfully committed upload %s: %d components with version %s",
            upload_id,
            len(staged_records),
            catalogue_version,
        )

        return {
            "status": "success",
            "message": (
                f"Committed {len(staged_records)} components from catalogue '{task.name}'"
            ),
            "records_committed": len(staged_records),
            "version": catalogue_version,
            "catalogue_name": task.name,
        }

    except Exception as e:
        db.rollback()
        # we need to rollback the version manually:
        try:
            task.catalogue_metadata.version = None
            db.commit()
        except Exception:
            pass
        logger.error("Failed to commit upload %s: %s", upload_id, e)
        logger.exception(e)
        raise HTTPException(status_code=500, detail=f"Commit failed: {str(e)}") from e


@app.delete(
    "/reject-upload/{upload_id}",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "message": "Rejected and deleted {count} staged records",
                        "records_deleted": "count",
                    }
                }
            }
        }
    },
)
def reject_upload(upload_id: str, db: Session = Depends(get_db)):
    """
    Reject and discard staged data.

    Parameters
    ----------
    upload_id : str
        Upload identifier
    db : Session
        Database session

    Returns
    -------
    dict
        Result of reject operation
    """
    task = UploadTask.fetch_from_db(db, upload_id)

    if not task.is_uploaded:
        raise HTTPException(
            status_code=400,
            detail=f"Upload not ready for rejection. Current state: {task.status}",
        )

    try:
        # Count records to be deleted
        count = (
            db.query(SkyComponentStaging)
            .filter(SkyComponentStaging.upload_id == upload_id)
            .count()
        )

        # Delete staged data
        db.query(SkyComponentStaging).filter(SkyComponentStaging.upload_id == upload_id).delete()
        db.query(GlobalSkyModelMetadata).filter(
            GlobalSkyModelMetadata.upload_id == upload_id
        ).delete()

        # Mark upload as failed
        task.mark_failed("Rejected by user")
        db.commit()

        logger.info("Rejected and deleted %d staged records for upload %s", count, upload_id)

        return {
            "status": "success",
            "message": f"Rejected and deleted {count} staged records",
            "records_deleted": count,
        }

    except Exception as e:
        db.rollback()
        logger.error("Failed to reject upload %s: %s", upload_id, e)
        raise HTTPException(status_code=500, detail=f"Reject failed: {str(e)}") from e


@app.get(
    "/catalogue-metadata",
    summary="Query catalogue metadata",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": [
                        {"id": 1, "version": "0.1.0", "catalogue_name": "TEST_CATALOGUE_1"}
                    ]
                }
            }
        }
    },
)
def query_gsm_metadata(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Query catalogue metadata records.

    Search by generic fields in the table (e.g catalogue name, version), or list all catalogues.
    Supports operators for filtering responses.
    Results are ordered by upload date (newest first).

    Parameters
    ----------
    catalogue_name : str, optional
        Filter by catalogue name (case-insensitive partial match)
    version : str, optional
        Filter by exact version
    limit : int, default 100
        Maximum number of results to return
    db : Session
        Database session

    Returns
    -------
    dict
        List of catalogue metadata records
    """

    return QueryBuilder(GlobalSkyModelMetadata, request.query_params).query(db)


@app.get(
    "/catalogue-metadata/{catalogue_id}",
    summary="Get specific catalogue metadata",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {"id": 1, "version": "0.1.0", "catalogue_name": "TEST_CATALOGUE_1"}
                }
            }
        }
    },
)
def get_catalogue_metadata_by_id(
    catalogue_id: int,
    db: Session = Depends(get_db),
):
    """
    Get catalogue metadata by ID.

    Parameters
    ----------
    catalogue_id : int
        catalogue metadata ID
    db : Session
        Database session

    Returns
    -------
    dict
        catalogue metadata record

    Raises
    ------
    HTTPException
        If catalogue not found
    """
    catalogue = (
        db.query(GlobalSkyModelMetadata).filter(GlobalSkyModelMetadata.id == catalogue_id).first()
    )

    if not catalogue:
        raise HTTPException(status_code=404, detail=f"catalogue with ID {catalogue_id} not found")

    return catalogue.to_dict()


@app.get("/uploads", summary="Get list of current uploads")
def list_uploads(request: Request, db: Session = Depends(get_db)):
    """List all uploads done, and their state"""
    # SQLAlchemy's outerjoin is always a left outer join
    stmt = select(UploadTaskState, GlobalSkyModelMetadata).join(
        GlobalSkyModelMetadata,
        UploadTaskState.upload_id == GlobalSkyModelMetadata.upload_id,
        full=True,
    )

    results = db.execute(stmt).all()
    output = []
    for result in results:
        task = {}
        metadata = {}

        if result[0] is None:
            # pylint: disable-next=no-member
            task = {key: None for key in UploadTaskState.__table__.columns.keys()}
            task["status"] = "unknown"
            task["reason"] = "Didn't use the upload API"
        else:
            task = result[0].columns_to_dict()

        if result[1] is None:
            # pylint: disable-next=no-member
            metadata = {key: None for key in GlobalSkyModelMetadata.__table__.columns.keys()}

            # Pop to remove bad None
            metadata.pop("upload_id")
        else:
            metadata = result[1].columns_to_dict()

        ids = {"task_id": task.pop("id"), "catalogue_id": metadata.pop("id")}

        output.append(ids | task | metadata)

    return templates.TemplateResponse(
        request=request,
        name="table.html",
        context={"items": list(output), "title": "Upload list"},
    )
