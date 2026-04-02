"""Config DB listener.

This module watches for flow entries that request for a Local Sky Model.

Requirements:

- Flow.sink: ``DataProduct``
- Flow.sink.data_dir: ``PVCPath``
- Flow.sources[*].function: ``GlobalSkyModel.RequestLocalSkyModel``
- Flow.sources:
    - 1 source
    - parameters contain: ``ra,dec,fov`` and optionally ``version``

The states are updated as follows:

- When starting the processing: ``FLOWING``
- When successfully completed: ``COMPLETED``
- When failed: ``FAILED``, with a ``reason`` field
"""

import logging
import threading
import time
from collections.abc import Generator
from pathlib import Path

import numpy as np
import ska_sdp_config
from fastapi import Depends
from packaging.version import Version
from ska_sdp_config.entity.flow import Flow, FlowSource
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    GlobalSkyModel,
)
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    SkyComponent as SkyComponentDataclass,
)
from ska_sdp_datamodels.global_sky_model.local_sky_model import LocalSkyModel
from sqlalchemy.orm import Session

from ska_sdp_global_sky_model.api.app.crud import q3c_radial_query
from ska_sdp_global_sky_model.api.app.models import GlobalSkyModelMetadata, SkyComponent
from ska_sdp_global_sky_model.configuration.config import (
    REQUEST_WATCHER_TIMEOUT,
    SHARED_VOLUME_MOUNT,
    get_db,
    resource_toggle,
)
from ska_sdp_global_sky_model.utilities.local_sky_model import save_lsm_with_metadata
from ska_sdp_global_sky_model.utilities.query_helpers import QueryBuilder

logger = logging.getLogger(__name__)


# pylint: disable=too-many-arguments,too-many-positional-arguments
class QueryParameters:
    """This class manages the query parameters and
    helper methods which are used to query the database."""

    def __init__(
        self,
        ra_deg: float,
        dec_deg: float,
        fov_deg: float,
        catalogue_name: str,
        version: str = "latest",
        **query_parameters,
    ):
        """Init method setting the query parameters

        Args:
            - ra_deg: The ra component of the fov
            - dec_deg: The dec component of the fov
            - fov_deg: The field of view
            - catalogue_name: The catalogue the components should be selected from
            - version: The version components should be selected from
            - query_parameters: All additional query parameters possibly including __ operators

        """
        self.ra_deg = ra_deg
        self.dec_deg = dec_deg
        self.fov_deg = fov_deg
        self.version = version
        self.catalogue_name = catalogue_name
        self.component_queries = {}
        self.metadata_queries = {}
        for key, value in query_parameters.items():
            if "__" in key:
                field = key.split("__")[0]
            else:
                field = key
            if field in SkyComponentDataclass.__annotations__.keys():
                self.component_queries[key] = value
            elif key in GlobalSkyModelMetadata.__annotations__.keys():
                self.metadata_queries[key] = value
            else:
                logger.warning("The QueryParameter %s=%s was not valid", key, value)

    def sky_components(self, db) -> list[SkyComponent]:
        """Get the sky components based on the query parameters
        Args:
            - db: The database handle

        Returns:
            - The query response of SkyComponents
        """
        # Query components within the field of view using spatial index
        # pylint: disable=no-member,duplicate-code
        sky_components_query = (
            db.query(SkyComponent)
            .where(
                q3c_radial_query(
                    SkyComponent.ra_deg,
                    SkyComponent.dec_deg,
                    self.ra_deg,
                    self.dec_deg,
                    self.fov_deg,
                )
            )
            .where(SkyComponent.version == self.get_version(db))
            .where(SkyComponent.catalogue_name == self.catalogue_name)
        )
        query_builder = QueryBuilder(SkyComponent, self.component_queries)
        sky_components_query = query_builder.apply_filters(sky_components_query)
        return sky_components_query.all()

    def get_version(self, db: Session) -> str:
        """Get version. If was 'latest' set convert to latest semantic version in database.
        Args:
            - db: The database handle
        Returns:
            - The version string
        """
        if self.version is None or self.version != "latest":
            return self.version

        try:
            versions = (
                db.query(GlobalSkyModelMetadata.version)
                .where(GlobalSkyModelMetadata.catalogue_name == self.catalogue_name)
                .all()
            )
            if not versions:
                logger.error("No GSM versions available in database.")
                raise ValueError("No GSM versions available in database.")

            version_strings = [v[0] for v in versions]

            # Parse and sort semantically
            latest_version = max(version_strings, key=Version)

            return latest_version

        except Exception as e:
            logger.exception("Failed to resolve version: %s", e)
            raise

    def __eq__(self, other):
        """Check equality between query parameter classes."""
        # Check if the other object is an instance of the same class
        if not isinstance(other, QueryParameters):
            return NotImplemented

        return all(
            getattr(self, var) == getattr(other, var)
            for var in [
                "ra_deg",
                "dec_deg",
                "fov_deg",
                "version",
                "catalogue_name",
                "component_queries",
                "metadata_queries",
            ]
        )


def start_thread():  # pragma: no cover
    """Start the background thread that will search for flow entries"""
    thread = threading.Thread(target=_db_watcher, kwargs={}, daemon=True, name="Thread-Watcher")
    thread.start()


def _db_watcher():  # pragma: no cover
    """Outer watcher function"""
    while True:
        try:
            config = ska_sdp_config.Config()
            _watcher_process(config)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("An error has occurred, thread needs to restart")
            logger.exception(e)
        finally:
            try:
                config.close()
            except Exception:  # pylint: disable=broad-exception-caught
                # explicitly ignoring this error
                pass
        # This sleep is for in case something goes wrong, so that the
        # CPU/Network doesn't get overloaded.
        logger.debug("Sleeping before restart")
        time.sleep(1)


def _watcher_process(config: ska_sdp_config.Config.txn):
    """Main watcher process."""
    # This function should only end if an exception is thrown, and when
    # that happens the process should be restarted automatically.
    for watcher in config.watcher(timeout=REQUEST_WATCHER_TIMEOUT):
        for txn in watcher.txn():
            flows = list(_get_flows(txn))

        for flow, source in flows:

            logger.info("Found flow ... %s", flow.key)
            _watcher_process_flow(watcher, flow, source)


def _watcher_process_flow(watcher, flow, source):
    for txn in watcher.txn():
        _update_state(txn, flow, "FLOWING")
        processing_block = txn.processing_block.get(flow.key.pb_id)
        eb_id = processing_block.eb_id
    try:
        query_params = QueryParameters(**source.parameters)
        if not query_params.version:
            query_params.version = "latest"
    except TypeError as err:
        logger.error("%s -> Used invalid query parameters: %s", flow.key, source.parameters)
        for txn in watcher.txn():
            _update_state(txn, flow, "FAILED", str(err)[27:])
        return

    successful, reason = _process_flow(flow, eb_id, query_params)

    for txn in watcher.txn():
        _update_state(txn, flow, "COMPLETED" if successful else "FAILED", reason)


def _get_flows(txn: ska_sdp_config.Config.txn) -> Generator[(Flow, FlowSource)]:
    """Get and filter the list of flows"""
    for key, flow in txn.flow.query_values(kind="data-product"):
        source = None
        for raw_source in flow.sources:
            if raw_source.function == "GlobalSkyModel.RequestLocalSkyModel":
                source = raw_source
                break

        if source is None:
            logger.debug("%s -> has no valid source", key)
            continue

        state = txn.flow.state(flow).get() or {}
        status = state.get("status", "NO-STATE")
        if status in ["COMPLETED", "FAILED"]:
            continue

        expected_state = "INITIALISED" if resource_toggle.is_active() else "PENDING"
        if status != expected_state:
            logger.debug(
                "%s -> not in correct state %s != %s", key, state.get("status"), expected_state
            )
            continue

        yield flow, source


def _process_flow(
    flow: Flow, eb_id: str, query_parameters: QueryParameters
) -> tuple[bool, str | None]:
    """Process the Flow entry"""

    output_location = SHARED_VOLUME_MOUNT / flow.sink.data_dir.pvc_subpath

    logger.info(" -> Save to %s", output_location)
    logger.info(" -> params: %s", query_parameters)

    try:
        db = next(get_db())
        try:
            output_data = _query_gsm_for_lsm(query_parameters, db)
            _write_data(eb_id, query_parameters, output_location, output_data)
        finally:
            db.close()
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.error(
            "Failed to process flow %s with parameters %s: %s",
            flow.key,
            query_parameters,
            err,
        )
        logger.exception(err)
        return False, f"Error processing flow {flow.key} with parameters {query_parameters}: {err}"

    return True, None


def _query_gsm_for_lsm(
    query_parameters: QueryParameters,
    db: Session = Depends(get_db),
) -> "GlobalSkyModel":
    """
    Query the Global Sky Model database for components within the specified field of view.

    This function queries the GSM database to retrieve sky components within a circular
    region defined by the provided coordinates and field of view. Results are returned
    as a GlobalSkyModel object.

    Args:
        query_parameters: QueryParameters object containing:
            - ra_deg: Right Ascension in degrees
            - dec_deg: Declination in degrees
            - fov_deg: Field of view radius in degrees
            - version: GSM catalogue version to query (e.g., "1.0.0", "latest")
            - catalogue_name: GSM catalogue name to query
        db: Database session

    Returns:
        A GlobalSkyModel object from ska_sdp_datamodels.global_sky_model,
        containing a dictionary of SkyComponent objects keyed by component ID.

    Note:
        - Empty GlobalSkyModel is returned if no components are found within the FOV
    """
    logger.info(
        "Querying GSM: RA=%.4f°, Dec=%.4f°, FOV=%.4f° (version=%s, catalogue=%s)",
        query_parameters.ra_deg,
        query_parameters.dec_deg,
        query_parameters.fov_deg,
        query_parameters.version,
        query_parameters.catalogue_name,
    )

    try:
        resolved_version = query_parameters.get_version(db)
        logger.info("Using resolved version: %s", resolved_version)

        # Query metadata for this version

        metadata_row = (
            db.query(GlobalSkyModelMetadata)
            .filter(GlobalSkyModelMetadata.version == resolved_version)
            .first()
        )
        metadata_dict = metadata_row.columns_to_dict() if metadata_row else {}

        sky_components_dict = {}
        for sky_component in query_parameters.sky_components(db):
            sky_component_dict = sky_component.columns_to_dict()
            # Remove database-specific fields that are not in SkyComponent dataclass
            del sky_component_dict["id"]
            del sky_component_dict["healpix_index"]
            del sky_component_dict["version"]
            del sky_component_dict["catalogue_name"]
            sky_components_dict[sky_component.id] = SkyComponentDataclass(**sky_component_dict)

        return GlobalSkyModel(metadata=metadata_dict, components=sky_components_dict)

    except Exception as e:
        logger.exception("Error querying GSM database: %s", e)
        raise


def _update_state(
    txn: ska_sdp_config.Config.txn, flow: Flow, state: str, reason: str | None = None
):
    """Update the Flow state"""
    current_state = txn.flow.state(flow).get()

    if current_state is not None and current_state.get("status") == state:
        logger.debug("Skip updating state to same state")
        return

    new_state = {"status": state, "last_updated": time.time()}
    if reason:
        new_state["reason"] = reason

    if current_state:
        current_state.update(new_state)
        txn.flow.state(flow).update(current_state)
    else:
        logger.warning("Flow was missing state, creating ... %s", flow.key)
        txn.flow.state(flow).create(new_state)


def _write_data(
    eb_id: str, query_parameters: QueryParameters, output: Path, data: "GlobalSkyModel"
):  # pylint: disable=too-many-locals
    """
    Write the LSM to disk as a CSV file.

    Args:
        eb_id: Execution block ID
        query_parameters: QueryParameters dataclass instance
        output: Path to the output directory
        data: GlobalSkyModel object containing the components to write
    """
    logger.info("Writing LSM data to: %s", output)
    logger.info("Query parameters: %s", query_parameters)
    logger.info("Number of components: %d", len(data.components))

    # Create output directory if it doesn't exist
    output.mkdir(parents=True, exist_ok=True)

    # Define the output file path
    lsm_file = output / "local_sky_model.csv"

    # Get column names from SkyComponent dataclass
    column_names = (
        list(next(iter(data.components.values())).__annotations__.keys())
        if data.components
        else list(SkyComponentDataclass.__annotations__.keys())
    )

    # Create LocalSkyModel with the right size
    local_model = LocalSkyModel(
        column_names=column_names,
        num_rows=len(data.components),
        max_vector_len=5,  # For spectral index vectors
    )

    local_model.set_header({"QUERY_CENTRE_RAJ2000_DEG": query_parameters.ra_deg})
    local_model.set_header({"QUERY_CENTRE_DEJ2000_DEG": query_parameters.dec_deg})
    local_model.set_header({"QUERY_RADIUS_DEG": query_parameters.fov_deg})
    local_model.set_header({"QUERY_CATALOGUE_VERSION": query_parameters.version})
    local_model.set_header({"QUERY_CATALOGUE_NAME": query_parameters.catalogue_name})
    local_model.set_header({"CATALOGUE_VERSION": data.metadata.get("version", "unknown")})
    local_model.set_header({"CATALOGUE_NAME": data.metadata.get("catalogue_name", "unknown")})

    for key, value in (
        query_parameters.component_queries | query_parameters.metadata_queries
    ).items():
        local_model.set_header({f"QUERY_EXTRA_{key}": value})

    # Populate the LocalSkyModel with data from GlobalSkyModel
    for row_idx, component in enumerate(data.components.values()):
        row_data = {}
        for field in column_names:
            value = getattr(component, field, None)
            # Handle None values in arrays (e.g., spec_idx with nulls)
            if isinstance(value, (list, tuple)):
                # Replace None with numpy.nan in arrays
                value = [np.nan if v is None else v for v in value]
            row_data[field] = value
        local_model.set_row(row_idx, row_data)

    # Find the ska-sdm directory for metadata
    metadata_dir = _find_ska_sdm_dir(output)

    # Handle empty components gracefully
    logger.info(
        "Saving LSM with metadata to %s and %s/ska-data-product.yaml", lsm_file, metadata_dir
    )

    save_lsm_with_metadata(
        local_model,
        {"execution_block_id": eb_id, "catalogue_metadata": data.metadata},
        str(lsm_file),
        str(metadata_dir),
    )

    # Verify files were actually written
    if lsm_file.exists():
        logger.info("Successfully wrote LSM to %s", lsm_file)
    else:
        raise FileNotFoundError(f"LSM file was not created at {lsm_file}")

    metadata_yaml = metadata_dir / "ska-data-product.yaml"
    if metadata_yaml.exists():
        logger.info("Metadata written to %s", metadata_yaml)
    else:
        logger.warning(
            "Metadata file was not created at %s (this may be expected in tests)",
            metadata_yaml,
        )


def _find_ska_sdm_dir(output: Path) -> Path:
    """
    Find the ska-sdm directory in the output path for metadata placement.

    The function returns the first ``<pb_id>/ska-sdm`` parent directory of the
    ``pvc_subpath``. If no ska-sdm directory is found in the path, a
    FileNotFoundError is raised to enforce correct metadata placement.

    Args:
        output: Output path (e.g., /mnt/data/product/{eb_id}/ska-sdp/{pb_id}
        /ska-sdm/sky/{field_id})

    Returns:
        Path to the ska-sdm directory where the metadata file should be written.

    Raises:
        FileNotFoundError: If no ska-sdm directory is found in the output path.
    """
    current = output
    while current != current.parent:
        if current.name == "ska-sdm":
            return current
        current = current.parent

    # If ska-sdm not found, raise an error to enforce correct metadata placement
    error_msg = (
        f"Could not find 'ska-sdm' directory in path {output}. "
        "Metadata file location is undefined. "
        "Please ensure the output path includes a ska-sdm directory as required."
    )
    logger.error(error_msg)
    raise FileNotFoundError(error_msg)
