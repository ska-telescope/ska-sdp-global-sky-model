"""
Config DB listener and LSM generator.

This module watches for flow entries that request for a Local Sky Model (LSM)
and writes the LSM data files to disk.

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

import dataclasses
import datetime
import io
import logging
import os
import re
import threading
import time
import traceback
import zipfile
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any, get_args, get_origin

import numpy
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
from ska_sdp_dataproduct_metadata import MetaData
from sqlalchemy import Boolean
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import GenericFunction

from ska_sdp_global_sky_model.api.app.models import GlobalSkyModelMetadata, SkyComponent
from ska_sdp_global_sky_model.configuration.config import (
    REQUEST_WATCHER_TIMEOUT,
    SHARED_VOLUME_MOUNT,
    get_db,
    resource_toggle,
)
from ska_sdp_global_sky_model.utilities.helper_functions import make_serisalisable
from ska_sdp_global_sky_model.utilities.query_helpers import QueryBuilder

logger = logging.getLogger(__name__)


# pylint: disable-next=too-few-public-methods,invalid-name
class q3c_radial_query(GenericFunction):
    """SQLAlchemy function for q3c_radial_query(hpx, center, radius) -> BOOLEAN"""

    type = Boolean()
    inherit_cache = True
    name = "q3c_radial_query"


class QueryParameters:
    """This class manages the query parameters and
    helper methods which are used to query the database."""

    def __init__(
        self,
        ra_deg: float,
        dec_deg: float,
        fov_deg: float,
        **query_parameters,
    ):
        """Init method setting the query parameters

        Args:
            - ra_deg: The ra component of the fov
            - dec_deg: The dec component of the fov
            - fov_deg: The field of view
            - query_parameters: All additional query parameters possibly including __ operators

        """
        self.ra_deg = ra_deg
        self.dec_deg = dec_deg
        self.fov_deg = fov_deg
        self._use_latest_version = False
        if query_parameters.get("version", None) == "latest":
            self._use_latest_version = True
            query_parameters.pop("version", None)

        self.sub_path = query_parameters.pop("sub_path", None)

        self.component_queries = {}
        # make default sorting be descending on upload time
        self.metadata_queries = {}

        self._update_component_and_metadata_queries(query_parameters)

    def _update_component_and_metadata_queries(self, query_params):
        # all component fields, except for internal columns
        component_columns = [
            k
            for k in SkyComponent.__table__.columns.keys()  # pylint: disable=no-member
            if k not in ["id", "gsm_id"]
        ]

        # all metadata fields, except for internal columns
        metadata_columns = [
            k
            for k in GlobalSkyModelMetadata.__table__.columns.keys()  # pylint: disable=no-member
            if k not in ["id", "staging"]
        ]
        for key, value in query_params.items():
            if "__" in key:
                field = key.split("__")[0]
            else:
                field = key
            if field in component_columns:
                self.component_queries[key] = value
            elif field in metadata_columns:
                self.metadata_queries[key] = value
            else:
                logger.warning("The QueryParameter %s=%s was not valid", key, value)

    def sky_components(self, db) -> list[tuple[GlobalSkyModelMetadata, list[SkyComponent]]]:
        """Get the sky components based on the query parameters
        Args:
            - db: The database handle

        Returns:
            - The query response of SkyComponents
        """
        # Query components within the field of view using spatial index
        # pylint: disable=no-member,duplicate-code
        metadata_records = self._get_metadata_record(db)

        output = []

        for metadata_record in metadata_records:

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
                .where(SkyComponent.gsm_id == metadata_record.id)
            )
            query_builder = QueryBuilder(SkyComponent, self.component_queries)
            sky_components_query = query_builder.apply_filters(sky_components_query)
            output.append((metadata_record, sky_components_query.all()))
        return output

    def _get_metadata_record(self, db) -> list[GlobalSkyModelMetadata]:
        metadata_query = db.query(GlobalSkyModelMetadata)
        query_builder = QueryBuilder(GlobalSkyModelMetadata, self.metadata_queries)
        metadata_query = query_builder.apply_filters(metadata_query)
        metadata_query = query_builder.apply_sort(metadata_query)

        metadata_records = metadata_query.all()

        if len(metadata_records) == 0:
            return []

        if self._use_latest_version:
            return [max(metadata_records, key=lambda r: Version(r.version))]

        return metadata_records

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
                "component_queries",
                "metadata_queries",
            ]
        )

    def __str__(self):
        """Return query parameters as a string"""
        return (
            f"ra: {self.ra_deg}, dec: {self.dec_deg}, fov: {self.fov_deg}, "
            f"component_queries: {self.component_queries}, "
            f"metadata_queries: {self.metadata_queries}"
        )


def start_lsm_response_thread():  # pragma: no cover
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

        for flow, sources in flows:

            logger.info("Found flow ... %s", flow.key)
            _watcher_process_flow(watcher, flow, sources)


def _watcher_process_flow(watcher, flow, sources):
    errors = []

    for txn in watcher.txn():
        _update_state(txn, flow, "FLOWING")
        processing_block = txn.processing_block.get(flow.key.pb_id)
        eb_id = processing_block.eb_id

    for source in sources:
        try:
            params = source.parameters
            if "version" not in params:
                params["version"] = "latest"

            if "catalogue_name" not in params:
                raise ValueError("'catalogue_name' is a required search parameter")

            query_params = QueryParameters(**params)
        except (TypeError, ValueError) as err:
            logger.error("%s -> Used invalid query parameters: %s", flow.key, source.parameters)
            errors.append(
                {
                    "error": f"Invalid query parameters: {err}",
                    "parameters": source.parameters,
                }
            )
            continue

        successful, error_state = _process_flow(flow, eb_id, query_params)
        if not successful:
            errors.append(error_state)

    # Final state decision
    for txn in watcher.txn():
        if errors:
            _update_state(
                txn,
                flow,
                "FAILED",
                error_state=errors,
            )
        else:
            _update_state(txn, flow, "COMPLETED")


def _get_flows(
    txn: ska_sdp_config.Config.txn,
) -> Generator[tuple[Flow, list[FlowSource]], None, None]:
    """Get and filter the list of flows"""
    for key, flow in txn.flow.query_values(kind="data-product"):
        sources = [
            raw_source
            for raw_source in flow.sources
            if raw_source.function == "GlobalSkyModel.RequestLocalSkyModel"
        ]

        if not sources:
            logger.debug("%s -> has no valid GSM sources", key)
            continue

        state = txn.flow.state(flow).get() or {}
        status = state.get("status", "NO-STATE")
        if status in ["COMPLETED", "FAILED"]:
            continue

        expected_states = ["INITIALISED"]
        if not resource_toggle.is_active():
            expected_states += ["PENDING"]
        if status not in expected_states:
            logger.debug(
                "%s -> not in correct state %s != %s", key, state.get("status"), expected_states
            )
            continue

        yield flow, sources


def _process_flow(
    flow: Flow, eb_id: str, query_parameters: QueryParameters
) -> tuple[bool, dict | None]:
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
            str(query_parameters),
            err,
        )
        logger.exception(err)

        # Extract traceback information
        error_trace = traceback.extract_tb(err.__traceback__)
        if error_trace:
            error_origin = error_trace[-1]
            origin_path = Path(error_origin.filename)
            error_source_info = {
                "file_path": str(origin_path),
                "line": error_origin.lineno,
                "function": error_origin.name,
            }
        else:
            error_source_info = None

        # Build query dict from QueryParameters
        query_dict = {}
        for key, value in query_parameters.__dict__.items():
            query_dict[key] = make_serisalisable(value)

        error_state = {
            "error": str(err),
            "query": query_dict,
        }
        if error_source_info:
            error_state["error_source"] = error_source_info

        return False, error_state

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
        db: Database session

    Returns:
        A GlobalSkyModel object from ska_sdp_datamodels.global_sky_model,
        containing a dictionary of SkyComponent objects keyed by component ID.

    Note:
        - Empty GlobalSkyModel is returned if no components are found within the FOV
    """
    logger.info(
        "Querying GSM: RA=%.4f°, Dec=%.4f°, FOV=%.4f° (metadata=%s, component=%s)",
        query_parameters.ra_deg,
        query_parameters.dec_deg,
        query_parameters.fov_deg,
        query_parameters.metadata_queries,
        query_parameters.component_queries,
    )

    try:
        # Query metadata for this version

        sky_components_dict = {}
        catalogues = query_parameters.sky_components(db)
        if len(catalogues) == 0:
            raise ValueError("No catalogue could be found for query parameters")
        if len(catalogues) > 1:
            raise ValueError("Multiple catalogues have been matched, refine your criteria")

        metadata_record, sky_components = catalogues[0]

        for sky_component in sky_components:
            sky_component_dict = sky_component.columns_to_dict()
            # Remove database-specific fields that are not in SkyComponent dataclass
            del sky_component_dict["id"]
            del sky_component_dict["gsm_id"]
            sky_components_dict[sky_component.id] = SkyComponentDataclass(**sky_component_dict)

        return GlobalSkyModel(
            metadata=metadata_record.columns_to_dict(), components=sky_components_dict
        )

    except Exception as e:
        logger.exception("Error querying GSM database: %s", e)
        raise


# pylint: disable=protected-access
def lsm_to_csv_lines(lsm: LocalSkyModel) -> Generator[str, None, None]:
    """Yield CSV lines from a LocalSkyModel in the standard SKA format.

    Args:
        lsm: Local Sky Model
    """
    yield f"# ({','.join(lsm._column_names)}) = format\n"
    yield f"# NUMBER_OF_COMPONENTS={lsm._num_rows}\n"
    for key, value in lsm.header.items():
        yield f"# {key}={str(value)}\n"
    for row_index in range(lsm._num_rows):
        row = [lsm.get_value_str(name, row_index) for name in lsm._column_names]
        yield ",".join(row) + "\n"


_ECSV_UNIT_MAP = {"degrees": "deg", "degree": "deg"}


# pylint: disable=too-many-return-statements
def _ecsv_dtype(field_type: type) -> tuple[str, str | None]:
    """Return (ecsv_datatype, ecsv_subtype) for a Python type annotation."""
    args = get_args(field_type)
    if args and type(None) in args:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            inner = non_none[0]
            if inner is bool:
                return "string", None  # nullable bool: ECSV bool has no null representation
            return _ecsv_dtype(inner)
    if get_origin(field_type) is list:
        return "string", "float64[5]"
    if field_type is str:
        return "string", None
    if field_type is float:
        return "float64", None
    if field_type is int:
        return "int64", None
    if field_type is bool:
        return "bool", None
    return "string", None


def _ecsv_yaml_value(value: Any) -> str:
    """Format a value as an inline YAML scalar, quoting strings when necessary."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value)
    if not s:
        return "''"
    if any(c in s for c in ":#{}[],&*?|>'\"`!@") or s[0] == "-":
        return "'" + s.replace("'", "''") + "'"
    return s


# pylint: disable=protected-access
# pylint: disable-next=too-many-locals
def lsm_to_ecsv_lines(lsm: LocalSkyModel) -> Generator[str, None, None]:
    """Yield ECSV lines from a LocalSkyModel.

    Produces an Enhanced CSV (ECSV 1.0) file with a YAML metadata header,
    readable by astropy/Topcat.

    Args:
        lsm: Local Sky Model
    """
    field_map = {f.name: f for f in dataclasses.fields(SkyComponentDataclass)}

    yield "# %ECSV 1.0\n"
    yield "# ---\n"
    yield "# datatype:\n"
    for name in lsm._column_names:
        f = field_map[name]
        dtype, subtype = _ecsv_dtype(f.type)
        parts = [f"name: {name}", f"datatype: {dtype}"]
        if subtype:
            parts.append(f"subtype: '{subtype}'")
        unit = f.metadata.get("units") or f.metadata.get("unit")
        if unit:
            parts.append(f"unit: {_ECSV_UNIT_MAP.get(unit, unit)}")
        description = f.metadata.get("description", "").strip()
        if description:
            parts.append(f"description: {_ecsv_yaml_value(description)}")
        yield f"# - {{{', '.join(parts)}}}\n"
    yield "# delimiter: ','\n"
    yield "# meta:\n"
    yield f"#   NUMBER_OF_COMPONENTS: {lsm._num_rows}\n"
    for key, value in lsm.header.items():
        yield f"#   {key}: {_ecsv_yaml_value(value)}\n"
    yield "# schema: astropy-2.0\n"
    yield ",".join(lsm._column_names) + "\n"

    vector_cols = {
        name
        for name in lsm._column_names
        if name in field_map and get_origin(field_map[name].type) is list
    }

    for row_index in range(lsm._num_rows):
        row = []
        for name in lsm._column_names:
            if name in vector_cols:
                vec = lsm._cols[name][row_index]
                parts = []
                for i in range(lsm._max_vector_len):
                    val = float(vec[i])
                    parts.append("null" if numpy.isnan(val) else f"{val:g}")
                row.append('"[' + ",".join(parts) + ']"')
            else:
                row.append(lsm.get_value_str(name, row_index))
        yield ",".join(row) + "\n"


def sky_components_to_ecsv_lines(
    catalogues: list[tuple["GlobalSkyModelMetadata", list["SkyComponent"]]],
    query_parameters: "QueryParameters",
) -> Generator[str, None, None]:
    """Yield LSM ECSV lines for all matching catalogues.

    Args:
        catalogues: The catalogs and skycomponents to be written out.
        query_parameters: The query parameters provided.
    """
    for catalogue, components in catalogues:
        gsm_components = {}
        for c in components:
            c_dict = c.columns_to_dict()
            del c_dict["id"]
            del c_dict["gsm_id"]
            gsm_components[c.id] = SkyComponentDataclass(**c_dict)
        lsm = _build_local_sky_model(catalogue.columns_to_dict(), gsm_components, query_parameters)
        yield from lsm_to_ecsv_lines(lsm)


def _build_local_sky_model(
    metadata_dict: Any,
    components: Any,
    query_parameters: "QueryParameters | None" = None,
) -> LocalSkyModel:
    """Build a LocalSkyModel from a metadata dict and sky components.

    Args:
        metadata_dict: Dictionary of metadata.
        components: The components of the sky model.
        query_parametrs: The query parameters provided.
    """
    column_names = (
        list(next(iter(components.values())).__annotations__.keys())
        if components
        else list(SkyComponentDataclass.__annotations__.keys())
    )
    local_model = LocalSkyModel(
        column_names=column_names,
        num_rows=len(components),
        max_vector_len=5,
    )
    header = {
        f"CATALOGUE_METADATA_{key}".upper(): value
        for key, value in metadata_dict.items()
        if key not in ("staging", "upload_id", "id")
    }
    if query_parameters is not None:
        for key, value in query_parameters.__dict__.items():
            if isinstance(value, dict):
                for key2, val2 in value.items():
                    header[f"QUERY_{key}_{key2}".upper()] = make_serisalisable(val2)
            else:
                header[f"QUERY_{key}".upper()] = make_serisalisable(value)
    local_model.set_header(header)
    for row_idx, component in enumerate(components.values()):
        row_data = {}
        for field in column_names:
            value = getattr(component, field, None)
            if isinstance(value, (list, tuple)):
                value = [numpy.nan if v is None else v for v in value]
            row_data[field] = value
        local_model.set_row(row_idx, row_data)
    return local_model


def sky_components_to_csv_lines(
    catalogues: list[tuple["GlobalSkyModelMetadata", list["SkyComponent"]]],
    query_parameters: "QueryParameters",
) -> Generator[str, None, None]:
    """Yield LSM CSV lines for all matching catalogues.

    Args:
        catalogues: The catalogs and skycomponents to be written out.
        query_parameters: The query parameters provided.
    """
    for catalogue, components in catalogues:
        gsm_components = {}
        for c in components:
            c_dict = c.columns_to_dict()
            del c_dict["id"]
            del c_dict["gsm_id"]
            gsm_components[c.id] = SkyComponentDataclass(**c_dict)
        lsm = _build_local_sky_model(catalogue.columns_to_dict(), gsm_components, query_parameters)
        yield from lsm_to_csv_lines(lsm)


# pylint: disable-next=too-many-locals
def sky_components_to_zip(
    catalogues: list[tuple["GlobalSkyModelMetadata", list["SkyComponent"]]],
    query_parameters: "QueryParameters",
    file_extension: str,
    lsm_to_lines_fn: Callable[[LocalSkyModel], Generator[str, None, None]],
) -> bytes:
    """Build a ZIP archive containing one file per catalogue/version.

    Each file is named ``<catalogue_name>_<version>.<file_extension>`` and
    contains the output of ``lsm_to_lines_fn`` for that catalogue.

    Args:
        catalogues: The catalogs and skycomponents to be written out.
        query_parameters: The query parameters provided.
        file_extension: Extension for each member file ('csv' or 'ecsv').
        lsm_to_lines_fn: Callable that converts a LocalSkyModel to text lines.

    Returns:
        ZIP file contents as bytes.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for catalogue, components in catalogues:
            meta = catalogue.columns_to_dict()
            gsm_components = {}
            for c in components:
                c_dict = c.columns_to_dict()
                del c_dict["id"]
                del c_dict["gsm_id"]
                gsm_components[c.id] = SkyComponentDataclass(**c_dict)
            lsm = _build_local_sky_model(meta, gsm_components, query_parameters)
            safe_name = re.sub(r"[^\w.\-]", "_", str(meta.get("catalogue_name", "catalogue")))
            safe_version = re.sub(r"[^\w.\-]", "_", str(meta.get("version", "unknown")))
            filename = f"{safe_name}_{safe_version}.{file_extension}"
            zf.writestr(filename, "".join(lsm_to_lines_fn(lsm)))
    buffer.seek(0)
    return buffer.read()


def _update_state(
    txn: ska_sdp_config.Config.txn,
    flow: Flow,
    state: str,
    error_state: list[dict] | None = None,
):
    """Update the Flow state"""
    current_state = txn.flow.state(flow).get()

    if current_state is not None and current_state.get("status") == state:
        logger.debug("Skip updating state to same state")
        return

    new_state = {"status": state, "last_updated": time.time()}
    if error_state:
        new_state["error_state"] = error_state

    if current_state:
        current_state.update(new_state)
        txn.flow.state(flow).update(current_state)
    else:
        logger.warning("Flow was missing state, creating ... %s", flow.key)
        txn.flow.state(flow).create(new_state)


def _write_data(
    eb_id: str, query_parameters: QueryParameters, output: Path, data: "GlobalSkyModel"
):  # pylint: disable=too-many-locals, too-many-branches
    """
    Write the LSM to disk as a CSV file.

    Args:
        eb_id: Execution block ID
        query_parameters: QueryParameters dataclass instance
        output: Path to the output directory
        data: GlobalSkyModel object containing the components to write
    """
    logger.info("Writing LSM data to: %s", output)
    logger.info("Query parameters: %s", str(query_parameters))
    logger.info("Number of components: %d", len(data.components))

    # Create output directory if it doesn't exist
    output.mkdir(parents=True, exist_ok=True)

    # Define the output file path
    sub_path = query_parameters.sub_path
    if not sub_path:
        raise ValueError("Missing required parameter: 'sub_path'")
    sub_path = Path(sub_path)

    lsm_file = output / sub_path
    lsm_file.parent.mkdir(parents=True, exist_ok=True)

    local_model = _build_local_sky_model(data.metadata, data.components, query_parameters)

    # Use the base path for the metadata location
    metadata_dir = output

    # Handle empty components gracefully
    logger.info(
        "Saving LSM with metadata to %s and %s/ska-data-product.yaml", lsm_file, metadata_dir
    )

    _save_lsm_with_metadata(
        local_model,
        {"execution_block_id": eb_id},
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


def _save_lsm_with_metadata(
    lsm: LocalSkyModel, metadata_dict: dict[str, Any], lsm_path: str, metadata_dir: str
) -> None:
    """
    Save a sky model to a CSV text file, and update SKA data product metadata.

    Extra metadata is supplied in the metadata_dict parameter, which is
    a dictionary containing specific keys. The keys currently required are:

    - execution_block_id

    If the metadata file already exists, it will be updated with details
    of the new sky model file; otherwise, it will be created.

    An error will be raised during validation if both the YAML file and
    the LSM file already exist.
    To avoid the error, either delete the existing YAML file first,
    or ensure the LSM path given is unique.

    :param lsm: Local sky model to write.
    :type lsm: LocalSkyModel
    :param metadata_dict: Dictionary of metadata.
    :type metadata_dict: dict[str, Any]
    :param path: Path of CSV text file to write.
    :type path: str
    :param metadata_dir: Directory in which to write YAML metadata file.
    :type metadata_dir: str
    """
    # Save the CSV file.
    lsm_file_path = Path(lsm_path)
    lsm_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lsm_path, "w", encoding="utf-8") as out:
        for line in lsm_to_csv_lines(lsm):
            out.write(line)

    # Write or update the YAML metadata file.
    yaml_path = os.path.join(metadata_dir, "ska-data-product.yaml")
    if os.path.exists(yaml_path):
        # Open the existing file for update.
        metadata = MetaData(path=yaml_path)
        metadata.output_path = yaml_path
    else:
        # Create a new metadata file.
        metadata = MetaData()
        metadata.output_path = yaml_path

        # Write any special values that have been set
        # (e.g. the execution block ID).
        if "execution_block_id" in metadata_dict:
            metadata.set_execution_block_id(metadata_dict["execution_block_id"])

    # Get a handle to the top-level metadata dictionary.
    data = metadata.get_data()

    # Create the header dictionary.
    header = {
        "NUMBER_OF_COMPONENTS": lsm.num_components,
    }
    header.update(lsm.header)

    for key, value in header.items():
        # e.g. "uploaded_at" is returned as a datetime object from the db;
        # metadata valudation fails if it's not converted to str first
        if isinstance(value, datetime.datetime):
            header[key] = str(value)

    # Create entry for new file in the list.
    data["sdm"]["lsm"].append(
        {
            "header": header,
            "file_path": lsm_path,
            "columns": lsm.column_names,
        }
    )

    try:
        # Save the LSM file name and metadata (metadata.new_file also writes the output data).
        # An error will be raised during validation if the LSM file already
        # exists. Ensure the LSM path given is unique.
        metadata.new_file(
            dp_path=lsm_path,
            description="Local sky model CSV text file",
        )
    except MetaData.ValidationError as err:
        logger.error("Validation failed with error(s): %s", err.errors)
        logger.debug("Errors occurred writing the following metadata: %s", data)
        raise err
