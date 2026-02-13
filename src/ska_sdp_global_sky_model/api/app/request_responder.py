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

import dataclasses
import logging
import threading
import time
from collections.abc import Generator
from pathlib import Path

from fastapi import Depends
from sqlalchemy.orm import Session

import ska_sdp_config
from ska_sdp_config.entity.flow import Flow, FlowSource
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    GlobalSkyModel,
)
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    SkyComponent as SkyComponentDataclass,
)

from ska_sdp_global_sky_model.api.app.crud import q3c_radial_query
from ska_sdp_global_sky_model.api.app.models import SkyComponent
from ska_sdp_global_sky_model.configuration.config import (
    REQUEST_WATCHER_TIMEOUT,
    SHARED_VOLUME_MOUNT,
    get_db,
)

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class QueryParameters:
    """The list of available and optional query parameters."""

    ra: float
    """Right Ascension [rad]"""

    dec: float
    """Declination [rad]"""

    fov: float
    """Field of View radius [rad]"""

    version: str = "latest"
    """version of the GSM data"""


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

    try:
        query_params = QueryParameters(**source.parameters)
    except TypeError as err:
        logger.error("%s -> Used invalid query parameters: %s", flow.key, source.parameters)
        for txn in watcher.txn():
            _update_state(txn, flow, "FAILED", str(err)[27:])
        return

    successful, reason = _process_flow(flow, query_params)

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

        if status != "INITIALISED":
            logger.debug("%s -> not in correct state %s != INITIALISED", key, state.get("status"))
            continue

        yield flow, source


def _process_flow(flow: Flow, query_parameters: QueryParameters) -> tuple[bool, str | None]:
    """Process the Flow entry"""

    output_location = SHARED_VOLUME_MOUNT / flow.sink.data_dir.pvc_subpath

    logger.info(" -> Save to %s", output_location)
    logger.info(" -> params: %s", query_parameters)

    try:
        output_data = _query_gsm_for_lsm(query_parameters)
        _write_metadata(output_location, flow)
        _write_data(output_location, output_data)
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.exception(err)
        return False, str(err)

    return True, None


def _query_gsm_for_lsm(
    query_parameters: QueryParameters,
    db: Session = Depends(get_db),
) -> "GlobalSkyModel":
    """
    Query the Global Sky Model database for sources within the specified field of view.

    This function queries the GSM database to retrieve sources and their associated
    narrowband and wideband measurements within a circular region defined by the
    provided coordinates and field of view. Results are returned as a GlobalSkyModel
    object.

    Args:
        query_parameters: QueryParameters object containing:
            - ra: Right Ascension in radians
            - dec: Declination in radians
            - fov: Field of view radius in radians
            - version: GSM version to query (currently unused, defaults to "latest")

    Returns:
        A GlobalSkyModel object from ska_sdp_datamodels.global_sky_model,
        containing a dictionary of SkySource objects keyed by source ID.

    Note:
        - The function uses q3c_radial_query for efficient spatial queries
        - Sources are retrieved along with all their narrowband and wideband data
        - Empty GlobalSkyModel is returned if no sources are found within the FOV
    """
    logger.info(
        "Querying GSM: RA=%.6f, Dec=%.6f, FOV=%.6f rad (version=%s)",
        query_parameters.ra,
        query_parameters.dec,
        query_parameters.fov,
        query_parameters.version,
    )

    try:
        # Query components within the field of view using spatial index
        sky_components = (
            db.query(SkyComponent.id)
            .where(
                q3c_radial_query(
                    SkyComponent.ra,
                    SkyComponent.dec,
                    query_parameters.ra,
                    query_parameters.dec,
                    query_parameters.fov,
                )
            )
            # .where(SkyComponent.version == query_parameters.version)
            .all()
        )

        sky_components_dict = {
            sky_component.id: SkyComponentDataclass(sky_component.to_dict())
            for sky_component in sky_components
        }
        return GlobalSkyModel(components=sky_components_dict)

    except Exception as e:
        logger.exception("Error querying GSM database: %s", e)
        raise


# The next functions are meant to be replaced when we actually query the
# data, as well as write the metadata and data to disk.


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


def _write_data(output: Path, data: "GlobalSkyModel"):  # pragma: no cover
    """This is a stub to write the LSM to disk

    Args:
        output: Path to the output directory
        data: GlobalSkyModel object containing the sources to write
    """
    logger.debug("output: %s", output)
    logger.debug("data: %s", data)


def _write_metadata(output: Path, flow: Flow):  # pragma: no cover
    """This is a stub to write the Metadata"""
    logger.debug("output: %s", output)
    logger.debug("flow: %s", flow)
