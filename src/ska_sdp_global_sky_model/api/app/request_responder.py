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
from typing import TYPE_CHECKING

import ska_sdp_config
from ska_sdp_config.entity.flow import Flow, FlowSource

from ska_sdp_global_sky_model.configuration.config import (
    REQUEST_WATCHER_TIMEOUT,
    SHARED_VOLUME_MOUNT,
)

if TYPE_CHECKING:
    from ska_sdp_datamodels.global_sky_model.global_sky_model import (
        GlobalSkyModel,
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


# The next functions are meant to be replaced when we actually query the
# data, as well as write the metadata and data to disk.


def _query_gsm_for_lsm(
    query_parameters: QueryParameters,
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
    # pylint: disable=too-many-locals,import-outside-toplevel
    from ska_sdp_datamodels.global_sky_model.global_sky_model import (
        GlobalSkyModel,
        NarrowbandMeasurement,
        SkySource,
        WidebandMeasurement,
    )

    from ska_sdp_global_sky_model.api.app.crud import q3c_radial_query
    from ska_sdp_global_sky_model.api.app.model import (
        Band,
        NarrowBandData,
        Source,
        Telescope,
        WideBandData,
    )
    from ska_sdp_global_sky_model.configuration.config import session_local

    logger.info(
        "Querying GSM: RA=%.6f, Dec=%.6f, FOV=%.6f rad (version=%s)",
        query_parameters.ra,
        query_parameters.dec,
        query_parameters.fov,
        query_parameters.version,
    )

    # Create database session
    db = session_local()

    try:
        # Query sources within the field of view using spatial index
        sources = (
            db.query(Source.id, Source.RAJ2000, Source.DECJ2000)
            .where(
                q3c_radial_query(
                    Source.RAJ2000,
                    Source.DECJ2000,
                    query_parameters.ra,
                    query_parameters.dec,
                    query_parameters.fov,
                )
            )
            .distinct(Source.id)
            .all()
        )

        if not sources:
            logger.info("No sources found within FOV")
            return GlobalSkyModel(sources={})

        logger.info("Found %d sources within FOV", len(sources))

        source_ids = [s.id for s in sources]

        # Query all wideband measurements for the sources
        wideband_rows = (
            db.query(WideBandData, Telescope.id.label("telescope_id"))
            .join(Telescope, WideBandData.telescope == Telescope.id)
            .filter(WideBandData.source.in_(source_ids))
            .all()
        )

        # Query all narrowband measurements for the sources
        narrowband_rows = (
            db.query(
                NarrowBandData,
                Band.id.label("band_id"),
                Telescope.id.label("telescope_id"),
            )
            .join(Band, NarrowBandData.band == Band.id)
            .join(Telescope, Band.telescope == Telescope.id)
            .filter(NarrowBandData.source.in_(source_ids))
            .all()
        )

        # Build the result list of SkySource objects
        result_sources = []

        for source in sources:
            # Get wideband measurements for this source
            wideband_measurements = []
            for wb_row, telescope_id in wideband_rows:
                if wb_row.source == source.id:
                    wideband_measurements.append(
                        WidebandMeasurement(
                            telescope=telescope_id,
                            bck=wb_row.Bck_Wide,
                            local_rms=wb_row.Local_RMS_Wide,
                            int_flux=wb_row.Int_Flux_Wide,
                            int_flux_error=wb_row.Int_Flux_Wide_Error,
                            resid_mean=wb_row.Resid_Mean_Wide,
                            resid_sd=wb_row.Resid_Sd_Wide,
                            abs_flux_pct_error=wb_row.Abs_Flux_Pct_Error,
                            fit_flux_pct_error=wb_row.Fit_Flux_Pct_Error,
                            a_psf=wb_row.A_PSF_Wide,
                            b_psf=wb_row.B_PSF_Wide,
                            pa_psf=wb_row.PA_PSF_Wide,
                            a=wb_row.A_Wide,
                            a_error=wb_row.A_Wide_Error,
                            b=wb_row.B_Wide,
                            b_error=wb_row.B_Wide_Error,
                            pa=wb_row.PA_Wide,
                            pa_error=wb_row.PA_Wide_Error,
                            flux=wb_row.Flux_Wide,
                            flux_error=wb_row.Flux_Wide_Error,
                            spectral_index=wb_row.Spectral_Index,
                            spectral_index_error=wb_row.Spectral_Index_Error,
                            spectral_curvature=wb_row.Spectral_Curvature,
                            spectral_curvature_error=wb_row.Spectral_Curvature_Error,
                            polarised=wb_row.Polarised,
                            stokes=wb_row.Stokes,
                            rotational_measure=wb_row.Rotational_Measure,
                            rotational_measure_error=wb_row.Rotational_Measure_Error,
                            fractional_polarisation=wb_row.Fractional_Polarisation,
                            fractional_polarisation_error=wb_row.Fractional_Polarisation_Error,
                            faraday_complex=wb_row.Faraday_Complex,
                            variable=wb_row.Variable,
                            modulation_index=wb_row.Modulation_Index,
                            debiased_modulation_index=wb_row.Debiased_Modulation_Index,
                        )
                    )

            # Get narrowband measurements for this source
            narrowband_measurements = []
            for nb_row, band_id, telescope_id in narrowband_rows:
                if nb_row.source == source.id:
                    narrowband_measurements.append(
                        NarrowbandMeasurement(
                            telescope=telescope_id,
                            band=band_id,
                            bck=nb_row.Bck_Narrow,
                            local_rms=nb_row.Local_RMS_Narrow,
                            int_flux=nb_row.Int_Flux_Narrow,
                            int_flux_error=nb_row.Int_Flux_Narrow_Error,
                            resid_mean=nb_row.Resid_Mean_Narrow,
                            resid_sd=nb_row.Resid_Sd_Narrow,
                            a_psf=nb_row.A_PSF_Narrow,
                            b_psf=nb_row.B_PSF_Narrow,
                            pa_psf=nb_row.PA_PSF_Narrow,
                            a=nb_row.A_Narrow,
                            a_error=nb_row.A_Narrow_Error,
                            b=nb_row.B_Narrow,
                            b_error=nb_row.B_Narrow_Error,
                            pa=nb_row.PA_Narrow,
                            pa_error=nb_row.PA_Narrow_Error,
                            flux=nb_row.Flux_Narrow,
                            flux_error=nb_row.Flux_Narrow_Error,
                            spectral_index=nb_row.Spectral_Index,
                            spectral_index_error=nb_row.Spectral_Index_Error,
                            polarised=nb_row.Polarised,
                            stokes=nb_row.Stokes,
                            rotational_measure=nb_row.Rotational_Measure,
                            rotational_measure_error=nb_row.Rotational_Measure_Error,
                            fractional_polarisation=nb_row.Fractional_Polarisation,
                            fractional_polarisation_error=nb_row.Fractional_Polarisation_Error,
                            faraday_complex=nb_row.Faraday_Complex,
                            variable=nb_row.Variable,
                            modulation_index=nb_row.Modulation_Index,
                            debiased_modulation_index=nb_row.Debiased_Modulation_Index,
                        )
                    )

            # Create SkySource object
            sky_source = SkySource(
                source_id=source.id,
                ra=source.RAJ2000,
                dec=source.DECJ2000,
                narrowband=narrowband_measurements,
                wideband=wideband_measurements,
            )

            result_sources.append(sky_source)

        logger.info("Converted %d sources to SkySource objects", len(result_sources))
        # Convert list to dictionary keyed by source_id
        sources_dict = {src.source_id: src for src in result_sources}
        return GlobalSkyModel(sources=sources_dict)

    except Exception as e:
        logger.exception("Error querying GSM database: %s", e)
        raise
    finally:
        db.close()


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
