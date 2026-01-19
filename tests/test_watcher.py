"""Tests for the background watcher"""

import copy
import pathlib
from unittest.mock import MagicMock, call, patch

import pytest
from ska_sdp_config.entity import Flow
from ska_sdp_config.entity.flow import DataProduct, FlowSource, PVCPath
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    GlobalSkyModel,
)

from ska_sdp_global_sky_model.api.app.request_responder import (
    QueryParameters,
    _get_flows,
    _process_flow,
    _query_gsm_for_lsm,
    _update_state,
    _watcher_process,
)

# pylint: disable=too-many-arguments


@pytest.fixture(name="valid_flow")
def fixture_valid_flow():
    """Fixture for a valid flow"""

    pb_id = "pb-test-20260108-1234"
    eb_id = "eb-test-20260108-1234"
    return Flow(
        key=Flow.Key(pb_id=pb_id, name="local-sky-model-field1"),
        sink=DataProduct(
            data_dir=PVCPath(
                k8s_namespaces=[],
                k8s_pvc_name="",
                pvc_mount_path="/mnt/data",
                pvc_subpath=pathlib.Path(f"product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/sky/field1"),
            ),
            paths=[],
        ),
        sources=[
            FlowSource(
                uri="gsm://request/lsm",
                function="GlobalSkyModel.RequestLocalSkyModel",
                parameters={
                    "ra": 2.9670,
                    "dec": -0.1745,
                    "fov": 0.0873,
                },
            ),
        ],
        data_model="CsvNamedColumns",
        expiry_time=-1,
    )


@patch("time.time")
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_metadata", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
def test_happy_path(
    mock_filter_function, mock_write_data, mock_write_metadata, mock_time, valid_flow
):
    """Test the happy path"""

    mock_time.return_value = 1234.5678
    mock_txn = MagicMock()
    mock_watcher = MagicMock()
    mock_config = MagicMock()
    mock_config.watcher.return_value = [mock_watcher]
    mock_watcher.txn.return_value = [mock_txn]

    mock_txn.flow.state.return_value.get.side_effect = [
        {"status": "INITIALISED"},  # from list
        {"status": "INITIALISED"},  # from update to flowing
        {"status": "FLOWING"},  # from update to completed
    ]
    mock_txn.flow.query_values.return_value = [(valid_flow.key, valid_flow)]

    # Create a mock GlobalSkyModel object
    mock_gsm = GlobalSkyModel(sources={})
    mock_filter_function.return_value = mock_gsm

    _watcher_process(mock_config)

    path = pathlib.Path("/mnt/data") / valid_flow.sink.data_dir.pvc_subpath

    assert mock_config.mock_calls == [call.watcher(timeout=30)]
    assert mock_watcher.mock_calls == [
        call.txn(),
        call.txn(),
        call.txn(),
    ]
    assert mock_txn.mock_calls == [
        call.flow.query_values(kind="data-product"),
        call.flow.state(valid_flow),
        call.flow.state().get(),
        call.flow.state(valid_flow),
        call.flow.state().get(),
        call.flow.state(valid_flow),
        call.flow.state().update({"status": "FLOWING", "last_updated": 1234.5678}),
        call.flow.state(valid_flow),
        call.flow.state().get(),
        call.flow.state(valid_flow),
        call.flow.state().update({"status": "COMPLETED", "last_updated": 1234.5678}),
    ]
    assert mock_filter_function.mock_calls == [
        call(QueryParameters(ra=2.9670, dec=-0.1745, fov=0.0873, version="latest"))
    ]
    assert mock_write_data.mock_calls == [call(path, mock_gsm)]
    assert mock_write_metadata.mock_calls == [call(path, valid_flow)]


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_metadata", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
def test_no_state(mock_filter_function, mock_write_data, mock_write_metadata, valid_flow):
    """Test watcher process when a flow has no state"""
    mock_txn = MagicMock()
    mock_watcher = MagicMock()
    mock_config = MagicMock()

    mock_config.watcher.return_value = [mock_watcher]
    mock_watcher.txn.return_value = [mock_txn]

    mock_txn.flow.state.return_value.get.return_value = None
    mock_txn.flow.query_values.return_value = [(valid_flow.key, valid_flow)]

    _watcher_process(mock_config)

    assert mock_config.mock_calls == [call.watcher(timeout=30)]
    assert mock_watcher.mock_calls == [
        call.txn(),
    ]
    assert mock_txn.mock_calls == [
        call.flow.query_values(kind="data-product"),
        call.flow.state(valid_flow),
        call.flow.state().get(),
    ]
    assert mock_filter_function.mock_calls == []
    assert mock_write_data.mock_calls == []
    assert mock_write_metadata.mock_calls == []


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_metadata", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
def test_state_completed(mock_filter_function, mock_write_data, mock_write_metadata, valid_flow):
    """Test watcher process when the state is already completed"""
    mock_txn = MagicMock()
    mock_watcher = MagicMock()
    mock_config = MagicMock()

    mock_config.watcher.return_value = [mock_watcher]
    mock_watcher.txn.return_value = [mock_txn]

    mock_txn.flow.state.return_value.get.return_value = {"status": "COMPLETED"}
    mock_txn.flow.query_values.return_value = [(valid_flow.key, valid_flow)]

    _watcher_process(mock_config)

    assert mock_config.mock_calls == [call.watcher(timeout=30)]
    assert mock_watcher.mock_calls == [
        call.txn(),
    ]
    assert mock_txn.mock_calls == [
        call.flow.query_values(kind="data-product"),
        call.flow.state(valid_flow),
        call.flow.state().get(),
    ]
    assert mock_filter_function.mock_calls == []
    assert mock_write_data.mock_calls == []
    assert mock_write_metadata.mock_calls == []


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_metadata", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
def test_state_not_initialised(
    mock_filter_function, mock_write_data, mock_write_metadata, valid_flow
):
    """Test watcher process when the state is already failed"""
    mock_txn = MagicMock()
    mock_watcher = MagicMock()
    mock_config = MagicMock()

    mock_config.watcher.return_value = [mock_watcher]
    mock_watcher.txn.return_value = [mock_txn]

    mock_txn.flow.state.return_value.get.return_value = {"status": "FLOWING"}
    mock_txn.flow.query_values.return_value = [(valid_flow.key, valid_flow)]

    _watcher_process(mock_config)

    assert mock_config.mock_calls == [call.watcher(timeout=30)]
    assert mock_watcher.mock_calls == [
        call.txn(),
    ]
    assert mock_txn.mock_calls == [
        call.flow.query_values(kind="data-product"),
        call.flow.state(valid_flow),
        call.flow.state().get(),
    ]
    assert mock_filter_function.mock_calls == []
    assert mock_write_data.mock_calls == []
    assert mock_write_metadata.mock_calls == []


@patch("time.time")
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_metadata", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
def test_watcher_process_missing_parameter(
    mock_filter_function, mock_write_data, mock_write_metadata, mock_time, valid_flow
):
    """Test the happy path"""

    del valid_flow.sources[0].parameters["fov"]

    mock_time.return_value = 1234.5678
    mock_txn = MagicMock()
    mock_watcher = MagicMock()
    mock_config = MagicMock()

    mock_config.watcher.return_value = [mock_watcher]
    mock_watcher.txn.return_value = [mock_txn]

    mock_txn.flow.state.return_value.get.side_effect = [
        {"status": "INITIALISED"},  # from list
        {"status": "INITIALISED"},  # from update to flowing
        {"status": "FLOWING"},  # from update to failed
    ]
    mock_txn.flow.query_values.return_value = [(valid_flow.key, valid_flow)]

    mock_filter_function.return_value = ["data"]

    _watcher_process(mock_config)

    assert mock_config.mock_calls == [call.watcher(timeout=30)]
    assert mock_watcher.mock_calls == [
        call.txn(),
        call.txn(),
        call.txn(),
    ]
    assert mock_txn.mock_calls == [
        call.flow.query_values(kind="data-product"),
        call.flow.state(valid_flow),
        call.flow.state().get(),
        call.flow.state(valid_flow),
        call.flow.state().get(),
        call.flow.state(valid_flow),
        call.flow.state().update(
            {
                "status": "FLOWING",
                "last_updated": 1234.5678,
            }
        ),
        call.flow.state(valid_flow),
        call.flow.state().get(),
        call.flow.state(valid_flow),
        call.flow.state().update(
            {
                "status": "FAILED",
                "last_updated": 1234.5678,
                "reason": "missing 1 required positional argument: 'fov'",
            }
        ),
    ]
    assert mock_filter_function.mock_calls == []
    assert mock_write_data.mock_calls == []
    assert mock_write_metadata.mock_calls == []


def test_get_flows_filtering(valid_flow):
    """Test that we can get flows and filter them correctly"""

    txn = MagicMock()

    flow2 = copy.deepcopy(valid_flow)
    flow2.sources[0].function = "invalid-function"

    txn.flow.query_values.return_value = [
        (valid_flow.key, valid_flow),
        (flow2.key, flow2),
    ]
    txn.flow.state.return_value.get.return_value = {"status": "INITIALISED"}

    output = list(_get_flows(txn))

    assert output == [(valid_flow, valid_flow.sources[0])]
    assert txn.mock_calls == [
        call.flow.query_values(kind="data-product"),
        call.flow.state(valid_flow),
        call.flow.state().get(),
    ]


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data")
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_metadata")
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm")
def test_process_flow(mock_call, mock_meta, mock_data, valid_flow):
    """Test that we cann start the processing for a flow"""

    mock_call.return_value = ["data"]

    output_path = pathlib.Path("/mnt/data") / valid_flow.sink.data_dir.pvc_subpath

    success, reason = _process_flow(
        valid_flow, QueryParameters(**valid_flow.sources[0].parameters)
    )

    assert success is True
    assert reason is None

    assert mock_call.mock_calls == [
        call(QueryParameters(ra=2.9670, dec=-0.1745, fov=0.0873, version="latest"))
    ]
    assert mock_meta.mock_calls == [
        call(
            pathlib.Path(output_path),
            valid_flow,
        )
    ]
    assert mock_data.mock_calls == [call(output_path, ["data"])]


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data")
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_metadata")
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm")
def test_process_flow_exception(mock_call, mock_meta, mock_data, valid_flow):
    """Test that we cann start the processing for a flow"""

    mock_call.return_value = ["data"]

    mock_call.side_effect = ValueError("An error occured")

    success, reason = _process_flow(
        valid_flow, QueryParameters(**valid_flow.sources[0].parameters)
    )

    assert success is False
    assert reason == "An error occured"

    assert mock_call.mock_calls == [
        call(QueryParameters(ra=2.9670, dec=-0.1745, fov=0.0873, version="latest"))
    ]
    assert mock_meta.mock_calls == []
    assert mock_data.mock_calls == []


@patch("time.time")
def test_update_state_no_reason(mock_time):
    """Test updating the state with no failure reason"""
    mock_time.return_value = 12345.123

    txn = MagicMock()

    txn.flow.state.return_value.get.return_value = {"status": "INITIALISED"}
    flow = MagicMock()

    _update_state(txn, flow, "NEW_STATE")

    assert txn.mock_calls == [
        call.flow.state(flow),
        call.flow.state().get(),
        call.flow.state(flow),
        call.flow.state().update({"status": "NEW_STATE", "last_updated": 12345.123}),
    ]


@patch("time.time")
def test_update_state_with_reason(mock_time):
    """Test updating the state with a failure reason"""
    mock_time.return_value = 12345.123

    txn = MagicMock()

    txn.flow.state.return_value.get.return_value = {"status": "INITIALISED"}
    flow = MagicMock()

    _update_state(txn, flow, "NEW_STATE", "reason")

    assert txn.mock_calls == [
        call.flow.state(flow),
        call.flow.state().get(),
        call.flow.state(flow),
        call.flow.state().update(
            {"status": "NEW_STATE", "last_updated": 12345.123, "reason": "reason"}
        ),
    ]


@patch("time.time")
def test_update_state_create_state(mock_time):
    """Test that we can create a new state if it is missing"""
    mock_time.return_value = 12345.123

    txn = MagicMock()

    txn.flow.state.return_value.get.return_value = None
    flow = MagicMock()

    _update_state(txn, flow, "NEW_STATE", "reason")

    assert txn.mock_calls == [
        call.flow.state(flow),
        call.flow.state().get(),
        call.flow.state(flow),
        call.flow.state().create(
            {"status": "NEW_STATE", "last_updated": 12345.123, "reason": "reason"}
        ),
    ]


def test_update_state_no_change():
    """Test state is not updated if same state is supplied"""
    txn = MagicMock()
    txn.flow.state.return_value.get.return_value = {"status": "FLOWING"}

    flow = MagicMock()

    _update_state(txn, flow, "FLOWING")

    assert txn.mock_calls == [
        call.flow.state(flow),
        call.flow.state().get(),
    ]


@patch("ska_sdp_global_sky_model.configuration.config.session_local")
def test_query_gsm_for_lsm_with_sources(mock_session_local):
    """Test querying GSM for LSM with sources found"""
    # pylint: disable=too-many-statements,import-outside-toplevel
    from ska_sdp_datamodels.global_sky_model.global_sky_model import (
        NarrowbandMeasurement,
        SkySource,
        WidebandMeasurement,
    )

    # Setup mock database session
    mock_db = MagicMock()
    mock_session_local.return_value = mock_db

    # Mock source data
    mock_source = MagicMock()
    mock_source.id = 1
    mock_source.RAJ2000 = 2.9670
    mock_source.DECJ2000 = -0.1745

    # Mock query chain for sources
    mock_query_sources = MagicMock()
    mock_query_sources.where.return_value.distinct.return_value.all.return_value = [mock_source]
    mock_db.query.return_value = mock_query_sources

    # Mock wideband data
    mock_wb = MagicMock()
    mock_wb.source = 1
    mock_wb.Bck_Wide = 0.01
    mock_wb.Local_RMS_Wide = 0.02
    mock_wb.Int_Flux_Wide = 1.5
    mock_wb.Int_Flux_Wide_Error = 0.1
    mock_wb.Resid_Mean_Wide = 0.001
    mock_wb.Resid_Sd_Wide = 0.002
    mock_wb.Abs_Flux_Pct_Error = 5.0
    mock_wb.Fit_Flux_Pct_Error = 3.0
    mock_wb.A_PSF_Wide = 10.0
    mock_wb.B_PSF_Wide = 8.0
    mock_wb.PA_PSF_Wide = 45.0
    mock_wb.A_Wide = 12.0
    mock_wb.A_Wide_Error = 1.0
    mock_wb.B_Wide = 10.0
    mock_wb.B_Wide_Error = 0.8
    mock_wb.PA_Wide = 50.0
    mock_wb.PA_Wide_Error = 5.0
    mock_wb.Flux_Wide = 2.0
    mock_wb.Flux_Wide_Error = 0.15
    mock_wb.Spectral_Index = -0.7
    mock_wb.Spectral_Index_Error = 0.05
    mock_wb.Spectral_Curvature = 0.1
    mock_wb.Spectral_Curvature_Error = 0.01
    mock_wb.Polarised = False
    mock_wb.Stokes = "I"
    mock_wb.Rotational_Measure = None
    mock_wb.Rotational_Measure_Error = None
    mock_wb.Fractional_Polarisation = None
    mock_wb.Fractional_Polarisation_Error = None
    mock_wb.Faraday_Complex = False
    mock_wb.Variable = False
    mock_wb.Modulation_Index = None
    mock_wb.Debiased_Modulation_Index = None

    # Mock narrowband data
    mock_nb = MagicMock()
    mock_nb.source = 1
    mock_nb.Bck_Narrow = 0.005
    mock_nb.Local_RMS_Narrow = 0.01
    mock_nb.Int_Flux_Narrow = 1.2
    mock_nb.Int_Flux_Narrow_Error = 0.08
    mock_nb.Resid_Mean_Narrow = 0.0005
    mock_nb.Resid_Sd_Narrow = 0.001
    mock_nb.A_PSF_Narrow = 9.0
    mock_nb.B_PSF_Narrow = 7.0
    mock_nb.PA_PSF_Narrow = 40.0
    mock_nb.A_Narrow = 11.0
    mock_nb.A_Narrow_Error = 0.9
    mock_nb.B_Narrow = 9.0
    mock_nb.B_Narrow_Error = 0.7
    mock_nb.PA_Narrow = 45.0
    mock_nb.PA_Narrow_Error = 4.0
    mock_nb.Flux_Narrow = 1.8
    mock_nb.Flux_Narrow_Error = 0.12
    mock_nb.Spectral_Index = -0.65
    mock_nb.Spectral_Index_Error = 0.04
    mock_nb.Polarised = False
    mock_nb.Stokes = "I"
    mock_nb.Rotational_Measure = None
    mock_nb.Rotational_Measure_Error = None
    mock_nb.Fractional_Polarisation = None
    mock_nb.Fractional_Polarisation_Error = None
    mock_nb.Faraday_Complex = False
    mock_nb.Variable = False
    mock_nb.Modulation_Index = None
    mock_nb.Debiased_Modulation_Index = None

    # Configure mock returns for subsequent queries
    def query_side_effect(*_args):  # pylint: disable=unused-argument
        # First call: sources query
        mock_q1 = MagicMock()
        mock_q1.where.return_value.distinct.return_value.all.return_value = [mock_source]

        # Second call: wideband query (has one .join())
        mock_q2 = MagicMock()
        mock_q2.join.return_value.filter.return_value.all.return_value = [(mock_wb, 1)]

        # Third call: narrowband query (has two .join() calls)
        mock_q3 = MagicMock()
        mock_q3.join.return_value.join.return_value.filter.return_value.all.return_value = [
            (mock_nb, 1, 1)
        ]

        # Return appropriate mock based on call order
        if not hasattr(query_side_effect, "call_count"):
            query_side_effect.call_count = 0
        query_side_effect.call_count += 1

        if query_side_effect.call_count == 1:
            return mock_q1
        if query_side_effect.call_count == 2:
            return mock_q2
        return mock_q3

    mock_db.query.side_effect = query_side_effect

    # Execute the function
    query_params = QueryParameters(ra=2.9670, dec=-0.1745, fov=0.0873, version="latest")
    result = _query_gsm_for_lsm(query_params)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert len(result.sources) == 1
    assert 1 in result.sources
    sky_source = result.sources[1]
    assert isinstance(sky_source, SkySource)
    assert sky_source.source_id == 1
    assert sky_source.ra == 2.9670
    assert sky_source.dec == -0.1745
    assert len(sky_source.wideband) == 1
    assert len(sky_source.narrowband) == 1
    assert isinstance(sky_source.wideband[0], WidebandMeasurement)
    assert isinstance(sky_source.narrowband[0], NarrowbandMeasurement)
    assert sky_source.wideband[0].flux == 2.0
    assert sky_source.narrowband[0].flux == 1.8

    # Verify session was closed
    mock_db.close.assert_called_once()


@patch("ska_sdp_global_sky_model.configuration.config.session_local")
def test_query_gsm_for_lsm_no_sources(mock_session_local):
    """Test querying GSM for LSM with no sources found"""

    # Setup mock database session
    mock_db = MagicMock()
    mock_session_local.return_value = mock_db

    # Mock empty source data
    mock_query = MagicMock()
    mock_query.where.return_value.distinct.return_value.all.return_value = []
    mock_db.query.return_value = mock_query

    # Execute the function
    query_params = QueryParameters(ra=2.9670, dec=-0.1745, fov=0.0873, version="latest")
    result = _query_gsm_for_lsm(query_params)

    # Verify empty result
    assert isinstance(result, GlobalSkyModel)
    assert len(result.sources) == 0
    assert not result.sources

    # Verify session was closed
    mock_db.close.assert_called_once()


@patch("ska_sdp_global_sky_model.configuration.config.session_local")
def test_query_gsm_for_lsm_database_error(mock_session_local):
    """Test querying GSM for LSM handles database errors"""

    # Setup mock database session
    mock_db = MagicMock()
    mock_session_local.return_value = mock_db

    # Mock database error
    mock_db.query.side_effect = Exception("Database connection error")

    # Execute the function and expect exception
    query_params = QueryParameters(ra=2.9670, dec=-0.1745, fov=0.0873, version="latest")

    with pytest.raises(Exception, match="Database connection error"):
        _query_gsm_for_lsm(query_params)

    # Verify session was closed even on error
    mock_db.close.assert_called_once()


@patch("ska_sdp_global_sky_model.configuration.config.session_local")
def test_query_gsm_for_lsm_multiple_sources(mock_session_local):
    """Test querying GSM for LSM with multiple sources found"""
    # pylint: disable=too-many-statements,import-outside-toplevel
    from ska_sdp_datamodels.global_sky_model.global_sky_model import SkySource

    # Setup mock database session
    mock_db = MagicMock()
    mock_session_local.return_value = mock_db

    # Mock source data with three sources
    mock_source1 = MagicMock()
    mock_source1.id = 1
    mock_source1.RAJ2000 = 2.9670
    mock_source1.DECJ2000 = -0.1745

    mock_source2 = MagicMock()
    mock_source2.id = 2
    mock_source2.RAJ2000 = 2.9680
    mock_source2.DECJ2000 = -0.1755

    mock_source3 = MagicMock()
    mock_source3.id = 3
    mock_source3.RAJ2000 = 2.9690
    mock_source3.DECJ2000 = -0.1765

    # Mock wideband data for sources
    mock_wb1 = MagicMock()
    mock_wb1.source = 1
    mock_wb1.Bck_Wide = 0.01
    mock_wb1.Local_RMS_Wide = 0.02
    mock_wb1.Int_Flux_Wide = 1.5
    mock_wb1.Int_Flux_Wide_Error = 0.1
    mock_wb1.Resid_Mean_Wide = 0.001
    mock_wb1.Resid_Sd_Wide = 0.002
    mock_wb1.Abs_Flux_Pct_Error = 5.0
    mock_wb1.Fit_Flux_Pct_Error = 3.0
    mock_wb1.A_PSF_Wide = 10.0
    mock_wb1.B_PSF_Wide = 8.0
    mock_wb1.PA_PSF_Wide = 45.0
    mock_wb1.A_Wide = 12.0
    mock_wb1.A_Wide_Error = 1.0
    mock_wb1.B_Wide = 10.0
    mock_wb1.B_Wide_Error = 0.8
    mock_wb1.PA_Wide = 50.0
    mock_wb1.PA_Wide_Error = 5.0
    mock_wb1.Flux_Wide = 2.0
    mock_wb1.Flux_Wide_Error = 0.15
    mock_wb1.Spectral_Index = -0.7
    mock_wb1.Spectral_Index_Error = 0.05
    mock_wb1.Spectral_Curvature = 0.1
    mock_wb1.Spectral_Curvature_Error = 0.01
    mock_wb1.Polarised = False
    mock_wb1.Stokes = "I"
    mock_wb1.Rotational_Measure = None
    mock_wb1.Rotational_Measure_Error = None
    mock_wb1.Fractional_Polarisation = None
    mock_wb1.Fractional_Polarisation_Error = None
    mock_wb1.Faraday_Complex = False
    mock_wb1.Variable = False
    mock_wb1.Modulation_Index = None
    mock_wb1.Debiased_Modulation_Index = None

    mock_wb2 = MagicMock()
    mock_wb2.source = 2
    mock_wb2.Bck_Wide = 0.01
    mock_wb2.Local_RMS_Wide = 0.02
    mock_wb2.Int_Flux_Wide = 1.5
    mock_wb2.Int_Flux_Wide_Error = 0.1
    mock_wb2.Resid_Mean_Wide = 0.001
    mock_wb2.Resid_Sd_Wide = 0.002
    mock_wb2.Abs_Flux_Pct_Error = 5.0
    mock_wb2.Fit_Flux_Pct_Error = 3.0
    mock_wb2.A_PSF_Wide = 10.0
    mock_wb2.B_PSF_Wide = 8.0
    mock_wb2.PA_PSF_Wide = 45.0
    mock_wb2.A_Wide = 12.0
    mock_wb2.A_Wide_Error = 1.0
    mock_wb2.B_Wide = 10.0
    mock_wb2.B_Wide_Error = 0.8
    mock_wb2.PA_Wide = 50.0
    mock_wb2.PA_Wide_Error = 5.0
    mock_wb2.Flux_Wide = 3.5
    mock_wb2.Flux_Wide_Error = 0.2
    mock_wb2.Spectral_Index = -0.8
    mock_wb2.Spectral_Index_Error = 0.06
    mock_wb2.Spectral_Curvature = 0.15
    mock_wb2.Spectral_Curvature_Error = 0.02
    mock_wb2.Polarised = False
    mock_wb2.Stokes = "I"
    mock_wb2.Rotational_Measure = None
    mock_wb2.Rotational_Measure_Error = None
    mock_wb2.Fractional_Polarisation = None
    mock_wb2.Fractional_Polarisation_Error = None
    mock_wb2.Faraday_Complex = False
    mock_wb2.Variable = False
    mock_wb2.Modulation_Index = None
    mock_wb2.Debiased_Modulation_Index = None

    # Configure mock returns for subsequent queries
    def query_side_effect(*_args):
        # First call: sources query
        mock_q1 = MagicMock()
        mock_q1.where.return_value.distinct.return_value.all.return_value = [
            mock_source1,
            mock_source2,
            mock_source3,
        ]

        # Second call: wideband query (has one .join())
        mock_q2 = MagicMock()
        mock_q2.join.return_value.filter.return_value.all.return_value = [
            (mock_wb1, 1),
            (mock_wb2, 1),
        ]

        # Third call: narrowband query (has two .join() calls, empty for this test)
        mock_q3 = MagicMock()
        mock_q3.join.return_value.join.return_value.filter.return_value.all.return_value = []

        # Return appropriate mock based on call order
        if not hasattr(query_side_effect, "call_count"):
            query_side_effect.call_count = 0
        query_side_effect.call_count += 1

        if query_side_effect.call_count == 1:
            return mock_q1
        if query_side_effect.call_count == 2:
            return mock_q2
        return mock_q3

    mock_db.query.side_effect = query_side_effect

    # Execute the function
    query_params = QueryParameters(ra=2.9670, dec=-0.1745, fov=0.0873, version="latest")
    result = _query_gsm_for_lsm(query_params)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert len(result.sources) == 3
    assert 1 in result.sources
    assert 2 in result.sources
    assert 3 in result.sources
    assert isinstance(result.sources[1], SkySource)
    assert isinstance(result.sources[2], SkySource)
    assert isinstance(result.sources[3], SkySource)
    assert result.sources[1].source_id == 1
    assert result.sources[2].source_id == 2
    assert result.sources[3].source_id == 3
    assert len(result.sources[1].wideband) == 1
    assert len(result.sources[2].wideband) == 1
    assert len(result.sources[3].wideband) == 0  # No wideband data for source 3
    assert result.sources[1].wideband[0].flux == 2.0
    assert result.sources[2].wideband[0].flux == 3.5

    # Verify session was closed
    mock_db.close.assert_called_once()
