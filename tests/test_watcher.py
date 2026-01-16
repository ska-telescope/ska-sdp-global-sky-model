"""Tests for the background watcher"""

import copy
import pathlib
from unittest.mock import MagicMock, call, patch

import pytest
from ska_sdp_config.entity import Flow
from ska_sdp_config.entity.flow import DataProduct, FlowSource, PVCPath

from ska_sdp_global_sky_model.api.app.request_responder import (
    QueryParameters,
    _get_flows,
    _process_flow,
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

    mock_filter_function.return_value = ["data"]

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
    assert mock_write_data.mock_calls == [call(path, ["data"])]
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
