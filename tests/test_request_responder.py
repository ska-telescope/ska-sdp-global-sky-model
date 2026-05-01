# pylint: disable=redefined-outer-name,unused-import,too-many-lines
"""Tests for the request_responder"""

import copy
import json
import pathlib
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock, call, patch

import pytest
from ska_sdp_config.entity import Flow
from ska_sdp_config.entity.flow import DataProduct, FlowSource, PVCPath
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    GlobalSkyModel,
)
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    SkyComponent as SkyComponentDataclass,
)

from ska_sdp_global_sky_model.api.app.models import GlobalSkyModelMetadata, SkyComponent
from ska_sdp_global_sky_model.api.app.request_responder import (
    QueryParameters,
    _get_flows,
    _process_flow,
    _query_gsm_for_lsm,
    _update_state,
    _watcher_process,
    _write_data,
)
from ska_sdp_global_sky_model.configuration.config import SHARED_VOLUME_MOUNT, resource_toggle
from tests.test_db_schema import db_session  # noqa: F401

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
                pvc_mount_path=str(SHARED_VOLUME_MOUNT),
                pvc_subpath=pathlib.Path(f"product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/sky/field1"),
            ),
            paths=[],
        ),
        sources=[
            FlowSource(
                uri="gsm://request/lsm",
                function="GlobalSkyModel.RequestLocalSkyModel",
                parameters={
                    "ra_deg": 2.9670,
                    "dec_deg": -0.1745,
                    "fov_deg": 0.0873,
                    "catalogue_name": "catalogue",
                    "sub_path": "test/lsm.csv",
                },
            ),
        ],
        data_model="CsvNamedColumns",
        expiry_time=-1,
    )


@patch("time.time")
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
def test_happy_path(mock_filter_function, mock_write_data, mock_time, valid_flow, monkeypatch):
    """Test the happy path"""

    monkeypatch.setenv("FEATURE_RESOURCE_MANAGEMENT_TOGGLE", "1")
    resource_toggle.is_active = lambda: True

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

    # Mock processing_block.get to return an object with eb_id
    mock_processing_block = MagicMock()
    mock_processing_block.eb_id = "eb-test-20260108-1234"
    mock_txn.processing_block.get.return_value = mock_processing_block

    # Create a mock GlobalSkyModel object
    mock_gsm = GlobalSkyModel(components={}, metadata={})
    mock_filter_function.return_value = mock_gsm

    _watcher_process(mock_config)

    path = SHARED_VOLUME_MOUNT / valid_flow.sink.data_dir.pvc_subpath
    eb_id = "eb-test-20260108-1234"

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
        call.processing_block.get(valid_flow.key.pb_id),
        call.flow.state(valid_flow),
        call.flow.state().get(),
        call.flow.state(valid_flow),
        call.flow.state().update({"status": "COMPLETED", "last_updated": 1234.5678}),
    ]
    assert mock_filter_function.mock_calls[0].args[0] == QueryParameters(
        ra_deg=2.9670,
        dec_deg=-0.1745,
        fov_deg=0.0873,
        version="latest",
        catalogue_name="catalogue",
        sub_path="test/lsm.csv",
    )
    # Second argument is the db session, just verify it was called
    assert len(mock_filter_function.mock_calls) == 1
    # The _write_data signature expects QueryParameters as the second argument
    expected_query_parameters = QueryParameters(
        ra_deg=2.9670,
        dec_deg=-0.1745,
        fov_deg=0.0873,
        version="latest",
        catalogue_name="catalogue",
        sub_path="test/lsm.csv",
    )
    assert mock_write_data.mock_calls == [call(eb_id, expected_query_parameters, path, mock_gsm)]


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
def test_no_state(mock_filter_function, mock_write_data, valid_flow):
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


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
def test_state_completed(mock_filter_function, mock_write_data, valid_flow):
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


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
def test_state_not_initialised(mock_filter_function, mock_write_data, valid_flow):
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


@patch("time.time")
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
def test_watcher_process_missing_parameter(
    mock_filter_function, mock_write_data, mock_time, valid_flow
):
    """Test the happy path"""

    del valid_flow.sources[0].parameters["fov_deg"]

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

    # Mock processing_block.get to return an object with eb_id
    mock_processing_block = MagicMock()
    mock_processing_block.eb_id = "eb-test-20260108-1234"
    mock_txn.processing_block.get.return_value = mock_processing_block

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
        call.processing_block.get(valid_flow.key.pb_id),
        call.flow.state(valid_flow),
        call.flow.state().get(),
        call.flow.state(valid_flow),
        call.flow.state().update(
            {
                "status": "FAILED",
                "last_updated": 1234.5678,
                # Just verify error_state exists as a list
                "error_state": [mock.ANY],
            }
        ),
    ]
    assert mock_filter_function.mock_calls == []
    assert mock_write_data.mock_calls == []


@pytest.mark.parametrize(
    "resource_management, flow_state, found",
    [
        (False, "PENDING", True),
        (False, "INITIALISED", True),
        (True, "INITIALISED", True),
        (True, "PENDING", False),
    ],
)
def test_get_flows_filtering(valid_flow, monkeypatch, resource_management, flow_state, found):
    """Test that we can get flows and filter them correctly"""
    monkeypatch.setenv("FEATURE_RESOURCE_MANAGEMENT_TOGGLE", "1" if resource_management else "0")
    resource_toggle.is_active = lambda: resource_management

    txn = MagicMock()

    flow2 = copy.deepcopy(valid_flow)
    flow2.sources[0].function = "invalid-function"

    txn.flow.query_values.return_value = [
        (valid_flow.key, valid_flow),
        (flow2.key, flow2),
    ]
    txn.flow.state.return_value.get.return_value = {"status": flow_state}

    output = list(_get_flows(txn))

    if found:
        # For each flow, collect all GSM sources
        expected_sources = [
            s for s in valid_flow.sources if s.function == "GlobalSkyModel.RequestLocalSkyModel"
        ]
        assert output == [(valid_flow, expected_sources)]
    else:
        assert len(output) == 0
    assert txn.mock_calls == [
        call.flow.query_values(kind="data-product"),
        call.flow.state(valid_flow),
        call.flow.state().get(),
    ]


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data")
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm")
def test_process_flow(mock_query, mock_write, valid_flow):
    """Test that we cann start the processing for a flow"""

    mock_query.return_value = ["data"]

    output_path = SHARED_VOLUME_MOUNT / valid_flow.sink.data_dir.pvc_subpath
    eb_id = "eb-test-20260108-1234"

    success, reason = _process_flow(
        valid_flow, eb_id, QueryParameters(**valid_flow.sources[0].parameters)
    )

    assert success is True
    assert reason is None

    # Check that _query_gsm_for_lsm was called with correct query parameters
    assert len(mock_query.mock_calls) == 1
    assert mock_query.mock_calls[0].args[0] == QueryParameters(
        ra_deg=2.9670,
        dec_deg=-0.1745,
        fov_deg=0.0873,
        version="latest",
        catalogue_name="catalogue",
        sub_path="test/lsm.csv",
    )
    expected_query_parameters = QueryParameters(
        ra_deg=2.9670,
        dec_deg=-0.1745,
        fov_deg=0.0873,
        version="latest",
        catalogue_name="catalogue",
        sub_path="test/lsm.csv",
    )
    assert mock_write.mock_calls == [call(eb_id, expected_query_parameters, output_path, ["data"])]


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data")
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm")
def test_process_flow_exception(mock_query, mock_write, valid_flow):
    """Test that we can start the processing for a flow"""

    mock_query.return_value = ["data"]

    mock_query.side_effect = ValueError("An error occured")
    eb_id = "eb-test-20260108-1234"

    success, error_json = _process_flow(
        valid_flow, eb_id, QueryParameters(**valid_flow.sources[0].parameters)
    )

    assert success is False
    error_state = json.loads(error_json)
    assert error_state["error"] == "An error occured"

    # Check that _query_gsm_for_lsm was called with correct query parameters
    assert len(mock_query.mock_calls) == 1
    assert mock_query.mock_calls[0].args[0] == QueryParameters(
        ra_deg=2.9670,
        dec_deg=-0.1745,
        fov_deg=0.0873,
        version="latest",
        catalogue_name="catalogue",
        sub_path="test/lsm.csv",
    )
    assert mock_write.mock_calls == []


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data")
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm")
def test_process_flow_error_state(mock_query, mock_write, valid_flow):
    """Test that we can report error state correctly when processing a flow"""

    mock_query.side_effect = RuntimeError("test error")
    eb_id = "eb-test-20260108-1234"

    success, error_json = _process_flow(
        valid_flow, eb_id, QueryParameters(**valid_flow.sources[0].parameters)
    )
    assert not success
    assert isinstance(error_json, str)
    error_state = json.loads(error_json)

    assert set(error_state.keys()) == {"error", "flow", "query"}
    assert error_state["error"] == "test error"
    assert error_state["flow"] == str(valid_flow.key)
    assert "ra_deg" in error_state["query"]
    assert error_state["query"]["ra_deg"] == 2.9670

    assert mock_write.mock_calls == []


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
def test_update_state_with_error_state(mock_time):
    """Test updating the state with error state"""
    mock_time.return_value = 12345.123

    txn = MagicMock()

    txn.flow.state.return_value.get.return_value = {"status": "INITIALISED"}
    flow = MagicMock()

    error_state = ['{"error": "test error"}']
    _update_state(txn, flow, "NEW_STATE", error_state=error_state)

    assert txn.mock_calls == [
        call.flow.state(flow),
        call.flow.state().get(),
        call.flow.state(flow),
        call.flow.state().update(
            {"status": "NEW_STATE", "last_updated": 12345.123, "error_state": error_state}
        ),
    ]


@patch("time.time")
def test_update_state_create_state(mock_time):
    """Test that we can create a new state if it is missing"""
    mock_time.return_value = 12345.123

    txn = MagicMock()

    txn.flow.state.return_value.get.return_value = None
    flow = MagicMock()

    error_state = ['{"error": "test error"}']
    _update_state(txn, flow, "NEW_STATE", error_state=error_state)

    assert txn.mock_calls == [
        call.flow.state(flow),
        call.flow.state().get(),
        call.flow.state(flow),
        call.flow.state().create(
            {"status": "NEW_STATE", "last_updated": 12345.123, "error_state": error_state}
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


def test_query_gsm_for_lsm_with_sources(db_session):  # noqa: F811
    """Test querying GSM for LSM with components found"""
    metadata = GlobalSkyModelMetadata(
        version="0.1.0",
        catalogue_name="test",
        description="test",
        upload_id="test",
        author="test",
        reference="test",
        notes="test",
    )
    db_session.add(metadata)
    db_session.commit()
    component = SkyComponent(
        component_id="DictTestSource",
        ra_deg=111.11,
        dec_deg=-22.22,
        healpix_index=33333,
        gsm_id=metadata.id,
        ref_freq_hz=20000000,
    )
    db_session.add(component)
    db_session.commit()

    # Execute the function
    query_params = QueryParameters(
        ra_deg=111.11,
        dec_deg=-22.22,
        fov_deg=180,
        version="latest",
        catalogue_name="test",
        sub_path="test/lsm.csv",
    )
    result = _query_gsm_for_lsm(query_params, db_session)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert len(result.components) == 1
    assert 1 in result.components
    sky_source = result.components[1]
    assert isinstance(sky_source, SkyComponentDataclass)
    assert sky_source.ra_deg == 111.11
    assert sky_source.dec_deg == -22.22


def test_query_gsm_for_lsm_no_version(db_session):  # noqa: F811
    """Test querying GSM for LSM with no version found"""

    # Execute the function
    query_params = QueryParameters(
        ra_deg=2.9670,
        dec_deg=-0.1745,
        fov_deg=0.0873,
        version="latest",
        catalogue_name="test",
        sub_path="test/lsm.csv",
    )
    with pytest.raises(ValueError, match="No catalogue could be found for query parameters"):
        _query_gsm_for_lsm(query_params, db_session)


def test_query_gsm_for_lsm_multiple_sources(db_session):  # noqa: F811
    """Test querying GSM for LSM with multiple components found"""

    metadata = GlobalSkyModelMetadata(
        version="0.1.0",
        catalogue_name="test",
        description="test",
        upload_id="test",
        author="test",
        reference="test",
        notes="test",
    )
    db_session.add(metadata)
    db_session.commit()
    component = SkyComponent(
        component_id="1",
        ra_deg=2.9670,
        dec_deg=-0.1745,
        healpix_index=1,
        gsm_id=metadata.id,
        ref_freq_hz=20000000,
    )
    db_session.add(component)

    component_2 = SkyComponent(
        component_id="2",
        ra_deg=2.9680,
        dec_deg=-0.1755,
        healpix_index=2,
        gsm_id=metadata.id,
        ref_freq_hz=20000000,
    )
    db_session.add(component_2)

    component_3 = SkyComponent(
        component_id="3",
        ra_deg=2.9690,
        dec_deg=-0.1765,
        healpix_index=3,
        gsm_id=metadata.id,
        ref_freq_hz=20000000,
    )
    db_session.add(component_3)

    db_session.commit()

    # Execute the function
    query_params = QueryParameters(
        ra_deg=2.9670,
        dec_deg=-0.1745,
        fov_deg=0.0873,
        version="latest",
        catalogue_name="test",
        sub_path="test/lsm.csv",
    )
    result = _query_gsm_for_lsm(query_params, db_session)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert len(result.components) == 3
    assert 1 in result.components
    assert 2 in result.components
    assert 3 in result.components
    assert isinstance(result.components[1], SkyComponentDataclass)
    assert isinstance(result.components[2], SkyComponentDataclass)
    assert isinstance(result.components[3], SkyComponentDataclass)


def test_query_gsm_for_lsm_multiple_sources_extra_limit(db_session):  # noqa: F811
    """Test querying GSM for LSM with multiple components found, and using an extra param"""

    metadata = GlobalSkyModelMetadata(
        version="0.1.0",
        catalogue_name="test",
        description="test",
        upload_id="test",
        author="test",
        reference="test",
        notes="test",
    )
    db_session.add(metadata)
    db_session.commit()
    component = SkyComponent(
        component_id="1",
        ra_deg=2.9670,
        dec_deg=-0.1745,
        healpix_index=1,
        gsm_id=metadata.id,
        ref_freq_hz=20000000,
        pa_deg=5,
    )
    db_session.add(component)

    component_2 = SkyComponent(
        component_id="2",
        ra_deg=2.9680,
        dec_deg=-0.1755,
        healpix_index=2,
        gsm_id=metadata.id,
        ref_freq_hz=20000000,
        pa_deg=5,
    )
    db_session.add(component_2)

    component_3 = SkyComponent(
        component_id="3",
        ra_deg=2.9690,
        dec_deg=-0.1765,
        healpix_index=3,
        gsm_id=metadata.id,
        ref_freq_hz=20000000,
        pa_deg=8,
    )
    db_session.add(component_3)

    db_session.commit()

    # Execute the function
    query_params = QueryParameters(
        ra_deg=2.9670,
        dec_deg=-0.1745,
        fov_deg=0.0873,
        version="latest",
        catalogue_name="test",
        pa_deg__lt=6,
        sub_path="test/lsm.csv",
    )
    result = _query_gsm_for_lsm(query_params, db_session)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert len(result.components) == 2
    assert 1 in result.components
    assert 2 in result.components
    assert isinstance(result.components[1], SkyComponentDataclass)
    assert isinstance(result.components[2], SkyComponentDataclass)


def test_query_gsm_for_lsm_by_author(db_session):  # noqa: F811
    """Test querying GSM for LSM by author metadata field"""
    metadata_gleam = GlobalSkyModelMetadata(
        version="0.1.0",
        catalogue_name="GLEAM",
        description="GLEAM catalogue",
        upload_id="upload1",
        author="Hurley-Walker et al., 2016",
        reference="DOI:10.1093/mnras/stw2337",
        notes="2017MNRAS.464.1146H",
        freq_min_hz=76e6,
        freq_max_hz=227e6,
    )
    metadata_ska = GlobalSkyModelMetadata(
        version="0.2.0",
        catalogue_name="Test",
        description="SKA AA1 catalogue",
        upload_id="upload2",
        author="SKA SDP Team",
        reference="none",
        notes="a different catalogue",
        freq_min_hz=50e6,
        freq_max_hz=350e6,
    )
    db_session.add(metadata_gleam)
    db_session.add(metadata_ska)
    db_session.commit()
    component_1 = SkyComponent(
        component_id="1",
        source_id="gleam_source",
        ra_deg=111.11,
        dec_deg=-22.22,
        healpix_index=1,
        gsm_id=metadata_gleam.id,
        ref_freq_hz=76e6,
    )
    component_2 = SkyComponent(
        component_id="2",
        source_id="ska_source_1",
        ra_deg=111.22,
        dec_deg=-22.33,
        healpix_index=0,
        gsm_id=metadata_ska.id,
        ref_freq_hz=100e6,
    )
    component_3 = SkyComponent(
        component_id="3",
        source_id="ska_source_2",
        ra_deg=111.45,
        dec_deg=-22.56,
        healpix_index=1,
        gsm_id=metadata_ska.id,
        ref_freq_hz=200e6,
    )
    db_session.add(component_1)
    db_session.add(component_2)
    db_session.add(component_3)
    db_session.commit()

    # Query by author
    query_params = QueryParameters(
        ra_deg=111.11,
        dec_deg=-22.22,
        fov_deg=180,
        author__contains="SDP",
        sub_path="test/lsm.csv",
    )
    result = _query_gsm_for_lsm(query_params, db_session)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert result.metadata["catalogue_name"] == "Test"
    assert result.metadata["author"] == "SKA SDP Team"
    assert result.metadata["freq_min_hz"] == 50e6
    assert result.metadata["freq_max_hz"] == 350e6
    assert len(result.components) == 2
    source_1 = result.components[2]
    source_2 = result.components[3]
    assert source_1.component_id == "2"
    assert source_1.source_id == "ska_source_1"
    assert source_1.ra_deg == 111.22
    assert source_1.dec_deg == -22.33
    assert source_1.ref_freq_hz == 100e6
    assert source_2.component_id == "3"
    assert source_2.source_id == "ska_source_2"
    assert source_2.ra_deg == 111.45
    assert source_2.dec_deg == -22.56
    assert source_2.ref_freq_hz == 200e6


def test_query_gsm_for_lsm_by_freq_min(db_session):  # noqa: F811
    """Test querying GSM for LSM by freq_min metadata field"""
    metadata_gleam = GlobalSkyModelMetadata(
        version="0.1.0",
        catalogue_name="GLEAM",
        description="GLEAM catalogue",
        upload_id="upload1",
        author="Hurley-Walker et al., 2016",
        reference="DOI:10.1093/mnras/stw2337",
        notes="2017MNRAS.464.1146H",
        freq_min_hz=76e6,
        freq_max_hz=227e6,
    )
    metadata_ska = GlobalSkyModelMetadata(
        version="0.2.0",
        catalogue_name="Test",
        description="SKA AA1 catalogue",
        upload_id="upload2",
        author="SKA SDP Team",
        reference="none",
        notes="a different catalogue",
        freq_min_hz=50e6,
        freq_max_hz=350e6,
    )
    db_session.add(metadata_gleam)
    db_session.add(metadata_ska)
    db_session.commit()
    component_1 = SkyComponent(
        component_id="1",
        source_id="gleam_source",
        ra_deg=111.11,
        dec_deg=-22.22,
        healpix_index=1,
        gsm_id=metadata_gleam.id,
        ref_freq_hz=76e6,
    )
    component_2 = SkyComponent(
        component_id="2",
        source_id="ska_source_1",
        ra_deg=111.22,
        dec_deg=-22.33,
        healpix_index=0,
        gsm_id=metadata_ska.id,
        ref_freq_hz=100e6,
    )
    component_3 = SkyComponent(
        component_id="3",
        source_id="ska_source_2",
        ra_deg=111.45,
        dec_deg=-22.56,
        healpix_index=1,
        gsm_id=metadata_ska.id,
        ref_freq_hz=200e6,
    )
    db_session.add(component_1)
    db_session.add(component_2)
    db_session.add(component_3)
    db_session.commit()

    # Query by freq_min_hz
    query_params = QueryParameters(
        ra_deg=111.11,
        dec_deg=-22.22,
        fov_deg=180,
        freq_min_hz=76e6,
        sub_path="test/lsm.csv",
    )
    result = _query_gsm_for_lsm(query_params, db_session)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert result.metadata["catalogue_name"] == "GLEAM"
    assert result.metadata["author"] == "Hurley-Walker et al., 2016"
    assert result.metadata["freq_min_hz"] == 76e6
    assert result.metadata["freq_max_hz"] == 227e6
    assert len(result.components) == 1
    source_1 = result.components[1]
    assert source_1.component_id == "1"
    assert source_1.source_id == "gleam_source"
    assert source_1.ra_deg == 111.11
    assert source_1.dec_deg == -22.22
    assert source_1.ref_freq_hz == 76e6


def test_write_data_integration(
    db_session, tmp_path  # noqa: F811  # pylint: disable=unused-argument,redefined-outer-name
):
    """Integration test for _write_data with actual file writing"""

    # Create test components
    component1 = SkyComponentDataclass(
        component_id="TEST001",
        source_id="S1",
        epoch=2026.2247,
        ra_deg=45.0,
        dec_deg=-30.0,
        i_pol_jy=1.5,
        a_arcsec=0.01,
        b_arcsec=0.005,
        pa_deg=45.0,
        spec_idx=[0.8, -0.5],
        log_spec_idx=False,
        ref_freq_hz=300e6,
    )

    component2 = SkyComponentDataclass(
        component_id="TEST002",
        source_id="S1",
        epoch=2026.2247,
        ra_deg=46.0,
        dec_deg=-31.0,
        i_pol_jy=2.3,
        spec_idx=[0.9],
        ref_freq_hz=300e6,
    )

    query_parameters = QueryParameters(
        ra_deg=2.9670,
        dec_deg=-0.1745,
        fov_deg=0.0873,
        catalogue_name="catalogue",
        sub_path="test/lsm.csv",
    )
    # Create GlobalSkyModel
    gsm = GlobalSkyModel(
        metadata={},
        components={"TEST001": component1, "TEST002": component2},
    )

    # Create output directory structure (simulating the expected path)
    output_dir = (
        tmp_path / "product" / "eb-test" / "ska-sdp" / "pb-test" / "ska-sdm" / "sky" / "field1"
    )

    # Ensure ska-sdm directory exists
    ska_sdm_dir = tmp_path / "product" / "eb-test" / "ska-sdp" / "pb-test" / "ska-sdm"
    ska_sdm_dir.mkdir(parents=True, exist_ok=True)

    eb_id = "eb-test-20260108-1234"

    # Mock the metadata writing to avoid validation issues
    # (metadata validation is tested separately in local_sky_model tests)
    with patch("ska_sdp_global_sky_model.utilities.local_sky_model.MetaData"):
        # Write the data
        _write_data(eb_id, query_parameters, output_dir, gsm)

    # Verify CSV file was created
    csv_file = output_dir / "test" / "lsm.csv"
    assert csv_file.exists()

    # Read and verify CSV content
    csv_content = csv_file.read_text()
    assert "TEST001" in csv_content
    assert "TEST002" in csv_content
    assert "45.0" in csv_content or "45" in csv_content
    assert "-30.0" in csv_content or "-30" in csv_content
    assert "# NUMBER_OF_COMPONENTS=2" in csv_content

    # Note: metadata file is not checked here because MetaData is mocked


def test_write_data_empty_components(tmp_path):
    """Test _write_data with empty GlobalSkyModel"""

    # Create empty GlobalSkyModel
    gsm = GlobalSkyModel(metadata={}, components={})

    # Create output directory
    output_dir = (
        tmp_path / "product" / "eb-test" / "ska-sdp" / "pb-test" / "ska-sdm" / "sky" / "field1"
    )

    # Ensure ska-sdm directory exists
    ska_sdm_dir = tmp_path / "product" / "eb-test" / "ska-sdp" / "pb-test" / "ska-sdm"
    ska_sdm_dir.mkdir(parents=True, exist_ok=True)

    eb_id = "eb-test-20260108-1234"

    query_parameters = QueryParameters(
        ra_deg=2.9670,
        dec_deg=-0.1745,
        fov_deg=0.0873,
        catalogue_name="catalogue",
        sub_path="test/lsm.csv",
    )

    # Write the data (should handle empty components gracefully)
    _write_data(eb_id, query_parameters, output_dir, gsm)

    # Verify CSV file was created
    csv_file = output_dir / "test" / "lsm.csv"
    assert csv_file.exists()

    # Verify it has headers but no data rows
    csv_content = csv_file.read_text()
    lines = csv_content.strip().split("\n")
    # Should have header comment lines but no data
    assert any("format" in line.lower() for line in lines)
    assert any("NUMBER_OF_COMPONENTS=0" in line for line in lines)


def test_metadata_sort_order(db_session):  # noqa: F811
    """Test that the latest uploaded catalogue is used"""
    metadata = GlobalSkyModelMetadata(
        version="1.1.0",
        catalogue_name="test_2",
        description="test",
        upload_id="test",
        author="test",
        reference="test",
        notes="test",
        uploaded_at=datetime(2026, 3, 21, 12, 34, 56),
    )
    db_session.add(metadata)
    db_session.commit()
    metadata2 = GlobalSkyModelMetadata(
        version="0.1.0",
        catalogue_name="test",
        description="test",
        upload_id="test2",
        author="test",
        reference="test",
        notes="test",
        uploaded_at=datetime(2026, 3, 22, 12, 34, 56),
    )
    db_session.add(metadata2)
    db_session.commit()

    # Execute the function
    query_params = QueryParameters(
        ra_deg=111.11,
        dec_deg=-22.22,
        fov_deg=180,
        sub_path="test/lsm.csv",
    )

    # pylint: disable-next=protected-access
    output_metadata = query_params._get_metadata_record(db_session)

    assert output_metadata.catalogue_name == metadata2.catalogue_name
    assert output_metadata.version == metadata2.version


def test_metadata_sort_order_and_latest_version(db_session):  # noqa: F811
    """Test that the latest version is used instead of latest catalogue"""
    metadata = GlobalSkyModelMetadata(
        version="1.1.0",
        catalogue_name="test_2",
        description="test",
        upload_id="test",
        author="test",
        reference="test",
        notes="test",
        uploaded_at=datetime(2026, 3, 21, 12, 34, 56),
    )
    db_session.add(metadata)
    db_session.commit()
    metadata2 = GlobalSkyModelMetadata(
        version="0.1.0",
        catalogue_name="test",
        description="test",
        upload_id="test2",
        author="test",
        reference="test",
        notes="test",
        uploaded_at=datetime(2026, 3, 22, 12, 34, 56),
    )
    db_session.add(metadata2)
    db_session.commit()

    # Execute the function
    query_params = QueryParameters(
        ra_deg=111.11, dec_deg=-22.22, fov_deg=180, sub_path="test/lsm.csv", version="latest"
    )

    # pylint: disable-next=protected-access
    output_metadata = query_params._get_metadata_record(db_session)

    assert output_metadata.catalogue_name == metadata.catalogue_name
    assert output_metadata.version == metadata.version


def test_no_parameters(db_session):  # noqa: F811
    """Test that when no catalogue parameters is given we still get something"""
    metadata = GlobalSkyModelMetadata(
        version="0.1.0",
        catalogue_name="test",
        description="test",
        upload_id="test",
        author="test",
        reference="test",
        notes="test",
    )
    db_session.add(metadata)
    db_session.commit()

    # Execute the function
    query_params = QueryParameters(
        ra_deg=111.11,
        dec_deg=-22.22,
        fov_deg=180,
        sub_path="test/lsm.csv",
    )

    # pylint: disable-next=protected-access
    output_metadata = query_params._get_metadata_record(db_session)

    assert output_metadata.catalogue_name == metadata.catalogue_name
    assert output_metadata.version == metadata.version


def test_get_flows_multiple_gsm_sources(valid_flow, monkeypatch):
    """Test that unrelated sources are ignored and both GSM sources are returned"""
    monkeypatch.setenv("FEATURE_RESOURCE_MANAGEMENT_TOGGLE", "1")
    resource_toggle.is_active = lambda: True

    flow2 = copy.deepcopy(valid_flow)
    flow2.sources = [
        FlowSource(
            uri="gsm://request/lsm",
            function="GlobalSkyModel.RequestLocalSkyModel",
            parameters={
                "ra_deg": 1.0,
                "dec_deg": 2.0,
                "fov_deg": 3.0,
                "catalogue_name": "catalogue",
                "sub_path": "test/a.csv",
            },
        ),
        FlowSource(
            uri="gsm://request/lsm",
            function="GlobalSkyModel.RequestLocalSkyModel",
            parameters={
                "ra_deg": 4.0,
                "dec_deg": 5.0,
                "fov_deg": 6.0,
                "catalogue_name": "catalogue",
                "sub_path": "test/b.csv",
            },
        ),
        FlowSource(
            uri="something://else",
            function="NotTheGsmFunction",
            parameters={"x": 1},
        ),
    ]

    txn = MagicMock()
    txn.flow.query_values.return_value = [(flow2.key, flow2)]
    txn.flow.state.return_value.get.return_value = {"status": "INITIALISED"}
    output = list(_get_flows(txn))
    assert output == [(flow2, flow2.sources[:2])]


@patch("time.time")
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
def test_watcher_process_multiple_sources(
    mock_query, mock_write_data, mock_time, valid_flow, monkeypatch
):
    """Test that we can process a flow with multiple GSM sources"""
    monkeypatch.setenv("FEATURE_RESOURCE_MANAGEMENT_TOGGLE", "1")
    resource_toggle.is_active = lambda: True

    mock_time.return_value = 1234.5678
    mock_txn = MagicMock()
    mock_watcher = MagicMock()
    mock_config = MagicMock()
    mock_config.watcher.return_value = [mock_watcher]
    mock_watcher.txn.return_value = [mock_txn]

    # multiple sources
    valid_flow.sources = [
        FlowSource(
            uri="gsm://request/lsm",
            function="GlobalSkyModel.RequestLocalSkyModel",
            parameters={
                "ra_deg": 2.9670,
                "dec_deg": -0.1745,
                "fov_deg": 0.0873,
                "catalogue_name": "catalogue",
                "sub_path": "test/lsm1.csv",
            },
        ),
        FlowSource(
            uri="gsm://request/lsm",
            function="GlobalSkyModel.RequestLocalSkyModel",
            parameters={
                "ra_deg": 2.9680,
                "dec_deg": -0.1755,
                "fov_deg": 0.0873,
                "catalogue_name": "catalogue",
                "sub_path": "test/lsm2.csv",
            },
        ),
    ]

    # state transitions
    mock_txn.flow.state.return_value.get.side_effect = [
        {"status": "INITIALISED"},
        {"status": "INITIALISED"},
        {"status": "FLOWING"},
    ]

    mock_txn.flow.query_values.return_value = [(valid_flow.key, valid_flow)]
    mock_processing_block = MagicMock()
    mock_processing_block.eb_id = "eb-test-20260108-1234"
    mock_txn.processing_block.get.return_value = mock_processing_block

    mock_gsm = GlobalSkyModel(components={}, metadata={})
    mock_query.return_value = mock_gsm

    _watcher_process(mock_config)

    # verify write call twice
    assert mock_write_data.mock_calls == [
        call(
            "eb-test-20260108-1234",
            QueryParameters(
                ra_deg=2.9670,
                dec_deg=-0.1745,
                fov_deg=0.0873,
                version="latest",
                catalogue_name="catalogue",
                sub_path="test/lsm1.csv",
            ),
            SHARED_VOLUME_MOUNT / valid_flow.sink.data_dir.pvc_subpath,
            mock_gsm,
        ),
        call(
            "eb-test-20260108-1234",
            QueryParameters(
                ra_deg=2.9680,
                dec_deg=-0.1755,
                fov_deg=0.0873,
                version="latest",
                catalogue_name="catalogue",
                sub_path="test/lsm2.csv",
            ),
            SHARED_VOLUME_MOUNT / valid_flow.sink.data_dir.pvc_subpath,
            mock_gsm,
        ),
    ]

    # Verify state transitions
    assert mock_txn.mock_calls == [
        call.flow.query_values(kind="data-product"),
        call.flow.state(valid_flow),
        call.flow.state().get(),
        call.flow.state(valid_flow),
        call.flow.state().get(),
        call.flow.state(valid_flow),
        call.flow.state().update({"status": "FLOWING", "last_updated": 1234.5678}),
        call.processing_block.get(valid_flow.key.pb_id),
        call.flow.state(valid_flow),
        call.flow.state().get(),
        call.flow.state(valid_flow),
        call.flow.state().update({"status": "COMPLETED", "last_updated": 1234.5678}),
    ]
