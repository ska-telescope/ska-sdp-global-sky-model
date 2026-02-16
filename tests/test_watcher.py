# pylint: disable=redefined-outer-name,unused-import
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
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    SkyComponent as SkyComponentDataclass,
)

from ska_sdp_global_sky_model.api.app.models import SkyComponent
from ska_sdp_global_sky_model.api.app.request_responder import (
    QueryParameters,
    _find_ska_sdm_dir,
    _get_flows,
    _process_flow,
    _query_gsm_for_lsm,
    _update_state,
    _watcher_process,
    _write_data,
)
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
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
def test_happy_path(mock_filter_function, mock_write_data, mock_time, valid_flow):
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
    mock_gsm = GlobalSkyModel(components={}, metadata={})
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
    assert mock_filter_function.mock_calls[0].args[0] == QueryParameters(
        ra=2.9670, dec=-0.1745, fov=0.0873, version="latest"
    )
    # Second argument is the db session, just verify it was called
    assert len(mock_filter_function.mock_calls) == 1
    assert mock_write_data.mock_calls == [call(path, mock_gsm)]


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
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm")
def test_process_flow(mock_query, mock_write, valid_flow):
    """Test that we cann start the processing for a flow"""

    mock_query.return_value = ["data"]

    output_path = pathlib.Path("/mnt/data") / valid_flow.sink.data_dir.pvc_subpath

    success, reason = _process_flow(
        valid_flow, QueryParameters(**valid_flow.sources[0].parameters)
    )

    assert success is True
    assert reason is None

    # Check that _query_gsm_for_lsm was called with correct query parameters
    assert len(mock_query.mock_calls) == 1
    assert mock_query.mock_calls[0].args[0] == QueryParameters(
        ra=2.9670, dec=-0.1745, fov=0.0873, version="latest"
    )
    assert mock_write.mock_calls == [call(output_path, ["data"])]


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data")
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm")
def test_process_flow_exception(mock_query, mock_write, valid_flow):
    """Test that we cann start the processing for a flow"""

    mock_query.return_value = ["data"]

    mock_query.side_effect = ValueError("An error occured")

    success, reason = _process_flow(
        valid_flow, QueryParameters(**valid_flow.sources[0].parameters)
    )

    assert success is False
    assert reason == "An error occured"

    # Check that _query_gsm_for_lsm was called with correct query parameters
    assert len(mock_query.mock_calls) == 1
    assert mock_query.mock_calls[0].args[0] == QueryParameters(
        ra=2.9670, dec=-0.1745, fov=0.0873, version="latest"
    )
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


def test_query_gsm_for_lsm_with_sources(db_session):  # noqa: F811
    """Test querying GSM for LSM with components found"""
    component = SkyComponent(
        component_id="DictTestSource",
        ra=111.11,
        dec=-22.22,
        healpix_index=33333,
        version="latest",
    )
    db_session.add(component)
    db_session.commit()

    # Execute the function
    query_params = QueryParameters(ra=111.11, dec=-22.22, fov=180, version="latest")
    result = _query_gsm_for_lsm(query_params, db_session)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert len(result.components) == 1
    assert 1 in result.components
    sky_source = result.components[1]
    assert isinstance(sky_source, SkyComponentDataclass)
    assert sky_source.ra == 111.11
    assert sky_source.dec == -22.22


def test_query_gsm_for_lsm_no_sources(db_session):  # noqa: F811
    """Test querying GSM for LSM with no components found"""

    # Execute the function
    query_params = QueryParameters(ra=2.9670, dec=-0.1745, fov=0.0873, version="latest")
    result = _query_gsm_for_lsm(query_params, db_session)

    # Verify empty result
    assert isinstance(result, GlobalSkyModel)
    assert len(result.components) == 0
    assert not result.components


def test_query_gsm_for_lsm_multiple_sources(db_session):  # noqa: F811
    """Test querying GSM for LSM with multiple components found"""

    component = SkyComponent(
        component_id="1",
        ra=2.9670,
        dec=-0.1745,
        healpix_index=1,
        version="latest",
    )
    db_session.add(component)

    component_2 = SkyComponent(
        component_id="2",
        ra=2.9680,
        dec=-0.1755,
        healpix_index=2,
        version="latest",
    )
    db_session.add(component_2)

    component_3 = SkyComponent(
        component_id="3",
        ra=2.9690,
        dec=-0.1765,
        healpix_index=3,
        version="latest",
    )
    db_session.add(component_3)

    db_session.commit()

    # Execute the function
    query_params = QueryParameters(ra=2.9670, dec=-0.1745, fov=0.0873, version="latest")
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


def test_write_data_integration(
    db_session, tmp_path  # noqa: F811  # pylint: disable=unused-argument,redefined-outer-name
):
    """Integration test for _write_data with actual file writing"""

    # Create test components
    component1 = SkyComponentDataclass(
        component_id="TEST001",
        ra=45.0,
        dec=-30.0,
        i_pol=1.5,
        major_ax=0.01,
        minor_ax=0.005,
        pos_ang=45.0,
        spec_idx=[0.8, -0.5],
        log_spec_idx=False,
    )

    component2 = SkyComponentDataclass(
        component_id="TEST002",
        ra=46.0,
        dec=-31.0,
        i_pol=2.3,
        spec_idx=[0.9],
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

    # Mock the metadata writing to avoid validation issues
    # (metadata validation is tested separately in local_sky_model tests)
    with patch("ska_sdp_global_sky_model.utilities.local_sky_model.MetaData"):
        # Write the data
        _write_data(output_dir, gsm)

    # Verify CSV file was created
    csv_file = output_dir / "local_sky_model.csv"
    assert csv_file.exists()

    # Read and verify CSV content
    csv_content = csv_file.read_text()
    assert "TEST001" in csv_content
    assert "TEST002" in csv_content
    assert "45.0" in csv_content or "45" in csv_content
    assert "-30.0" in csv_content or "-30" in csv_content
    assert "# NUMBER_OF_COMPONENTS: 2" in csv_content


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

    # Mock the metadata writing to avoid validation issues
    with patch("ska_sdp_global_sky_model.utilities.local_sky_model.MetaData"):
        # Write the data
        _write_data(output_dir, gsm)

    # Verify CSV file was created
    csv_file = output_dir / "local_sky_model.csv"
    assert csv_file.exists()

    # Verify it has headers but no data rows
    csv_content = csv_file.read_text()
    lines = csv_content.strip().split("\n")
    # Should have header comment lines but no data
    assert any("format" in line.lower() for line in lines)
    assert any("NUMBER_OF_COMPONENTS: 0" in line for line in lines)


def test_find_ska_sdm_dir():
    """Test _find_ska_sdm_dir helper function"""
    # Test with ska-sdm in path
    test_path = pathlib.Path("/mnt/data/product/eb-123/ska-sdp/pb-456/ska-sdm/sky/field1")
    result = _find_ska_sdm_dir(test_path)
    assert result == pathlib.Path("/mnt/data/product/eb-123/ska-sdp/pb-456/ska-sdm")

    # Test with ska-sdm as the current directory
    test_path = pathlib.Path("/mnt/data/product/eb-123/ska-sdp/pb-456/ska-sdm")
    result = _find_ska_sdm_dir(test_path)
    assert result == pathlib.Path("/mnt/data/product/eb-123/ska-sdp/pb-456/ska-sdm")

    # Test with no ska-sdm in path (should return parent)
    test_path = pathlib.Path("/some/other/path")
    result = _find_ska_sdm_dir(test_path)
    assert result == pathlib.Path("/some/other")
