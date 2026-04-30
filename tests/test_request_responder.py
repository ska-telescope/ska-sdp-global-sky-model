"""Tests for the request_responder"""

import copy
import os
import tempfile
from unittest.mock import MagicMock, call, patch

import pytest
import yaml
from ska_sdp_config.entity.flow import FlowSource
from ska_sdp_datamodels.global_sky_model import LocalSkyModel
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    GlobalSkyModel,
)
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    SkyComponent as SkyComponentDataclass,
)

from ska_sdp_global_sky_model.api.app.request_responder import (
    QueryParameters,
    _get_flows,
    _process_flow,
    _query_gsm_for_lsm,
    _save_lsm_with_metadata,
    _update_state,
    _watcher_process,
    _write_data,
)
from ska_sdp_global_sky_model.configuration.config import SHARED_VOLUME_MOUNT, resource_toggle
from tests.utils import clean_all_tables, override_get_db, set_up_db


@pytest.fixture(scope="module", autouse=True)
def set_up_database():
    """
    Add data for tests, then clean up once
    all of them ran in this module.

    Specific to this module. Do not move.
    """
    set_up_db()
    yield
    clean_all_tables()


@patch("time.time")
@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data", autospec=True)
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm", autospec=True)
# pylint: disable-next=too-many-arguments, too-many-positional-arguments
def test_happy_path(
    mock_filter_function,
    mock_write_data,
    mock_time,
    valid_flow,
    expected_query_parameters,
    monkeypatch,
):
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
    assert mock_filter_function.mock_calls[0].args[0] == expected_query_parameters
    # Second argument is the db session, just verify it was called
    assert len(mock_filter_function.mock_calls) == 1
    # The _write_data signature expects QueryParameters as the second argument
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
                "error_state": "QueryParameters.__init__() missing 1 required "
                "positional argument: 'fov_deg'",
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
def test_process_flow(mock_query, mock_write, valid_flow, expected_query_parameters):
    """Test that we cann start the processing for a flow"""

    mock_query.return_value = ["data"]

    output_path = SHARED_VOLUME_MOUNT / valid_flow.sink.data_dir.pvc_subpath
    eb_id = "eb-test-20260108-1234"

    success, reason = _process_flow(valid_flow, eb_id, expected_query_parameters)

    assert success is True
    assert reason is None

    # Check that _query_gsm_for_lsm was called with correct query parameters
    assert len(mock_query.mock_calls) == 1
    assert mock_query.mock_calls[0].args[0] == expected_query_parameters
    assert mock_write.mock_calls == [call(eb_id, expected_query_parameters, output_path, ["data"])]


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data")
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm")
def test_process_flow_exception(mock_query, mock_write, valid_flow, expected_query_parameters):
    """Test that we can start the processing for a flow"""

    mock_query.return_value = ["data"]

    mock_query.side_effect = ValueError("An error occured")
    eb_id = "eb-test-20260108-1234"

    success, error_state = _process_flow(valid_flow, eb_id, expected_query_parameters)

    assert success is False
    assert error_state["error"] == "An error occured"

    # Check that _query_gsm_for_lsm was called with correct query parameters
    assert len(mock_query.mock_calls) == 1
    assert mock_query.mock_calls[0].args[0] == expected_query_parameters
    assert mock_write.mock_calls == []


@patch("ska_sdp_global_sky_model.api.app.request_responder._write_data")
@patch("ska_sdp_global_sky_model.api.app.request_responder._query_gsm_for_lsm")
def test_process_flow_error_state(mock_query, mock_write, valid_flow, expected_query_parameters):
    """Test that we can report error state correctly when processing a flow"""

    mock_query.side_effect = RuntimeError("test error")
    eb_id = "eb-test-20260108-1234"

    success, error_state = _process_flow(valid_flow, eb_id, expected_query_parameters)
    assert not success
    assert isinstance(error_state, dict)
    assert set(error_state.keys()) == {"flow_key", "parameters", "timestamp", "error"}
    assert error_state["error"] == "test error"
    assert error_state["flow_key"] == str(valid_flow.key)
    assert error_state["parameters"] == expected_query_parameters.__dict__
    assert isinstance(error_state["timestamp"], float)

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
            {"status": "NEW_STATE", "last_updated": 12345.123, "error_state": "reason"}
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
            {"status": "NEW_STATE", "last_updated": 12345.123, "error_state": "reason"}
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


def test_query_gsm_for_lsm_with_sources():  # noqa: F811
    """Test querying GSM for LSM with components found"""
    db_session = next(override_get_db())

    # Execute the function
    query_params = QueryParameters(
        ra_deg=90,
        dec_deg=4,
        fov_deg=0.01,
        version="latest",
        catalogue_name="catalogue1",
        sub_path="test/lsm.csv",
    )
    result = _query_gsm_for_lsm(query_params, db_session)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert len(result.components) == 1
    sky_source = result.components[26]  # 26th element in the db matches these criteria
    assert isinstance(sky_source, SkyComponentDataclass)
    assert sky_source.ra_deg == 90.0
    assert sky_source.dec_deg == 4.0


def test_query_gsm_for_lsm_no_version():  # noqa: F811
    """Test querying GSM for LSM with no version found"""
    db_session = next(override_get_db())
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


def test_query_gsm_for_lsm_multiple_sources():  # noqa: F811
    """Test querying GSM for LSM with multiple components found"""
    db_session = next(override_get_db())

    # Execute the function
    query_params = QueryParameters(
        ra_deg=90,
        dec_deg=4,
        fov_deg=0.4,
        version="latest",
        catalogue_name="catalogue1",
        sub_path="test/lsm.csv",
    )

    result = _query_gsm_for_lsm(query_params, db_session)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert len(result.components) == 3
    for i in [25, 26, 27]:  # components that match criteria are under these ids in db
        assert isinstance(result.components[i], SkyComponentDataclass)


def test_query_gsm_for_lsm_multiple_sources_extra_limit():  # noqa: F811
    """Test querying GSM for LSM with multiple components found, and using an extra param"""
    db_session = next(override_get_db())
    # Execute the function
    query_params = QueryParameters(
        ra_deg=90,
        dec_deg=4,
        fov_deg=180,
        version="latest",
        catalogue_name__endswith="1",
        pa_deg__lt=2,
        sub_path="test/lsm.csv",
    )
    result = _query_gsm_for_lsm(query_params, db_session)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert len(result.components) == 2

    # components that match criteria are under these ids in db
    assert isinstance(result.components[21], SkyComponentDataclass)
    assert isinstance(result.components[22], SkyComponentDataclass)


def test_query_gsm_for_lsm_by_author():  # noqa: F811
    """Test querying GSM for LSM by author metadata field"""
    db_session = next(override_get_db())

    # Query by author
    query_params = QueryParameters(
        ra_deg=0,
        dec_deg=0,
        fov_deg=180,
        author__contains="SDP",
        sub_path="test/lsm.csv",
    )
    result = _query_gsm_for_lsm(query_params, db_session)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert result.metadata["catalogue_name"] == "catalogue3"
    assert result.metadata["author"] == "SKA SDP Team"
    assert result.metadata["freq_min_hz"] == 50e6
    assert result.metadata["freq_max_hz"] == 350e6
    assert len(result.components) == 20
    for _, comp in enumerate(result.components.values()):
        # catalogue3 components have component_ids starting with L000105
        assert comp.component_id.startswith("L000105")


def test_query_gsm_for_lsm_by_freq_min():  # noqa: F811
    """Test querying GSM for LSM by freq_min metadata field"""
    db_session = next(override_get_db())

    # Query by freq_min_hz
    query_params = QueryParameters(
        ra_deg=0,
        dec_deg=0,
        fov_deg=180,
        freq_min_hz=76e6,
        sub_path="test/lsm.csv",
    )
    result = _query_gsm_for_lsm(query_params, db_session)

    # Verify results
    assert isinstance(result, GlobalSkyModel)
    assert result.metadata["catalogue_name"] == "catalogue1"
    assert result.metadata["author"] == "Alice"
    assert result.metadata["freq_min_hz"] == 76e6
    assert result.metadata["freq_max_hz"] == 100e6
    assert len(result.components) == 20
    for _, comp in enumerate(result.components.values()):
        # catalogue1-Alica components have component_ids starting with L000105
        assert comp.component_id.startswith("W000010")
        # mid-frequency of 76-100 MHz (see tests.utils._generate_catalogue)
        assert comp.ref_freq_hz == 88e6


def test_write_data_integration(tmp_path):
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
    with patch("ska_sdp_global_sky_model.api.app.request_responder.MetaData"):
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


def test_metadata_sort_order():
    """
    Test that QueryParameters._get_metadata_record uses the latest
    uploaded catalogue when no catalogue metadata query is provided.
    """
    db_session = next(override_get_db())

    # these params don't actually matter, if metadata-related
    # values were added, those would be used;
    # these are required fields to call QueryParameters
    query_params = QueryParameters(
        ra_deg=111.11,
        dec_deg=-22.22,
        fov_deg=180,
        sub_path="test/lsm.csv",
    )

    # pylint: disable-next=protected-access
    output_metadata = query_params._get_metadata_record(db_session)

    assert output_metadata.catalogue_name == "catalogue2"
    assert output_metadata.version == "1.0.0"


def test_metadata_sort_order_and_latest_version():
    """
    Test that QueryParameters._get_metadata_record uses
    the latest version instead of latest catalogue, when no other
    metadata query parameters are give.
    """
    db_session = next(override_get_db())

    # these params don't actually matter, if metadata-related
    # values were added, those would be used;
    # these are required fields to call QueryParameters
    query_params = QueryParameters(
        ra_deg=111.11, dec_deg=-22.22, fov_deg=180, sub_path="test/lsm.csv", version="latest"
    )

    # pylint: disable-next=protected-access
    output_metadata = query_params._get_metadata_record(db_session)

    assert output_metadata.catalogue_name == "catalogue3"
    assert output_metadata.version == "1.0.5"


def test_get_flows_multiple_gsm_sources(valid_flow, monkeypatch):
    """
    Test that _get_flows returns only GSM related flow source information.
    Any unrelated sources are ignored.
    """
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


# pylint: disable=duplicate-code
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


def test_save_lsm_with_metadata():
    """
    Test that we can save a sky model and metadata YAML file correctly.
    """

    # Create an empty local sky model.
    column_names = ["ra_deg", "dec_deg", "i_pol_jy", "ref_freq_hz", "spec_idx"]
    num_rows = 20
    model = LocalSkyModel(column_names=column_names, num_rows=num_rows)

    # Set a couple of header key, value pairs (as comments).
    header = {
        "QUERY_PARAM_1": "PARAM_1_VALUE",
        "QUERY_PARAM_2": 42,
    }
    model.set_header(header)

    # Write the CSV and the YAML metadata files.
    with tempfile.TemporaryDirectory() as temp_dir_name:
        csv_file_names = [
            os.path.join(temp_dir_name, "_temp_test_lsm1.csv"),
            os.path.join(temp_dir_name, "_temp_test_lsm2.csv"),
        ]
        yaml_dir_name = os.path.join(temp_dir_name, "_temp_test_yaml_metadata_dir")
        yaml_path = os.path.join(yaml_dir_name, "ska-data-product.yaml")

        # Save two copies of the LSM so we have two entries in the YAML.
        execution_block_id = "eb-test-write-lsm"
        for csv_file_name in csv_file_names:
            _save_lsm_with_metadata(
                model, {"execution_block_id": execution_block_id}, csv_file_name, yaml_dir_name
            )

        # Check that the metadata YAML file was written correctly.
        with open(yaml_path, encoding="utf-8") as stream:
            metadata = yaml.safe_load(stream)

        # Check the entry for each file.
        for i, csv_file_name in enumerate(csv_file_names):
            lsm_dict = metadata["local_sky_model"][i]
            assert lsm_dict["columns"] == column_names
            assert lsm_dict["file_path"] == csv_file_name
            assert lsm_dict["header"]["QUERY_PARAM_1"] == header["QUERY_PARAM_1"]
            assert lsm_dict["header"]["QUERY_PARAM_2"] == header["QUERY_PARAM_2"]
            assert lsm_dict["header"]["NUMBER_OF_COMPONENTS"] == num_rows
            assert metadata["execution_block"] == execution_block_id
            assert metadata["files"][i]["path"] == csv_file_name
