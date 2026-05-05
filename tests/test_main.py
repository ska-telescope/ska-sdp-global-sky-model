"""
Basic testing of the API
"""

from unittest.mock import MagicMock, patch

import pytest

from ska_sdp_global_sky_model.api.app.main import wait_for_db
from tests.utils import clean_all_tables, set_up_db


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


def test_read_main(myclient):
    """Unit test for the root path "/" """
    response = myclient.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "live"}


def test_redirect_from_home(myclient):
    """Unit test for the root path "/" """
    response = myclient.get("/")
    assert str(response.url).endswith("/docs")
    assert response.status_code == 200


def test_components(myclient):
    """Unit test for the /components endpoint"""
    response = myclient.get("/components")
    assert response.status_code == 200

    # Data from all three added catalogues and all versions appear
    assert response.text.count("W000010") == 20  # catalogue1-Alice
    assert response.text.count("X000020") == 10  # catalogue1-Bob
    assert response.text.count("A000100") == 200  # catalogue2
    assert response.text.count("L000105") == 20  # catalogue3


def test_local_sky_model(myclient):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query in the region covered by test data (RA ~80, Dec ~4 +- 10)
    without a specified version
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={"ra_deg": 80, "dec_deg": 4, "fov_deg": 10, "catalogue_name": "catalogue3"},
    )

    assert local_sky_model.status_code == 200

    assert local_sky_model.text.count("L000105") == 20
    for i in range(5, 16):
        assert f"L000105+0000{i:0>2d}" in local_sky_model.text


def test_local_sky_model_with_version(myclient):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query in the region covered by test data (RA ~90, Dec ~2 +- 5)
    but with version that only includes one component
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": "90",
            "dec_deg": "2",
            "fov_deg": 5,
            "version": "0.1.0",
            "catalogue_name": "catalogue1",
        },
    )

    assert local_sky_model.status_code == 200
    assert local_sky_model.text.count("W000010") == 20
    for i in range(20):
        assert f"W000010+0000{i:0>2d}" in local_sky_model.text


def test_local_sky_model_query_author(myclient):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query by author to test metadata query.
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": "0",
            "dec_deg": "0",
            "fov_deg": 180,
            "author__contains": "Other",  # Should match the whole of "catalogue2".
        },
    )

    assert local_sky_model.status_code == 200
    assert local_sky_model.text.count("A000100") == 200
    for i in range(200):
        assert f"A000100+000{i:0>3d}" in local_sky_model.text


def test_local_sky_model_query_freq_max(myclient):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query by freq_min_hz to test metadata query.
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": "0",
            "dec_deg": "0",
            "fov_deg": 180,
            "freq_max_hz__gt": 300e6,  # Should match the whole of "catalogue3".
        },
    )

    assert local_sky_model.status_code == 200
    assert local_sky_model.text.count("L000105") == 20
    for i in range(20):
        assert f"L000105+0000{i:0>2d}" in local_sky_model.text


def test_local_sky_model_small_fov(myclient):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query in the region covered by test data (RA ~70, Dec ~4, +-4)
    without version, with fov that only returns 41/200 objects
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": 70,
            "dec_deg": 4,
            "fov_deg": 20,
            "catalogue_name": "catalogue2",
            "version": "1.0.0",
        },
    )

    assert local_sky_model.status_code == 200
    assert local_sky_model.text.count("A000100") == 200
    for i in range(80, 121):
        assert f"A000100+000{i:0>3d}" in local_sky_model.text


def test_local_sky_model_extra_param(myclient):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query in the region covered by test data (RA ~70, Dec ~4, +-20)
    without version, and limiting another value.
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": 70,
            "dec_deg": 4,
            "fov_deg": 20,
            "catalogue_name": "catalogue2",
            "pa_deg__lt": 50,
        },
    )

    assert local_sky_model.status_code == 200
    assert local_sky_model.text.count("A000100") == 50


def test_local_sky_model_flux_range_filter(myclient):
    """Test range filtering on flux values for /local-sky-model."""
    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": 0,
            "dec_deg": 0,
            "fov_deg": 180,
            "catalogue_name": "catalogue1",
            "version": "0.1.0",
            "i_pol_jy__gte": 0.2,
            "i_pol_jy__lt": 0.5,
        },
    )

    assert local_sky_model.status_code == 200
    assert "W000010+000001" in local_sky_model.text
    assert "W000010+000002" in local_sky_model.text
    assert "W000010+000003" in local_sky_model.text
    # the following are out of the flux range of the same catalogue
    assert "W000010+000000" not in local_sky_model.text
    assert "W000010+000004" not in local_sky_model.text
    # the following are not the right catalogue
    assert "A000100" not in local_sky_model.text
    assert "L000105" not in local_sky_model.text


def test_local_sky_model_missing_version(myclient):  # pylint: disable=unused-argument
    """
    Unit test for the /local-sky-model path

    Query in the region covered by test data (RA ~42-50, Dec ~0-7)
    with version that does not exist
    """

    local_sky_model = myclient.get(
        "/local-sky-model/",
        params={
            "ra_deg": "90",
            "dec_deg": "2",
            "fov_deg": 5,
            "version": "2.0.0",
            "catalogue_name": "catalogue",
        },
    )

    assert local_sky_model.status_code == 200
    assert local_sky_model.text.count("0+0") == 0


def test_wait_for_db_success():
    """Test wait_for_db succeeds on first try."""
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_connection

    with patch("ska_sdp_global_sky_model.api.app.main.engine", mock_engine):
        wait_for_db()

    # Verify connection was attempted
    mock_engine.connect.assert_called_once()


def test_wait_for_db_retry():
    """Test wait_for_db retries on failure then succeeds."""
    mock_engine = MagicMock()
    # Fail once, then succeed
    mock_engine.connect.side_effect = [
        Exception("Connection failed"),
        MagicMock(),
    ]

    with (
        patch("ska_sdp_global_sky_model.api.app.main.engine", mock_engine),
        patch("time.sleep") as mock_sleep,
    ):
        wait_for_db()

    # Verify retry occurred
    assert mock_engine.connect.call_count == 2
    mock_sleep.assert_called_once_with(5)


def test_query_metadata_basic(myclient):
    """Test retrieving all metadata records with no filters."""
    response = myclient.get("/catalogue-metadata")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)
    # there are 4 catalogues in the table (see db_session fixture)
    assert len(data) == 4


def test_query_metadata_filter_version_and_name(myclient):
    """Test filtering by version and partial catalogue name."""
    response = myclient.get(
        "/catalogue-metadata",
        params={"version": "0.2.0", "catalogue_name__contains": "ue1"},
    )
    assert response.status_code == 200

    data = response.json()
    assert len(data) == 1
    record = data[0]
    assert record["version"] == "0.2.0"
    assert "catalogue1" in record["catalogue_name"]


def test_query_metadata_sorting(myclient):
    """Test sorting by version descending."""
    response = myclient.get("/catalogue-metadata", params={"sort": "-version"})
    assert response.status_code == 200

    data = response.json()
    # there are 4 catalogues in the table (see db_session fixture)
    assert len(data) == 4
    versions = [r["version"] for r in data]
    assert versions == sorted(versions, reverse=True)


def test_query_metadata_fields_selection(myclient):
    """Test selecting only specific columns."""
    response = myclient.get("/catalogue-metadata", params={"fields": "version,catalogue_name"})
    assert response.status_code == 200

    data = response.json()
    for row in data:
        assert set(row.keys()) == {"version", "catalogue_name"}


def test_query_metadata_pagination(myclient):
    """Test limit parameter."""
    # Limit 2
    response = myclient.get("/catalogue-metadata", params={"limit": "2"})
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2


def test_query_metadata_gt_lt(myclient):
    """Test __gt and __lt operators on version field."""
    # __gt operator
    response = myclient.get("/catalogue-metadata", params={"version__gt": "0.8"})
    assert response.status_code == 200
    data = response.json()
    versions = [r["version"] for r in data]
    assert versions == ["1.0.0", "1.0.5"]
    assert len(versions) == 2

    # __lt operator
    response = myclient.get("/catalogue-metadata", params={"version__lt": "1.0"})
    assert response.status_code == 200
    data = response.json()
    versions = [r["version"] for r in data]
    assert versions == ["0.1.0", "0.2.0"]
    assert len(versions) == 2


def test_query_metadata_gte_lte_range(myclient):
    """Test range filtering on metadata fields."""
    response = myclient.get(
        "/catalogue-metadata",
        params={
            "version__gte": "0.2",
            "version__lte": "3.0",
            "sort": "version",
        },
    )
    assert response.status_code == 200
    data = response.json()

    versions = [row["version"] for row in data]
    assert versions == ["0.2.0", "1.0.0", "1.0.5"]


def test_query_metadata_in_operator(myclient):
    """Test __in operator on catalogue_name."""
    response = myclient.get(
        "/catalogue-metadata", params={"catalogue_name__in": "catalogue1,catalogue3"}
    )
    assert response.status_code == 200
    data = response.json()
    names = [r["catalogue_name"] for r in data]
    assert set(names) == {"catalogue1", "catalogue3"}


def test_query_metadata_combined_operators(myclient):
    """Test combination of filters with sorting and fields selection."""
    response = myclient.get(
        "/catalogue-metadata",
        params={
            "version__gt": "1.0.0",
            "version__lt": "3.0.0",
            "fields": "version,catalogue_name",
            "sort": "-version",
        },
    )
    assert response.status_code == 200
    data = response.json()

    # Should only include version 1.0.5
    assert len(data) == 1
    row = data[0]
    assert row.keys() == {"version", "catalogue_name"}
    assert row["version"] == "1.0.5"
    assert row["catalogue_name"] == "catalogue3"
