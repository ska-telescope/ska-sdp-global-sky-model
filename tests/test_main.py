"""
Basic testing of the API
"""

from json import loads
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ska_sdp_global_sky_model.api.main import DataStore, app, get_ds

TEST_DATASTORE: DataStore = DataStore(Path("tests/datasets"))


def override_get_ds():
    """Get the test datastore"""
    # TEST_DATASTORE.
    return TEST_DATASTORE


app.dependency_overrides[get_ds] = override_get_ds


@pytest.fixture(scope="module", name="myclient")
def fixture_client():
    """Trigger the on_startup events - GitLab CI tests
    are not starting the FastAPI app correctly"""
    with TestClient(app) as client:
        # Manually trigger the startup events

        app.router.startup()
        yield client


def test_read_main(myclient):
    """Unit test for the root path "/" """
    response = myclient.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "live"}


# def test_upload_rcal(myclient):
#     """Unit test for the /upload_rcal path"""
#     file_path = "tests/data/rcal.csv"
#     # Open the file in binary mode
#     with open(file_path, "rb") as file:
#         # Create a dictionary with the file
#         files = {"file": file}
#
#         # Send a POST request to the FastAPI endpoint
#         response = myclient.post("/upload-rcal/", files=files)
#
#     assert response.status_code == 200
#     assert response.json() == {"message": "RCAL uploaded and ingested successfully"}


def test_sources(myclient):
    """Unit test for the /local_sky_model path"""
    response = myclient.get("/sources")
    assert response.status_code == 200
    assert len(loads(response.json())) == 228


def test_local_sky_model(myclient):
    """Unit test for the /local_sky_model path"""
    local_sky_model = myclient.get(
        "/local_sky_model/",
        params={"ra": 62, "dec": 15, "telescope": "TEST", "bckwide": 0, "fov": 0.5},
    )

    assert local_sky_model.status_code == 200
    data = "".join(local_sky_model.iter_text())
    assert len(loads(data)) == 10
