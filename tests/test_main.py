# pylint: disable=no-member
"""
Basic testing of the API
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ska_sdp_global_sky_model.api.app.main import Base, app, get_db
from ska_sdp_global_sky_model.configuration.config import DB_URL, q3c_index

SQLALCHEMY_DATABASE_URL = DB_URL

engine = create_engine(SQLALCHEMY_DATABASE_URL)

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    """
    Create a local testing session.
    """
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


@pytest.fixture()
def test_db():
    """
    Database for test purposes.
    """
    Base.metadata.create_all(bind=engine)
    q3c_index()
    yield
    Base.metadata.drop_all(bind=engine)


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(scope="module", name="myclient")
def fixture_client():
    """Trigger the on_startup events - GitLab CI tests
    are not starting the FastAPI app correctly"""
    with TestClient(app) as client:
        # Manually trigger the startup events
        Base.metadata.create_all(bind=engine)
        q3c_index()
        yield client


def test_read_main(myclient):
    """Unit test for the root path "/" """
    response = myclient.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "live"}


def test_upload_rcal(myclient):
    """Unit test for the /upload_rcal path"""
    file_path = "tests/data/rcal.csv"
    # Open the file in binary mode
    with open(file_path, "rb") as file:
        # Create a dictionary with the file
        files = {"file": ("rcal.csv", file, "text/csv")}

        # Send a POST request to the FastAPI endpoint
        response = myclient.post("/upload-rcal", files=files)

    assert response.status_code == 200
    assert response.json() == {"message": "RCAL uploaded and ingested successfully"}


def test_sources(myclient):
    """Unit test for the /local_sky_model path"""
    file_path = "tests/data/rcal.csv"
    # Open the file in binary mode
    with open(file_path, "rb") as file:
        # Create a dictionary with the file
        files = {"file": file}

        # Send a POST request to the FastAPI endpoint
        myclient.post("/upload-rcal/", files=files)
    response = myclient.get("/sources")
    assert response.status_code == 200
    assert response.json()[0][0] == "J235613-743047"


def test_local_sky_model(myclient):
    """Unit test for the /local_sky_model path"""
    file_path = "tests/data/rcal.csv"
    # Open the file in binary mode
    with open(file_path, "rb") as file:
        # Create a dictionary with the file
        files = {"file": file}

        # Send a POST request to the FastAPI endpoint
        myclient.post("/upload-rcal/", files=files)

    local_sky_model = myclient.get(
        "/local_sky_model/",
        params={"ra": 359, "dec": -74, "telescope": "MWA", "flux_wide": 0, "fov": 1},
    )

    assert local_sky_model.status_code == 200
    assert len(local_sky_model.json()) >= 1
