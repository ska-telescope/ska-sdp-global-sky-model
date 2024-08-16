# pylint: disable=no-member
"""
Basic testing of the API
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ska_sdp_global_sky_model.api.app.main import Base, app, get_db
from ska_sdp_global_sky_model.configuration.config import DB_URL

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
    yield
    Base.metadata.drop_all(bind=engine)


app.dependency_overrides[get_db] = override_get_db


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


def test_upload_rcal(myclient):
    """Unit test for the /upload_rcal path"""
    file_path = "tests/data/rcal.csv"
    # Open the file in binary mode
    with open(file_path, "rb") as file:
        # Create a dictionary with the file
        files = {"file": file}

        # Send a POST request to the FastAPI endpoint
        response = myclient.post("/upload-rcal/", files=files)

    assert response.status_code == 200
    assert response.json() == {"message": "RCAL uploaded and ingested successfully"}

    # response = client.get("/sources")
