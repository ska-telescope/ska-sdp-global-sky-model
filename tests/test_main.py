"""
Basic testing of the API
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ska_sdp_global_sky_model.api.app.main import Base, app, get_db

SQLALCHEMY_DATABASE_URL = "postgresql://postgres:pass@db:5432/postgres"

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

client = TestClient(app)


def test_read_main():
    """Unit test for the root path "/" """
    response = client.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "live"}
