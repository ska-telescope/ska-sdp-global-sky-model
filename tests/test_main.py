"""
Basic testing of the API
"""

from fastapi.testclient import TestClient

from ska_sdp_global_sky_model.kubernetes.api.app.main import app

client = TestClient(app)

def test_read_main():
  """Unit test for the root path "/" """
  response = client.get("/ping")
  assert response.status_code == 200
  assert response.json() == {"ping": "live"}
