import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock

from api.app.main import app
from api.app.services.snowflake_client import SnowflakeClient


@pytest.fixture
def mock_sf_client():
    """Mock Snowflake client for testing without real connection."""
    client = MagicMock(spec=SnowflakeClient)
    return client


@pytest.fixture
def test_client(mock_sf_client):
    """FastAPI test client with mocked Snowflake."""
    app.state.sf_client = mock_sf_client
    return TestClient(app)
