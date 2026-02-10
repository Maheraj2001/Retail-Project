import sys
import os
import pytest
from unittest.mock import MagicMock

# Add api/ to path so `app.*` imports resolve correctly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from fastapi.testclient import TestClient
from app.main import app
from app.services.local_client import LocalClient


@pytest.fixture
def mock_sf_client():
    """Mock data client for testing without real connection."""
    client = MagicMock(spec=LocalClient)
    return client


@pytest.fixture
def test_client(mock_sf_client):
    """FastAPI test client with mocked data client."""
    app.state.sf_client = mock_sf_client
    return TestClient(app)
