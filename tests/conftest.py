import os

os.environ["DATABASE_URL"] = os.getenv("TEST_DATABASE_URL", "sqlite:///./test_monitor.db")
os.environ.pop("OPENAI_API_KEY", None)

import pytest
from fastapi.testclient import TestClient

from app.db import drop_all
from app.main import app


@pytest.fixture()
def client():
    drop_all()
    with TestClient(app) as c:  # startup creates tables (and views on Postgres)
        yield c
    drop_all()
