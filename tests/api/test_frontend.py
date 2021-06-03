from fastapi.testclient import TestClient

from oar import VERSION
from oar.api import API_VERSION
from oar.api.app import app

client = TestClient(app)


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"api_version": API_VERSION, "oar_version": VERSION}


def test_whoami_nouser():
    response = client.get("/whoami")
    assert response.status_code == 200
    assert response.json() == {"user": None}


def test_whoami_user():
    response = client.get("/whoami", headers={"X-REMOTE-IDENT": "bob"})
    assert response.status_code == 200
    assert response.json() == {"user": "bob"}
