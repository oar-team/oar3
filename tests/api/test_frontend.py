from oar import VERSION
from oar.api import API_VERSION


def test_root(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"api_version": API_VERSION, "oar_version": VERSION}


def test_whoami_nouser(client):
    response = client.get("/whoami")
    assert response.status_code == 200
    assert response.json() == {"user": None}


def test_whoami_user(client):
    response = client.get("/whoami", headers={"X_REMOTE_IDENT": "bob"})
    assert response.status_code == 200
    assert response.json() == {"user": "bob"}
