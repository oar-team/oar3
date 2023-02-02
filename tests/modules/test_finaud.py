# coding: utf-8
import pytest

import oar.lib.tools  # for monkeypatching
from oar.lib import Resource, db
from oar.modules.finaud import Finaud

fake_bad_nodes = (1, [])


def set_fake_bad_nodes(bad_nodes):
    global fake_bad_nodes
    fake_bad_nodes = (1, bad_nodes)


def fake_pingchecker(hosts):
    return fake_bad_nodes


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(5):
            Resource.create(network_address="localhost" + str(i))
        yield


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "pingchecker", fake_pingchecker)


def test_finaud_void():
    finaud = Finaud()
    finaud.run()
    print(finaud.return_value)
    assert finaud.return_value == 0


def test_finaud_one_bad_node():
    set_fake_bad_nodes(["localhost0"])
    finaud = Finaud()
    finaud.run()
    set_fake_bad_nodes([])

    print(finaud.return_value)

    resource = db.query(Resource).filter(Resource.next_state == "Suspected").first()
    assert resource.network_address == "localhost0"
    assert finaud.return_value == 1


def test_finaud_one_suspected_node_is_not_bad():
    # resources = db.query(Resource).all()
    # import pdb; pdb.set_trace()
    db.query(Resource).filter(Resource.network_address == "localhost0").update(
        {Resource.state: "Suspected", Resource.finaud_decision: "YES"},
        synchronize_session=False,
    )
    finaud = Finaud()
    finaud.run()
    print(finaud.return_value)

    resource = (
        db.query(Resource).filter(Resource.network_address == "localhost0").first()
    )
    assert resource.next_state == "Alive"
    assert finaud.return_value == 1
