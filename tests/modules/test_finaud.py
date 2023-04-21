# coding: utf-8
import pytest

import oar.lib.tools  # for monkeypatching
from oar.lib.models import Resource
from oar.lib.database import ephemeral_session
from oar.modules.finaud import Finaud
from sqlalchemy.orm import scoped_session, sessionmaker

fake_bad_nodes = (1, [])


def set_fake_bad_nodes(bad_nodes):
    global fake_bad_nodes
    fake_bad_nodes = (1, bad_nodes)


def fake_pingchecker(hosts):
    return fake_bad_nodes


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        # add some resources
        for i in range(5):
            Resource.create(session, network_address="localhost" + str(i))
        yield session


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request,setup_config, monkeypatch,):
    monkeypatch.setattr(oar.lib.tools, "pingchecker", fake_pingchecker)


def test_finaud_void(setup_config, minimal_db_initialization):
    config, _, _ = setup_config
    finaud = Finaud(config)
    finaud.run(minimal_db_initialization)
    print(finaud.return_value)
    assert finaud.return_value == 0


def test_finaud_one_bad_node(setup_config,minimal_db_initialization):
    config, _, _ = setup_config
    set_fake_bad_nodes(["localhost0"])
    finaud = Finaud(config)
    finaud.run(minimal_db_initialization)
    set_fake_bad_nodes([])

    print(finaud.return_value)

    resource = minimal_db_initialization.query(Resource).filter(Resource.next_state == "Suspected").first()
    assert resource.network_address == "localhost0"
    assert finaud.return_value == 1


def test_finaud_one_suspected_node_is_not_bad(setup_config,minimal_db_initialization):
    # resources = db.query(Resource).all()
    # import pdb; pdb.set_trace()
    config, _, _ = setup_config
    minimal_db_initialization.query(Resource).filter(Resource.network_address == "localhost0").update(
        {Resource.state: "Suspected", Resource.finaud_decision: "YES"},
        synchronize_session=False,
    )
    finaud = Finaud(config)
    finaud.run(minimal_db_initialization)
    print(finaud.return_value)

    resource = (
        minimal_db_initialization.query(Resource).filter(Resource.network_address == "localhost0").first()
    )
    assert resource.next_state == "Alive"
    assert finaud.return_value == 1
