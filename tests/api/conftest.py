# -*- coding: utf-8 -*-

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.api.app import create_app
from oar.api.dependencies import get_db

# from oar.lib import db
from oar.lib.database import ephemeral_session
from oar.lib.models import Queue, Resource


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


# TODO TOREPLACE
node_list = []


def assign_node_list(nodes):  # TODO TOREPLACE
    global node_list
    node_list = nodes


@pytest.fixture()
def fastapi_app(setup_config):
    config, _, engine = setup_config
    app = create_app(config=config, engine=engine)
    yield app


@pytest.fixture(scope="function")
def client(fastapi_app, minimal_db_initialization, setup_config):
    config, _, db = setup_config
    with TestClient(fastapi_app) as app:
        # override the get_db dependency to inject the test session
        fastapi_app.dependency_overrides[get_db] = lambda: minimal_db_initialization

        yield app

        del fastapi_app.dependency_overrides[get_db]


@pytest.fixture(scope="function", autouse=False)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "create_almighty_socket", lambda x, y: None)
    monkeypatch.setattr(oar.lib.tools, "notify_almighty", lambda x: True)
    monkeypatch.setattr(oar.lib.tools, "notify_bipbip_commander", lambda x: True)
    monkeypatch.setattr(
        oar.lib.tools, "notify_tcp_socket", lambda addr, port, msg: len(msg)
    )
    monkeypatch.setattr(
        oar.lib.tools, "notify_user", lambda job, state, msg: len(state + msg)
    )
    monkeypatch.setattr(
        oar.lib.tools,
        "fork_and_feed_stdin",
        lambda cmd, timeout_cmd, nodes: assign_node_list(nodes),
    )
    monkeypatch.setattr(oar.lib.tools, "signal_oarexec", lambda *x: 0)


@pytest.fixture(scope="function")
def minimal_db_initialization(setup_config, monkeypatch_tools):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        Queue.create(
            session,
            name="default",
            priority=3,
            scheduler_policy="kamelot",
            state="Active",
        )

        # add some resources
        for i in range(10):
            Resource.create(session, network_address="localhost" + str(int(i / 2)))

        yield session
