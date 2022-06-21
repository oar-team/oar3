# -*- coding: utf-8 -*-

import pytest
from fastapi.testclient import TestClient

import oar.lib.tools  # for monkeypatching
from oar.api.query import APIQuery
from oar.lib import db
from oar.lib.basequery import BaseQuery


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
def fastapi_app():
    from oar.api.app import create_app

    app = create_app()

    # force to use APIQuery needed when all tests are launched and previous ones have set BaseQuery
    db.sessionmaker.configure(query_cls=APIQuery)

    yield app

    db.sessionmaker.configure(query_cls=BaseQuery)


@pytest.fixture()
def client(fastapi_app):
    with TestClient(fastapi_app) as app:
        yield app


@pytest.fixture(scope="function", autouse=False)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "create_almighty_socket", lambda: None)
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


@pytest.fixture(scope="function", autouse=False)
def monkeypatch_scoped_session(request, monkeypatch):
    from sqlalchemy.util import ScopedRegistry

    monkeypatch.setattr(
        db.session,
        "registry",
        ScopedRegistry(db.session.session_factory, lambda: request.node.name),
    )


@pytest.fixture(scope="function")
def minimal_db_initialization(client, monkeypatch_tools, monkeypatch_scoped_session):
    with db.session(ephemeral=True):
        db["Queue"].create(
            name="default", priority=3, scheduler_policy="kamelot", state="Active"
        )
        # add some resources
        for i in range(10):
            db["Resource"].create(network_address="localhost" + str(int(i / 2)))

        yield
