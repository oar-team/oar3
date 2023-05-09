# -*- coding: utf-8 -*-

import pytest

import oar.lib.tools  # for monkeypatching

# from oar.lib import db
from oar.lib.basequery import BaseQuery
from oar.rest_api.app import create_app
from oar.rest_api.query import APIQuery


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


@pytest.fixture(scope="function")
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


@pytest.fixture(scope="function")
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        db["Queue"].create(
            name="default", priority=3, scheduler_policy="kamelot", state="Active"
        )
        # add some resources
        for i in range(10):
            db["Resource"].create(network_address="localhost" + str(int(i / 2)))

        yield


@pytest.fixture
def app(request):
    app = create_app()
    # force to use APIQuery needed when all tests are launched and previous ones have set BaseQuery
    db.sessionmaker.configure(query_cls=APIQuery)

    @request.addfinalizer
    def teardown():
        db.sessionmaker.configure(query_cls=BaseQuery)

    return app
