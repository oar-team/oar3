# -*- coding: utf-8 -*-

import os
import tempfile
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.api.app import create_app
from oar.api.dependencies import get_db
from oar.lib.access_token import create_access_token

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


banned_file = """
{
    "global" : "2023-11-16 18:30:00",
    "revoked" : {
        "bob" : "2023-11-25 18:30:00"
    }
}
"""


def write_banned_file(config):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    banned_file = f"""
        {{
            "global" : "{now}",
            "revoked" : {{
                "old_token" : "{now}"
            }}
        }}
    """

    # Write content
    print(f"{banned_file}", file=open(config["API_REVOKED_TOKENS"], "a"))


@pytest.fixture()
def fastapi_app(setup_config):
    config, engine = setup_config

    tempdir = tempfile.mkdtemp()
    # Config for jwt
    config[
        "API_SECRET_KEY"
    ] = "3f22a0a65212bfb6cdf0dc4b39be189b3c89c6c2c8ed0d1655e0df837145208b"
    config["API_SECRET_ALGORITHM"] = "HS256"
    config["API_ACCESS_TOKEN_EXPIRE_MINUTES"] = 524160  # One year

    config["API_REVOKED_TOKENS"] = os.path.join(tempdir, "tokens_revocation.json")
    write_banned_file(config)

    app = create_app(config=config, engine=engine)
    yield app


@pytest.fixture()
def user_tokens(setup_config):
    config, _ = setup_config
    tokens = {}

    now = datetime.utcnow()
    expires_delta = timedelta(minutes=-15)
    passed_date = now + expires_delta

    tokens["user1"] = create_access_token({"user": "user1"}, config)
    tokens["bob"] = create_access_token({"user": "bob"}, config)
    tokens["oar"] = create_access_token({"user": "oar"}, config)

    tokens["globally_revoked_token"] = create_access_token(
        {"user": "globally_revoked_token"}, config, now=passed_date
    )
    tokens["old_token"] = create_access_token(
        {"user": "old_token"}, config, now=passed_date
    )

    yield tokens


@pytest.fixture(scope="function")
def client(fastapi_app, minimal_db_initialization, setup_config):
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
        oar.lib.tools, "notify_user", lambda session, job, state, msg: len(state + msg)
    )
    monkeypatch.setattr(
        oar.lib.tools,
        "fork_and_feed_stdin",
        lambda cmd, timeout_cmd, nodes: assign_node_list(nodes),
    )
    monkeypatch.setattr(oar.lib.tools, "signal_oarexec", lambda *x, user_signal: 0)


@pytest.fixture(scope="function")
def minimal_db_initialization(setup_config, monkeypatch_tools):
    _, engine = setup_config
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


@pytest.fixture(scope="function", autouse=False)
def with_admission_rules(request, setup_config):
    config, _ = setup_config

    config["ADMISSION_RULES_IN_FILES"] = "yes"
    config["ADMISSION_RULES_PATH"] = os.path.join(
        os.path.dirname(__file__), "..", "lib/etc/oar/admission_rules.d/"
    )

    yield

    config["ADMISSION_RULES_IN_FILES"] = "no"
    config["ADMISSION_RULES_PATH"] = os.path.join(
        "..", os.path.dirname(__file__), "etc/oar/admission_rules.d/"
    )
