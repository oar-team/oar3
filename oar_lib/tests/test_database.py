# -*- coding: utf-8 -*-
import pytest

from sqlalchemy.orm.util import object_state

from . import get_default_config

from oar.lib import Resource
from oar.lib import config, db


@pytest.fixture(scope="session", autouse=True)
def setup_config(request):
    config.clear()
    config.update(get_default_config())


@pytest.fixture(scope='session', autouse=True)
def setup_db(request):
    # Create the tables based on the current model
    db.create_all()
    # Add base data here
    # ...
    db.session.flush()
    db.session.expunge_all()
    db.session.commit()


@pytest.fixture(autouse=True)
def db_session(request, monkeypatch):
    # Roll back at the end of every test
    request.addfinalizer(db.session.remove)
    # Prevent the session from closing (make it a no-op)
    monkeypatch.setattr(db.session, 'remove', lambda: None)


def test_db_insert():
    r1 = Resource(id=100)
    db.add(r1)
    assert object_state(r1).pending is True
    db.flush()
    assert object_state(r1).persistent is True
    db.commit()


def test_db_query():
    res = db.query(Resource).order_by(Resource.id.asc()).first()
    assert res.id == 100
