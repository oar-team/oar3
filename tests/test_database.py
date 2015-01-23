# -*- coding: utf-8 -*-
import pytest

from sqlalchemy.orm.util import object_state

from tempfile import mkstemp
from . import DEFAULT_CONFIG
# from .helpers import generate_fake_data

from oar.lib import Resource
from oar.lib import config, db as initial_db


@pytest.fixture(scope="session")
def db(request):
    config.clear()
    config.update(DEFAULT_CONFIG)
    _, config["LOG_FILE"] = mkstemp()
    def teardown():
        initial_db.close()
    request.addfinalizer(teardown)

    initial_db.create_all()
    initial_db.reflect()
    # generate_fake_data(initial_db)
    return initial_db


def test_db_insert(db):
    r1 = Resource(id=100)
    db.add(r1)
    assert object_state(r1).pending is True
    db.flush()
    assert object_state(r1).persistent is True


def test_db_query(db):
    res = db.query(Resource).order_by(Resource.id.asc()).first()
    assert res.id == 100
