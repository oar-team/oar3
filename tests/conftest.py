import os
from tempfile import mkstemp

import pytest

from sqlalchemy import Column, Integer, String
from oar.lib import config, db
from oar.lib.fixture import load_fixtures

from . import DEFAULT_CONFIG


@pytest.fixture(scope="session", autouse=True)
def setup_config_and_create_database_schema(request):
    # Create the tables based on the current model
    config.clear()
    config.update(DEFAULT_CONFIG.copy())
    _, config["LOG_FILE"] = mkstemp()

    db.create_all()
    db.op.add_column('resources', Column('core', Integer()))
    db.op.add_column('resources', Column('cpu', Integer()))
    db.op.add_column('resources', Column('host', String()))
    db.op.add_column('resources', Column('mem', Integer()))


@pytest.fixture(scope='module', autouse=True)
def populate_database(request):
    # populate database
    here = os.path.abspath(os.path.dirname(__file__))
    load_fixtures(db, os.path.join(here, "data", "dataset_1.json"), clear=True)


@pytest.fixture(autouse=True)
def db_session(request, monkeypatch):
    # Roll back at the end of every test
    request.addfinalizer(db.session.remove)
    # Prevent the session from closing (make it a no-op)
    monkeypatch.setattr(db.session, 'remove', lambda: None)
