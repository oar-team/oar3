# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest
import re
from tempfile import mkstemp
from oar.lib import config, db


from __init__ import DEFAULT_CONFIG


@pytest.fixture(scope="module", autouse=True)
def setup_config(request):
    if not re.search(r'test_db_metasched', request.node.name):
        print("setup_config")
        config.clear()
        config.update(DEFAULT_CONFIG.copy())
        _, config["LOG_FILE"] = mkstemp()

    # if re.search(r'test_db_metasched', request.node.name):
    #    _, config["LOG_FILE"] = mkstemp()


@pytest.fixture(scope='module', autouse=True)
def setup_db(request):

    if re.search(r'test_db', request.node.name) and\
       not re.search(r'test_db_metasched', request.node.name):
        print("setup_db")
        # Create the tables based on the current model
        db.create_all()
        # Add base data here
        # ...
        db.session.flush()
        db.session.expunge_all()
        db.session.commit()

        def teardown():
            db.delete_all()

        request.addfinalizer(teardown)


@pytest.fixture(autouse=True)
def db_session(request, monkeypatch):
    if re.search(r'test_db', request.node.name):
        # Roll back at the end of every test
        request.addfinalizer(db.session.remove)
        # Prevent the session from closing (make it a no-op)
        monkeypatch.setattr(db.session, 'remove', lambda: None)
