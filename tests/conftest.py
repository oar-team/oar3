# coding: utf-8
from __future__ import unicode_literals, print_function

import os
import tempfile
import shutil
import pytest

from oar.lib import config, db
from . import DEFAULT_CONFIG


@pytest.yield_fixture(scope='session', autouse=True)
def setup_config(request):
    config.clear()
    config.update(DEFAULT_CONFIG.copy())
    tempdir = tempfile.mkdtemp()
    config["LOG_FILE"] = os.path.join(tempdir, 'oar.log')

    db_type = os.environ.get('DB_TYPE', 'memory')
    os.environ.setdefault('DB_TYPE', db_type)

    if db_type not in ('memory', 'sqlite', 'postgresql'):
        pytest.exit("Unsupported database '%s'" % db_type)

    if db_type == "sqlite":
        config['DB_BASE_FILE'] = os.path.join(tempdir, 'db.sqlite')
        config['DB_TYPE'] = 'sqlite'
    elif db_type == "memory":
        config['DB_TYPE'] = 'sqlite'
        config['DB_BASE_FILE'] = ':memory:'
    else:
        config['DB_TYPE'] = 'Pg'
        config['DB_PORT'] = '5432'
        config['DB_BASE_NAME'] = 'oar'
        config['DB_BASE_PASSWD'] = 'oar'
        config['DB_BASE_LOGIN'] = 'oar'
        config['DB_BASE_PASSWD_RO'] = 'oar_ro'
        config['DB_BASE_LOGIN_RO'] = 'oar_ro'
        config['DB_HOSTNAME'] = 'localhost'

    db.metadata.drop_all(bind=db.engine)
    db.create_all(bind=db.engine)
    # with db.session(ephemeral=True, reflect=False):
    kw = {"nullable": True}
    db.op.add_column('resources', db.Column('core', db.Integer, **kw))
    db.op.add_column('resources', db.Column('cp', db.Integer, **kw))
    db.op.add_column('resources', db.Column('host', db.String(255), **kw))
    db.op.add_column('resources', db.Column('mem', db.Integer, **kw))
    db.reflect()
    yield
    shutil.rmtree(tempdir)
