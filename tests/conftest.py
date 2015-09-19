# coding: utf-8
from __future__ import unicode_literals, print_function

import os
import tempfile
import shutil
import pytest

from codecs import open

from oar.lib import config, db
from oar.lib.compat import iteritems
from . import DEFAULT_CONFIG


@pytest.fixture(scope="session", autouse=True)
def setup_config(request):
    config.clear()
    config.update(DEFAULT_CONFIG.copy())
    tempdir = tempfile.mkdtemp()
    config["LOG_FILE"] = os.path.join(tempdir, 'oar.log')

    db_type = os.environ.get('DB_TYPE', 'memory')

    if db_type not in ('memory', 'sqlite', 'postgresql'):
        raise ValueError("Unsupported database '%s'" % db_type)

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
        config['DB_BASE_PASSWD_RO'] = 'oar'
        config['DB_BASE_LOGIN_RO'] = 'oar'
        config['DB_HOSTNAME'] = 'localhost'

    def dump_configuration(filename):
        with open(filename, 'w', encoding='utf-8') as fd:
            for key, value in iteritems(config):
                if not key.startswith('SQLALCHEMY_'):
                    fd.write("%s=%s\n" % (key, str(value)))

    @request.addfinalizer
    def teardown():
        shutil.rmtree(tempdir)

    dump_configuration('/etc/oar/oar.conf')
    db.create_all()
    db.reflect()
    db.delete_all()
