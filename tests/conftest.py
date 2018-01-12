# coding: utf-8

import os
import tempfile
import shutil

from codecs import open

import pytest

from oar.lib import config, db
from . import DEFAULT_CONFIG

from oar.lib.tools import TimeoutExpired

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

    def dump_configuration(filename):
        folder = os.path.dirname(filename)
        if not os.path.exists(folder):
            os.makedirs(folder)
        with open(filename, 'w', encoding='utf-8') as fd:
            for key, value in config.items():
                if not key.startswith('SQLALCHEMY_'):
                    fd.write("%s=%s\n" % (key, str(value)))

    dump_configuration('/tmp/oar.conf')
    db.metadata.drop_all(bind=db.engine)
    db.create_all(bind=db.engine)
    kw = {"nullable": True}
    db.op.add_column('resources', db.Column('core', db.Integer, **kw))
    db.op.add_column('resources', db.Column('cpu', db.Integer, **kw))
    db.op.add_column('resources', db.Column('host', db.String(255), **kw))
    db.op.add_column('resources', db.Column('mem', db.Integer, **kw))
    db.reflect()
    yield
    db.close()
    shutil.rmtree(tempdir)
    
fake_popen = {'wait_return_code': 0, 'exception': None}
class FakePopen(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.pid = 111

    def wait(self, timeout):
        print(timeout)
        #import pdb; pdb.set_trace()
        if fake_popen['exception']:
            if fake_popen['exception'] == 'OSError':
                raise OSError
            elif fake_popen['exception'] == 'TimeoutExpired':
                raise TimeoutExpired(cmd=self.cmd, timeout=timeout)
        return(fake_popen['wait_return_code']) 
    
