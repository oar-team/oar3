# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest
import os
import os.path
import re

from oar.lib import (config, db, Resource, Job, Queue, GanttJobsPrediction)
from oar.kao.job import insert_job
from oar.kao.meta_sched import meta_schedule

import oar.kao.utils  # for monkeypatching
from oar.kao.utils import get_date

from __init__ import DEFAULT_CONFIG

# def update_conf(fs=False):
#    if fs:
#        DEFAULT_CONFIG['FAIRSHARING_ENABLED'] = 'yes'
#    config.update(DEFAULT_CONFIG.copy())


@pytest.fixture(scope='module', autouse=True)
def generate_oar_conf(request):

    print("generate_oar_conf")

    #print(config['DB_BASE_FILE'])

    # old_config_db_base_file = config['DB_BASE_FILE']
    DEFAULT_CONFIG['DB_BASE_FILE'] = "/tmp/oar.sqlite"
    config.update(DEFAULT_CONFIG.copy())
    config['DB_BASE_FILE'] = DEFAULT_CONFIG['DB_BASE_FILE']

    file = open("/etc/oar/oar.conf", 'w')
    for key, value in config.iteritems():
        if not re.search(r'SQLALCHEMY_', key):
            # print key, value
            file.write(key + '="' + str(value) + '"\n')
    file.close()

    def teardown():
        # config['DB_BASE_FILE'] = old_config_db_base_file
        os.remove('/etc/oar/oar.conf')

    request.addfinalizer(teardown)


@pytest.fixture(scope='module', autouse=True)
def setup_db_file(request):

    db_file = config['DB_BASE_FILE']

    print("setup_db_file: ", db_file)
    if os.path.isfile(db_file):
        db.delete_all()

    db.create_all()
    print("setup_db_file: end")


@pytest.fixture(scope="function", autouse=True)
def minimal_db_intialization(request):
    # pdb.set_trace()
    print("set default queue")
    db.add(Queue(name='default', priority=3, scheduler_policy='kamelot', state='Active'))

    print("add resources")
    # add some resources
    for i in range(5):
        db.add(Resource(network_address="localhost"))

    db_flush()
    # pdb.set_trace()

    def teardown():
        db.delete_all()

    request.addfinalizer(teardown)


def db_flush():
    db.session.flush()
    db.session.expunge_all()
    db.session.commit()


@pytest.fixture(scope='function', autouse=True)
def monkeypatch_utils(request, monkeypatch):
    monkeypatch.setattr(oar.kao.utils, 'init_judas_notify_user', lambda: None)
    monkeypatch.setattr(oar.kao.utils, 'create_almighty_socket', lambda: None)
    monkeypatch.setattr(oar.kao.utils, 'notify_almighty', lambda x: len(x))
    monkeypatch.setattr(oar.kao.utils, 'notify_tcp_socket', lambda addr, port, msg: len(msg))
    monkeypatch.setattr(oar.kao.utils, 'notify_user', lambda job, state, msg: len(state + msg))


def test_db_metasched_simple_1(monkeypatch):

    print("DB_BASE_FILE: ", config["DB_BASE_FILE"])
    insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    db_flush()
    job = db.query(Job).one()
    print('job state:', job.state)
    # plt = Platform()
    # r = plt.resource_set()

    # pdb.set_trace()

    meta_schedule()

    # retrieve jobs
    # jobs = {job.id: job for job in db.query(Job).all()}

    # pdb.set_trace()

    for i in db.query(GanttJobsPrediction).all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time)

    # pdb.set_trace()
    job = db.query(Job).one()
    print(job.state)
    assert job.state == 'toLaunch'


def test_db_metasched_ar_1(monkeypatch):

    # add one job
    now = get_date()
    # sql_now = local_to_sql(now)

    insert_job(res=[(60, [('resource_id=4', "")])], properties="",
               reservation='toSchedule', start_time=(now + 10),
               info_type='localhost:4242')
    db_flush()

    # plt = Platform()
    # r = plt.resource_set()

    meta_schedule()

    job = db.query(Job).one()
    print(job.state, ' ', job.reservation)

    assert ((job.state == 'Waiting') and (job.reservation == 'Scheduled'))
