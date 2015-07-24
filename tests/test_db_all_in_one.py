# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest

import os
from codecs import open
from tempfile import mkstemp

from oar.lib import db, config
from oar.kao.job import insert_job
from oar.kao.meta_sched import meta_schedule

import oar.kao.utils  # for monkeypatching
from oar.kao.utils import get_date
import oar.kao.quotas as qts


@pytest.fixture(scope="module", autouse=True)
def create_db(request):
    db.create_all()
    db.reflect()
    db.delete_all()

    @request.addfinalizer
    def teardown():
        db.delete_all()
        db.session.close()


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    db['Queue'].create(name='default', priority=3, scheduler_policy='kamelot', state='Active')

    # add some resources
    for i in range(5):
        db['Resource'].create(network_address="localhost")

    @request.addfinalizer
    def teardown():
        db.delete_all()
        db.session.close()


@pytest.fixture(scope="function")
def active_quotas(request):
    print('active_quotas')
    config['QUOTAS'] = 'yes'
    _, quotas_file_name = mkstemp()
    config['QUOTAS_FILE'] = quotas_file_name

    def teardown():
        config['QUOTAS'] = 'no'
        os.remove(config['QUOTAS_FILE'])
        del config['QUOTAS_FILE']

    request.addfinalizer(teardown)


def create_quotas_rules_file(quotas_rules):
    ''' create_quotas_rules_file('{"quotas": {"*,*,*,toto": [1,-1,-1],"*,*,*,john": [150,-1,-1]}}')
    '''
    with open(config['QUOTAS_FILE'], 'w', encoding="utf-8") as quotas_fd:
        quotas_fd.write(quotas_rules)
    qts.load_quotas_rules()


@pytest.fixture(scope='function', autouse=True)
def monkeypatch_utils(request, monkeypatch):
    monkeypatch.setattr(oar.kao.utils, 'init_judas_notify_user', lambda: None)
    monkeypatch.setattr(oar.kao.utils, 'create_almighty_socket', lambda: None)
    monkeypatch.setattr(oar.kao.utils, 'notify_almighty', lambda x: len(x))
    monkeypatch.setattr(oar.kao.utils, 'notify_tcp_socket', lambda addr, port, msg: len(msg))
    monkeypatch.setattr(oar.kao.utils, 'notify_user', lambda job, state, msg: len(state + msg))


def test_db_all_in_one_simple_1(monkeypatch):
    insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    job = db['Job'].query.one()
    print('job state:', job.state)

    # pdb.set_trace()
    meta_schedule('internal')

    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time)

    job = db['Job'].query.one()
    print(job.state)
    assert (job.state == 'toLaunch')


def test_db_all_in_one_ar_1(monkeypatch):
    # add one job
    now = get_date()
    # sql_now = local_to_sql(now)

    #
    insert_job(res=[(60, [('resource_id=4', "")])], properties="",
               reservation='toSchedule', start_time=(now + 10),
               info_type='localhost:4242')

    # plt = Platform()
    # r = plt.resource_set()

    meta_schedule('internal')

    job = db['Job'].query.one()
    print(job.state, ' ', job.reservation)

    assert ((job.state == 'Waiting') and (job.reservation == 'Scheduled'))


@pytest.mark.usefixtures("active_quotas")
def test_db_all_in_one_quotas_1(monkeypatch):
    """
    quotas[queue, project, job_type, user] = [int, int, float];
                                               |    |     |
              maximum used resources ----------+    |     |
              maximum number of running jobs -------+     |
              maximum resources times (hours) ------------+
    """

    create_quotas_rules_file('{"quotas": {"*,*,*,/": [-1, 1, -1], "/,*,*,*": [-1, -1, 0.55]}}')

    insert_job(res=[(100, [('resource_id=1', "")])], properties="", user="toto")
    insert_job(res=[(200, [('resource_id=1', "")])], properties="", user="toto")
    insert_job(res=[(200, [('resource_id=1', "")])], properties="", user="toto")

    # pdb.set_trace()
    now = get_date()
    meta_schedule('internal')

    res = []
    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time - now)
        res.append(i.start_time - now)

    assert res == [0, 160, 420]


@pytest.mark.usefixtures("active_quotas")
def test_db_all_in_one_quotas_2(monkeypatch):
    """
    quotas[queue, project, job_type, user] = [int, int, float];
                                               |    |     |
              maximum used resources ----------+    |     |
              maximum number of running jobs -------+     |
              maximum resources times (hours) ------------+
    """

    create_quotas_rules_file('{"quotas": {"*,*,*,/": [-1, 1, -1]}}')

    # Submit and allocate an Advance Reservation
    t0 = get_date()
    insert_job(res=[(60, [('resource_id=1', "")])], properties="",
               reservation='toSchedule', start_time=(t0 + 100),
               info_type='localhost:4242')
    meta_schedule('internal')

    # Submit other jobs
    insert_job(res=[(100, [('resource_id=1', "")])], properties="", user="toto")
    insert_job(res=[(200, [('resource_id=1', "")])], properties="", user="toto")

    # pdb.set_trace()
    t1 = get_date()
    meta_schedule('internal')

    res = []
    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time - t1)
        res.append(i.start_time - t1)

    assert (res[1] - res[0]) == 120
    assert (res[2] - res[0]) == 280


@pytest.mark.usefixtures("active_quotas")
def test_db_all_in_one_quotas_AR(monkeypatch):
    create_quotas_rules_file('{"quotas": {"*,*,*,*": [1, -1, -1]}}')

    now = get_date()

    insert_job(res=[(60, [('resource_id=4', "")])],
               reservation='toSchedule', start_time=(now + 10),
               info_type='localhost:4242')

    meta_schedule('internal')

    job = db['Job'].query.one()
    print(job.state, ' ', job.reservation)

    assert job.state == 'Error'


def test_db_all_in_one_BE(monkeypatch):

    db['Queue'].create(name='besteffort', priority=3, scheduler_policy='kamelot', state='Active')

    insert_job(res=[(100, [('resource_id=1', "")])], queue_name='besteffort', types='besteffort')

    meta_schedule('internal')

    job = db['Job'].query.one()
    print(job.state)
    assert (job.state == 'toLaunch')


def test_db_all_in_one_BE_to_kill(monkeypatch):

    os.environ['USER'] = 'root'  # to allow fragging
    db['Queue'].create(name='besteffort', priority=3, scheduler_policy='kamelot', state='Active')

    insert_job(res=[(100, [('resource_id=2', "")])], queue_name='besteffort', types='besteffort')

    meta_schedule('internal')

    job = db['Job'].query.one()
    assert (job.state == 'toLaunch')

    insert_job(res=[(100, [('resource_id=5', "")])])

    meta_schedule('internal')

    jobs = db['Job'].query.all()

    print(jobs[0].state, jobs[1].state)

    print("frag...", db['FragJob'].query.one())
    frag_job = db['FragJob'].query.one()
    assert jobs[0].state == 'toLaunch'
    assert jobs[1].state == 'Waiting'
    assert frag_job.job_id ==  jobs[0].id
