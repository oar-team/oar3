# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest

from oar.lib import (db, Resource, Job, Queue, GanttJobsPrediction)
from oar.kao.job import insert_job
from oar.kao.meta_sched import meta_schedule

import oar.kao.utils  # for monkeypatching
from oar.kao.utils import get_date
# import pdb


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


def test_db_all_in_one_simple_1(monkeypatch):

    insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    db_flush()
    job = db.query(Job).one()
    print('job state:', job.state)

    # pdb.set_trace()
    meta_schedule('internal')

    for i in db.query(GanttJobsPrediction).all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time)

    job = db.query(Job).one()
    print(job.state)
    assert (job.state == 'toLaunch')


def test_db_all_in_one_ar_1(monkeypatch):

    # add one job
    now = get_date()
    # sql_now = local_to_sql(now)

    insert_job(res=[(60, [('resource_id=4', "")])], properties="",
               reservation='toSchedule', start_time=(now + 10),
               info_type='localhost:4242')
    db_flush()

    # plt = Platform()
    # r = plt.resource_set()

    meta_schedule('internal')

    job = db.query(Job).one()
    print(job.state, ' ', job.reservation)

    assert ((job.state == 'Waiting') and (job.reservation == 'Scheduled'))
