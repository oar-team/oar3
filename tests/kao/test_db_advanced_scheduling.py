# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest

from oar.lib import db
from oar.kao.job import insert_job
from oar.kao.meta_sched import meta_schedule

import oar.lib.tools  # for monkeypatching


@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        db['Queue'].create(name='default', priority=3, scheduler_policy='kamelot', state='Active')

        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost" + str(int(i / 2)))
        yield


@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'init_judas_notify_user', lambda: None)
    monkeypatch.setattr(oar.lib.tools, 'create_almighty_socket', lambda: None)
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: len(x))
    monkeypatch.setattr(oar.lib.tools, 'notify_tcp_socket', lambda addr, port, msg: len(msg))
    monkeypatch.setattr(oar.lib.tools, 'notify_user', lambda job, state, msg: len(state + msg))


def test_db_all_in_assgin_default_simple_1(monkeypatch):
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", types=["assign=default"])
    job = db['Job'].query.one()
    print('job state:', job.state)

    # pdb.set_trace()
    meta_schedule('internal')

    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time)

    job = db['Job'].query.one()
    print(job.state)
    assert (job.state == 'toLaunch')


def test_db_all_in_find_default_simple_1(monkeypatch):
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", types=["find=default"])
    job = db['Job'].query.one()
    print('job state:', job.state)

    # pdb.set_trace()
    meta_schedule('internal')

    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time)

    job = db['Job'].query.one()
    print(job.state)
    assert (job.state == 'toLaunch')


def test_db_all_in_assgin_default_params_1(monkeypatch):
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", types=["assign=default:foo.1"])
    job = db['Job'].query.one()
    print('job state:', job.state)

    # pdb.set_trace()
    meta_schedule('internal')

    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time)

    job = db['Job'].query.one()
    print(job.state)
    assert (job.state == 'toLaunch')


def test_db_all_in_find_default_params_1(monkeypatch):
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", types=["find=default:foo.1"])
    job = db['Job'].query.one()
    print('job state:', job.state)

    # pdb.set_trace()
    meta_schedule('internal')

    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time)

    job = db['Job'].query.one()
    print(job.state)
    assert (job.state == 'toLaunch')
