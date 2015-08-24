from __future__ import unicode_literals, print_function
import pytest

from oar.lib import db
from oar.kao.job import (insert_job, set_job_state)
from oar.kao.meta_sched import meta_schedule

import oar.kao.utils  # for monkeypatching
from oar.kao.utils import get_date


@pytest.fixture(scope="function", autouse=True)
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
        db['Resource'].create(network_address="localhost" + str(int(i / 2)))

    @request.addfinalizer
    def teardown():
        db.delete_all()
        db.session.close()


@pytest.fixture(scope='function', autouse=True)
def monkeypatch_utils(request, monkeypatch):
    monkeypatch.setattr(oar.kao.utils, 'init_judas_notify_user', lambda: None)
    monkeypatch.setattr(oar.kao.utils, 'create_almighty_socket', lambda: None)
    monkeypatch.setattr(oar.kao.utils, 'notify_almighty', lambda x: len(x))
    monkeypatch.setattr(oar.kao.utils, 'notify_tcp_socket', lambda addr, port, msg: len(msg))
    monkeypatch.setattr(oar.kao.utils, 'notify_user', lambda job, state, msg: len(state + msg))


@pytest.mark.skipif("os.environ.get('TRAVIS', '') == 'true'")
def test_db_timesharing_1(monkeypatch):
    now = get_date()
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", types=["timesharing=*,*"])

    insert_job(res=[(60, [('resource_id=4', "")])], properties="", types=["timesharing=*,*"])

    meta_schedule('internal')

    for j in db['Job'].query.all():
        print(j.state)

    res = []
    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] == res[1]


def test_db_timesharing_2(monkeypatch):
    now = get_date()
    insert_job(res=[(60, [('resource_id=4', "")])], properties="",
               types=["timesharing=user,*"], user='toto')
    insert_job(res=[(60, [('resource_id=4', "")])], properties="",
               types=["timesharing=user,*"], user='titi')
    meta_schedule('internal')

    for j in db['Job'].query.all():
        print(j.state)

    res = []
    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] != res[1]


def test_db_timesharing_3(monkeypatch):
    now = get_date()
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", types=["timesharing=*,*"])

    insert_job(res=[(60, [('resource_id=4', "")])], properties="", types=["timesharing=*,*"])

    meta_schedule('internal')

    for j in db['Job'].query.all():
        print(j.id, j.state)

    res = []
    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] == res[1]


def test_db_properties_1(monkeypatch):
    now = get_date()
    insert_job(res=[(60, [('resource_id=2', "")])], properties="network_address='localhost1'")
    insert_job(res=[(60, [('resource_id=2', "")])], properties="network_address='localhost1'")

    meta_schedule('internal')

    for j in db['Job'].query.all():
        print(j.state)

    res = []
    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] != res[1]


def test_db_properties_2(monkeypatch):
    now = get_date()
    insert_job(res=[(60, [('resource_id=2', "network_address='localhost1'")])], properties="")
    insert_job(res=[(60, [('resource_id=2', "network_address='localhost1'")])], properties="")

    meta_schedule('internal')

    for j in db['Job'].query.all():
        print(j.state)

    res = []
    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] != res[1]


def test_db_properties_3(monkeypatch):
    now = get_date()
    insert_job(res=[(60, [('resource_id=2', "network_address='localhost0'")])],
               properties="network_address='localhost0'")
    insert_job(res=[(60, [('resource_id=2', "network_address='localhost1'")])],
               properties="network_address='localhost1'")
    meta_schedule('internal')

    for j in db['Job'].query.all():
        print(j.state)

    res = []
    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] == res[1]


@pytest.mark.skipif("os.environ.get('TRAVIS', '') == 'true'")
def test_db_placeholder_1(monkeypatch):
    now = get_date()
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", types=["placeholder=yop"])
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", types=["allow=yop"])
    meta_schedule('internal')

    for j in db['Job'].query.all():
        print(j.state)

    res = []
    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] == res[1]


def test_db_placeholder_2(monkeypatch):
    now = get_date()
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", types=["placeholder=yop"])
    insert_job(res=[(60, [('resource_id=4', "")])], properties="", types=["allow=poy"])
    meta_schedule('internal')

    for j in db['Job'].query.all():
        print(j.state)

    res = []
    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] != res[1]


def test_db_moldable_1(monkeypatch):
    now = get_date()
    insert_job(res=[(60, [('resource_id=3', "")])], properties="")
    insert_job(res=[(60, [('resource_id=4', "")]), (70, [('resource_id=3', "")])], properties="")
    meta_schedule('internal')

    for j in db['Job'].query.all():
        print(j.state)

    res = []
    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] != res[1]


def test_db_moldable_2(monkeypatch):
    now = get_date()
    insert_job(res=[(60, [('resource_id=3', "")])], properties="")
    insert_job(res=[(60, [('resource_id=4', "")]), (70, [('resource_id=2', "")])], properties="")
    meta_schedule('internal')

    for j in db['Job'].query.all():
        print(j.state)

    res = []
    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time - now)
        res.append(i.start_time - now)

    assert res[0] == res[1]


def test_db_suspended_duration_1(monkeypatch):
    insert_job(res=[(60, [('resource_id=3', "")])], properties="", suspended='YES')
    meta_schedule('internal')
    job = db['Job'].query.one()
    assert (job.state == 'toLaunch')
    # set_job_state(job.id, 'Running')

