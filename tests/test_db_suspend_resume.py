from __future__ import unicode_literals, print_function
import pytest

from oar.lib import (db, config)
from oar.kao.job import (insert_job, set_job_state)
from oar.kao.meta_sched import meta_schedule

import oar.kao.utils  # for monkeypatching


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


@pytest.fixture(scope="function")
def config_suspend_resume(request):

    config['JUST_BEFORE_RESUME_EXEC_FILE'] = 'true'
    config['SUSPEND_RESUME_SCRIPT_TIMEOUT'] = '1'

    def teardown():
        del config['JUST_BEFORE_RESUME_EXEC_FILE']
        del config['SUSPEND_RESUME_SCRIPT_TIMEOUT']

    request.addfinalizer(teardown)


@pytest.mark.usefixtures('config_suspend_resume')
def test_suspend_resume_1(monkeypatch):
    # now = get_date()
    insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    meta_schedule('internal')
    job = db['Job'].query.one()
    print(job.state)
    set_job_state(job.id, 'Resuming')
    job = db['Job'].query.one()
    print(job.state)
    meta_schedule('internal')
    assert(job.state == 'Resuming')
    # assert(True)


@pytest.mark.usefixtures('config_suspend_resume')
def test_suspend_resume_2(monkeypatch):
    config['JUST_BEFORE_RESUME_EXEC_FILE'] = 'sleep 2'
    # now = get_date()
    insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    meta_schedule('internal')
    job = db['Job'].query.one()
    print(job.state)
    set_job_state(job.id, 'Resuming')
    job = db['Job'].query.one()
    print(job.state)
    meta_schedule('internal')
    assert(job.state == 'Resuming')
