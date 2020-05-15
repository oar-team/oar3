# coding: utf-8
import pytest

from oar.lib import config, db, Job
from oar.lib.job_handling import insert_job
from oar.kao.meta_sched import meta_schedule

import oar.lib.tools  # for monkeypatching
from oar.lib.tools import get_date


@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        db['Queue'].create(name='default', priority=3, scheduler_policy='kamelot', state='Active')

        # add some resources
        for _ in range(5):
            db['Resource'].create(network_address='localhost')
        yield


@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'create_almighty_socket', lambda: None)
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: True)
    monkeypatch.setattr(oar.lib.tools, 'notify_bipbip_commander', lambda x: True)
    monkeypatch.setattr(oar.lib.tools, 'notify_tcp_socket', lambda addr, port, msg: len(msg))
    monkeypatch.setattr(oar.lib.tools, 'notify_user', lambda job, state, msg: len(state + msg))


@pytest.fixture(scope='module', autouse=True)
def oar_conf(request):

    @request.addfinalizer
    def remove_job_sorting():
        config['EXTRA_METASCHED'] = 'default'


def test_db_extra_metasched_1():
    config['EXTRA_METASCHED'] = 'foo'

    insert_job(res=[(60, [('resource_id=1', "")])], properties="deploy='YES'")
    insert_job(res=[(60, [('resource_id=1', "")])], properties="deploy='FOO'")
    insert_job(res=[(60, [('resource_id=1', "")])], properties="")

    for job in  db['Job'].query.all():
        print('job state:', job.state, job.id)

    meta_schedule()

    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time)

    states = [job.state for job in db['Job'].query.order_by(Job.id).all()]
    print(states)
    assert states == ['toLaunch', 'Waiting', 'toLaunch']
