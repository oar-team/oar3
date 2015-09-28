# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest

from oar.lib import config, db
from oar.kao.job import insert_job
from oar.kao.meta_sched import meta_schedule

import oar.lib.tools  # for monkeypatching
from oar.lib.tools import get_date


@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        db['Queue'].create(name='default', priority=3, scheduler_policy='kamelot', state='Active')

        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost")
        yield


@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'init_judas_notify_user', lambda: None)
    monkeypatch.setattr(oar.lib.tools, 'create_almighty_socket', lambda: None)
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: len(x))
    monkeypatch.setattr(oar.lib.tools, 'notify_tcp_socket', lambda addr, port, msg: len(msg))
    monkeypatch.setattr(oar.lib.tools, 'notify_user', lambda job, state, msg: len(state + msg))


def test_db_metasched_simple_1(monkeypatch):

    print("DB_BASE_FILE: ", config["DB_BASE_FILE"])
    insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    job = db['Job'].query.one()
    print('job state:', job.state)

    meta_schedule()

    for i in db['GanttJobsPrediction'].query.all():
        print("moldable_id: ", i.moldable_id, ' start_time: ', i.start_time)

    job = db['Job'].query.one()
    print(job.state)
    assert (job.state == 'toLaunch')


def test_db_metasched_ar_1(monkeypatch):

    # add one job
    now = get_date()
    # sql_now = local_to_sql(now)

    insert_job(res=[(60, [('resource_id=4', "")])], properties="",
               reservation='toSchedule', start_time=(now + 10),
               info_type='localhost:4242')

    meta_schedule()

    job = db['Job'].query.one()
    print(job.state, ' ', job.reservation)

    assert ((job.state == 'Waiting') and (job.reservation == 'Scheduled'))
