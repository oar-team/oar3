# coding: utf-8
import pytest


from oar.lib import (db, config, Job, Accounting, Resource, AssignedResource)
from oar.lib.job_handling import (insert_job)
from oar.lib.accounting import(check_accounting_update)

from click.testing import CliRunner

from oar.cli.oaraccounting import cli

import oar.lib.tools  # for monkeypatching
from oar.lib.tools import get_date

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(10):
            db['Resource'].create(network_address="localhost")

        db['Queue'].create(name='default')
        yield
        
def insert_terminated_jobs():
    
    window_size = 86400
    j_duration = window_size * 10
    j_walltime = j_duration + 2 * window_size

    user = 'zozo'
    project = 'yopa'
    resources = db.query(Resource).all()
    for i in range(5):
        start_time = 30000 + (window_size / 4) * i
        stop_time = start_time + j_duration
        job_id = insert_job(res=[(j_walltime, [('resource_id=2', '')])],
                            properties='', command='yop',
                            user = user, project = project,
                            start_time = start_time,
                            stop_time = stop_time,
                            state='Terminated')
        db.query(Job).update({Job.assigned_moldable_job: job_id}, synchronize_session=False)

        for r in resources[i:i+2]:
            AssignedResource.create(moldable_id=job_id, resource_id=r.id)
            print(r.id, r.network_address)
        db.commit()


    check_accounting_update(window_size)


@pytest.mark.skipif("os.environ.get('DB_TYPE', '') != 'postgresql'",
                    reason="need postgresql database")
def test_oaraccounting():
    insert_terminated_jobs()
    
    accounting = db.query(Accounting).all()
    for a in accounting:
        print(a.user, a.project, a.consumption_type, a.queue_name,
              a.window_start, a.window_stop, a.consumption)

    assert accounting[7].consumption == 864000
