# coding: utf-8
import pytest

from ..helpers import insert_terminated_jobs

from oar.lib import (db, config, Job, Accounting, Resource, AssignedResource)
from oar.lib.job_handling import (insert_job)
from oar.lib.accounting import(check_accounting_update, delete_all_from_accounting,
                               delete_accounting_windows_before)

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(10):
            db['Resource'].create(network_address="localhost")

        db['Queue'].create(name='default')
        yield

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') != 'postgresql'",
                    reason="need postgresql database")
def test_check_accounting_update():
    insert_terminated_jobs()
    accounting = db.query(Accounting).all()
    for a in accounting:
        print(a.user, a.project, a.consumption_type, a.queue_name,
              a.window_start, a.window_stop, a.consumption)
        
    assert accounting[7].consumption == 864000

    
@pytest.mark.skipif("os.environ.get('DB_TYPE', '') != 'postgresql'",
                    reason="need postgresql database")
def test_delete_all_from_accounting():
    insert_terminated_jobs()
    delete_all_from_accounting()
    accounting = db.query(Accounting).all()
    assert accounting == []

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') != 'postgresql'",
                    reason="need postgresql database")
def test_delete_accounting_windows_before():
    insert_terminated_jobs()
    accounting1 = db.query(Accounting).all()
    delete_accounting_windows_before(5*86400)
    accounting2 = db.query(Accounting).all()
    assert len(accounting1) > len(accounting2)
