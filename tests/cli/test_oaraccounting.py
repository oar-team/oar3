# coding: utf-8
import pytest

from ..helpers import insert_terminated_jobs

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

@pytest.mark.skipif("os.environ.get('DB_TYPE', '') != 'postgresql'",
                    reason="need postgresql database")
def test_oaraccounting():
    insert_terminated_jobs()
    
    accounting = db.query(Accounting).all()
    for a in accounting:
        print(a.user, a.project, a.consumption_type, a.queue_name,
              a.window_start, a.window_stop, a.consumption)

    assert accounting[7].consumption == 864000
