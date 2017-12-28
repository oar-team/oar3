# coding: utf-8
import pytest

from oar.modules.bipbip import BipBip

from oar.lib import (db, config, Challenge)
from oar.lib.job_handling import insert_job


@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost")

        db['Queue'].create(name='default')
        yield

def test_bipbip_void():
    bipbip = BipBip(None)
    bipbip.run()
    print(bipbip.exit_code)
    assert bipbip.exit_code == 1

def test_bipbip_simple():
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='')
    Challenge.create(job_id=job_id, challenge='foo1', ssh_private_key='foo2', ssh_public_key='foo2')
    
    # Bipbip needs a job id
    bipbip = BipBip([job_id])
    bipbip.run()
    print(bipbip.exit_code)
    assert bipbip.exit_code == 1
