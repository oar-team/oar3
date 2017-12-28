# coding: utf-8
import pytest

from oar.modules.bipbip import BipBip

from oar.lib import (db, config, Challenge)
from oar.lib.job_handling import insert_job

def test_bipbip_void():
    bipbip = BipBip(None)
    bipbip.run()
    print(bipbip.exit_code)
    assert bipbip.exit_code == 1

def _test_bipbip_simple():
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='')
    Challenge.create(job_id=job_id, challenge='foo1', ssh_private_key='foo2', ssh_public_key='foo2')
    
    # Bipbip needs a job id
    bipbip = BipBip([job_id])
    bipbip.run()
    print(bipbip.exit_code)
    assert bipbip.exit_code == 1
