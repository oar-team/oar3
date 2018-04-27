# coding: utf-8
#
#  OAR's Live Testing
#  Requirement: a live configured OAR installation (oardocker is ok)
#  Usage: OAR_LIVE_TEST=1 pytest
#
import pytest
import re
from . import live_testing, pytestmark

@live_testing
def test_oarsub_date(script_runner):
    ret = script_runner.run('oarsub', 'date')
    print(ret.stdout)
    assert re.match(r'^OAR_JOB_ID= \d+$', ret.stdout)
    assert ret.success

