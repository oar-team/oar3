# coding: utf-8
#
#  OAR's Live Testing
#  Requirement: a live configured OAR installation (oardocker is ok)
#  Usage: OAR_LIVE_TEST=1 pytest
#
import pytest
import re
import pexpect
from subprocess import call
from . import live_testing, pytestmark


@live_testing
def test_oarsub_date(script_runner):
    ret = script_runner.run("oarsub", "date")
    print(ret.stdout)
    assert re.match(r"^OAR_JOB_ID= \d+$", ret.stdout)
    assert ret.success


@live_testing
def test_oarsub_I():
    child = pexpect.spawn("oarsub -I")
    try:
        child.expect("OAR_JOB_ID= (\d+)\r\n", timeout=30)
        job_id = child.match.group(1).decode()
        call("oardel " + job_id, shell=True)
        child.expect(pexpect.EOF, timeout=30)
    except pexpect.TIMEOUT:
        assert False
    assert not child.isalive()
    assert child.exitstatus == 0
