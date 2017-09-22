# coding: utf-8
import pytest
import sys
import time

from oar.lib import db

from oar.lib.job_handling import insert_job
from oar.kao.kamelot import main


@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        for i in range(5):
            db['Resource'].create(network_address="localhost")

        for i in range(5):
            insert_job(res=[(60, [('resource_id=2', "")])], properties="")
        yield


def test_db_kamelot_1():

    old_sys_argv = sys.argv
    sys.argv = ['test_kamelot', 'default', time.time()]
    main()
    sys.argv = old_sys_argv
    req = db['GanttJobsPrediction'].query.all()
    assert len(req) == 5


def test_db_kamelot_2():

    old_sys_argv = sys.argv
    sys.argv = ['test_kamelot', 'default']
    main()
    sys.argv = old_sys_argv
    req = db['GanttJobsPrediction'].query.all()
    assert len(req) == 5


def test_db_kamelot_3():

    old_sys_argv = sys.argv
    sys.argv = ['test_kamelot']
    main()
    sys.argv = old_sys_argv
    req = db['GanttJobsPrediction'].query.all()
    assert len(req) == 5
