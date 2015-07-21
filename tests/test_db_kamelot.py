# coding: utf-8
from __future__ import unicode_literals, print_function

import pytest
import sys
import time

from oar.lib import db

from oar.kao.job import insert_job
from oar.kao.kamelot import main


@pytest.fixture(scope="module", autouse=True)
def create_db(request):
    db.create_all()
    db.reflect()
    db.delete_all()

    @request.addfinalizer
    def teardown():
        db.delete_all()
        db.session.close()


@pytest.fixture(scope='function', autouse=True)
def minimal_db_initialization(request):

    for i in range(5):
        db['Resource'].create(network_address="localhost")

    for i in range(5):
        insert_job(res=[(60, [('resource_id=2', "")])], properties="")

    @request.addfinalizer
    def teardown():
        db.delete_all()
        db.session.close()


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
