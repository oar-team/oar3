# coding: utf-8
import sys
import time

import pytest

from oar.kao.kamelot import main
from oar.lib.job_handling import insert_job


@pytest.fixture(scope="function", autouse=False)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        for i in range(5):
            db["Resource"].create(network_address="localhost")

        for i in range(5):
            insert_job(res=[(60, [("resource_id=2", "")])], properties="")
        yield


def test_db_kamelot_1(minimal_db_initialization):
    old_sys_argv = sys.argv
    sys.argv = ["test_kamelot", "default", time.time()]
    main()
    sys.argv = old_sys_argv
    req = db["GanttJobsPrediction"].query.all()
    assert len(req) == 5


def test_db_kamelot_2(minimal_db_initialization):
    old_sys_argv = sys.argv
    sys.argv = ["test_kamelot", "default"]
    main()
    sys.argv = old_sys_argv
    req = db["GanttJobsPrediction"].query.all()
    assert len(req) == 5


def test_db_kamelot_3(minimal_db_initialization):
    old_sys_argv = sys.argv
    sys.argv = ["test_kamelot"]
    main()
    sys.argv = old_sys_argv
    req = db["GanttJobsPrediction"].query.all()
    assert len(req) == 5


@pytest.fixture(scope="function", autouse=False)
def properties_init(request):
    with db.session(ephemeral=True):
        for i in range(4):
            db["Resource"].create(network_address="localhost")

        for i in range(3):
            insert_job(res=[(60, [("resource_id=2", "")])], properties="")

        tokens = [
            db["Resource"].create(type="token").id,
            db["Resource"].create(type="token").id,
        ]

        yield tokens


def test_db_kamelot_4(properties_init):
    old_sys_argv = sys.argv
    sys.argv = ["test_kamelot", "default", time.time()]
    main()
    sys.argv = old_sys_argv
    req = db["GanttJobsResource"].query.all()

    for alloc in req:
        assert alloc.resource_id not in properties_init
