# coding: utf-8
import pytest

from oar.kao.kamelot_fifo import main, schedule_fifo_cycle
from oar.kao.platform import Platform
from oar.lib.job_handling import insert_job


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        yield


def test_db_kamelot_fifo_no_hierarchy():
    # add some resources
    for i in range(5):
        db["Resource"].create(network_address="localhost")

    for i in range(5):
        insert_job(res=[(60, [("resource_id=2", "")])], properties="")

    main()

    req = db["GanttJobsPrediction"].query.all()

    # for i, r in enumerate(req):
    #    print "req:", r.moldable_id, r.start_time

    assert len(req) == 2


def test_db_kamelot_fifo_w_hierarchy():
    # add some resources
    for i in range(5):
        db["Resource"].create(network_address="localhost" + str(int(i / 2)))

    for res in db["Resource"].query.all():
        print(res.id, res.network_address)

    for i in range(5):
        insert_job(res=[(60, [("network_address=1", "")])], properties="")

    plt = Platform()

    schedule_fifo_cycle(plt, "default", True)

    req = db["GanttJobsPrediction"].query.all()

    # for i, r in enumerate(req):
    #    print("req:", r.moldable_id, r.start_time)

    assert len(req) == 3
