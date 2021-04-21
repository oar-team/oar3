# coding: utf-8
import pytest

from oar.kao.kamelot import schedule_cycle
from oar.kao.platform import Platform
from oar.lib import config, db
from oar.lib.job_handling import insert_job


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        yield


@pytest.fixture(scope="module", autouse=True)
def oar_conf(request):
    config["JOB_PRIORITY"] = "CUSTOM"

    @request.addfinalizer
    def remove_job_sorting():
        config["JOB_PRIORITY"] = "FIFO"
        config["CUSTOM_JOB_SORTING"] = ""
        config["CUSTOM_JOB_SORTING_CONFIG"] = ""


def test_db_job_sorting_simple_priority_no_waiting_time():

    config["CUSTOM_JOB_SORTING"] = "simple_priority"

    plt = Platform()
    now = plt.get_time()

    # add some resources
    for i in range(4):
        db["Resource"].create(network_address="localhost")

    # add some job with priority
    for i in range(10):
        priority = str(float(i) / 10.0)
        insert_job(
            res=[(60, [("resource_id=4", "")])],
            submission_time=now,
            types=["priority=" + priority],
        )

    schedule_cycle(plt, plt.get_time())

    req = (
        db["GanttJobsPrediction"]
        .query.order_by(db["GanttJobsPrediction"].start_time)
        .all()
    )
    flag = True

    print(req)
    for r in req:
        print(r.moldable_id, r.start_time)

    prev_id = req[0].moldable_id
    for i, r in enumerate(req):
        if i != 0:
            print(r.moldable_id, prev_id)
            if r.moldable_id > prev_id:
                flag = False
                break
        prev_id = r.moldable_id

    assert flag
