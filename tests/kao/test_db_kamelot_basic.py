# coding: utf-8
import pytest

from oar.kao.kamelot_basic import main
from oar.lib.job_handling import insert_job


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        yield


def test_db_kamelot_basic_1():
    # add some resources
    for i in range(5):
        db["Resource"].create(network_address="localhost")

    for i in range(5):
        insert_job(res=[(60, [("resource_id=2", "")])], properties="")

    main()

    req = db["GanttJobsPrediction"].query.all()

    for i, r in enumerate(req):
        print("req:", r.moldable_id, r.start_time)

    assert len(req) == 5
