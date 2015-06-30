# coding: utf-8
import pytest
from oar.lib import (db, Resource, GanttJobsPrediction)
from oar.kao.job import insert_job
from oar.kao.platform import Platform
from oar.kao.kamelot_fifo  import schedule_fifo_cycle


def db_flush():
    db.session.flush()
    db.session.expunge_all()
    db.session.commit()


@pytest.fixture(scope='function', autouse=True)
def flush_db(request):
    def teardown():
        db.delete_all()

    request.addfinalizer(teardown)

def test_db_kamelot_fifo_no_hierarchy():
    # add some resources
    for i in range(5):
        db.add(Resource(network_address="localhost"))

    for i in range(5):
        insert_job(res=[(60, [('resource_id=2', "")])],
                   properties="")
    db_flush()

    plt = Platform()

    schedule_fifo_cycle(plt, "default", False)

    req = db.query(GanttJobsPrediction).all()

    #for i, r in enumerate(req):
    #    print "req:", r.moldable_id, r.start_time

    assert len(req) == 2


def test_db_kamelot_fifo_w_hierarchy():
    # add some resources
    for i in range(5):
        db.add(Resource(network_address="localhost"+str(int(i/2))))

    print

    for res in db.query(Resource).all():
        print res.id, res.network_address

    for i in range(5):
        insert_job(res=[(60, [('network_address=1', "")])],
                   properties="")
    db_flush()

    plt = Platform()

    schedule_fifo_cycle(plt, "default", True)

    req = db.query(GanttJobsPrediction).all()

    #for i, r in enumerate(req):
    #    print "req:", r.moldable_id, r.start_time

    assert len(req) == 3
