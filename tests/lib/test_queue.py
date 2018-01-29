# coding: utf-8
import pytest

from oar.lib import (Queue, db)

from oar.lib.queue import (stop_a_queue, start_a_queue, stop_all_queues,
                           start_all_queues)



@pytest.yield_fixture(scope='module', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        Queue.create(name='default', state='unkown')
        yield

def test_stop_a_queue():
    stop_a_queue('default')
    queue = db.query(Queue).filter(Queue.name=='default').one()
    assert queue.state == 'notActive'
    
def test_start_a_queue():
    start_a_queue('default')
    queue = db.query(Queue).filter(Queue.name=='default').one()
    assert queue.state == 'Active'

def test_stop_all_queues():
    stop_all_queues()
    queue = db.query(Queue).filter(Queue.name=='default').one()
    assert queue.state == 'notActive'

def test_start_all_queues():
    start_all_queues()
    queue = db.query(Queue).filter(Queue.name=='default').one()
    assert queue.state == 'Active'
    
