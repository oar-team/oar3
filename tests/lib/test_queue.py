# coding: utf-8
import pytest

from oar.lib import Queue, db

from oar.lib.queue import (
    stop_queue,
    start_queue,
    stop_all_queues,
    start_all_queues,
    create_queue,
    change_queue,
    remove_queue,
)


@pytest.yield_fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        Queue.create(name="default", state="unkown")
        yield


def test_get_all_queue_by_priority():
    queue = db.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "unkown"


def test_stop_queue():
    stop_queue("default")
    queue = db.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "notActive"


def test_start_queue():
    start_queue("default")
    queue = db.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "Active"


def test_stop_all_queues():
    stop_all_queues()
    queue = db.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "notActive"


def test_start_all_queues():
    start_all_queues()
    queue = db.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "Active"


def test_create_queue():
    create_queue("admin", 10, "kamelot")
    queue = db.query(Queue).filter(Queue.scheduler_policy == "kamelot").one()
    assert queue.name == "admin"


def test_change_queue():
    change_queue("default", 42, "fast")
    queue = db.query(Queue).filter(Queue.name == "default").one()
    assert queue.priority == 42
    assert queue.scheduler_policy == "fast"


def test_remove_queue():
    create_queue("admin", 10, "kamelot")
    assert len(db.query(Queue).all()) == 2
    remove_queue("admin")
    assert len(db.query(Queue).all()) == 1
