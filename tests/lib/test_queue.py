# coding: utf-8
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.lib.database import ephemeral_session
from oar.lib.models import Queue
from oar.lib.queue import (
    change_queue,
    create_queue,
    remove_queue,
    start_all_queues,
    start_queue,
    stop_all_queues,
    stop_queue,
)


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        Queue.create(session, name="default", state="unkown")
        yield session


def test_get_all_queue_by_priority(minimal_db_initialization):
    queue = minimal_db_initialization.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "unkown"


def test_stop_queue(minimal_db_initialization):
    stop_queue(minimal_db_initialization, "default")
    queue = minimal_db_initialization.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "notActive"


def test_start_queue(minimal_db_initialization):
    start_queue(minimal_db_initialization, "default")
    queue = minimal_db_initialization.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "Active"


def test_stop_all_queues(minimal_db_initialization):
    stop_all_queues(
        minimal_db_initialization,
    )
    queue = minimal_db_initialization.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "notActive"


def test_start_all_queues(minimal_db_initialization):
    start_all_queues(minimal_db_initialization)
    queue = minimal_db_initialization.query(Queue).filter(Queue.name == "default").one()
    assert queue.state == "Active"


def test_create_queue(minimal_db_initialization):
    create_queue(minimal_db_initialization, "admin", 10, "kamelot")
    queue = (
        minimal_db_initialization.query(Queue)
        .filter(Queue.scheduler_policy == "kamelot")
        .one()
    )
    assert queue.name == "admin"


def test_change_queue(minimal_db_initialization):
    change_queue(minimal_db_initialization, "default", 42, "fast")
    queue = minimal_db_initialization.query(Queue).filter(Queue.name == "default").one()
    assert queue.priority == 42
    assert queue.scheduler_policy == "fast"


def test_remove_queue(minimal_db_initialization):
    create_queue(minimal_db_initialization, "admin", 10, "kamelot")
    assert len(minimal_db_initialization.query(Queue).all()) == 2
    remove_queue(minimal_db_initialization, "admin")
    assert len(minimal_db_initialization.query(Queue).all()) == 1
