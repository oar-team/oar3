# -*- coding: utf-8 -*-
import os

import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.lib.database import ephemeral_session

# from oar.lib import db
from oar.lib.fixture import load_fixtures
from oar.lib.models import Queue

REFTIME = 1437050120


@pytest.fixture(scope="module", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        here = os.path.abspath(os.path.dirname(__file__))
        filename = os.path.join(here, "data", "dataset_1.json")
        load_fixtures(session, filename, ref_time=REFTIME, clear=True)
        yield session


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_pg_bulk_insert_binary(minimal_db_initialization):
    from oar.lib.psycopg2 import pg_bulk_insert

    cursor = session.session.bind.connection.cursor()
    columns = ("queue_name", "priority", "scheduler_policy", "state")
    rows = [
        ("old_test", -1, "LIFO", "Inactive"),
        ("default_test", 1, "FIFO", "Active"),
        ("vip_test", 10, "FIFO", "Active"),
    ]
    pg_bulk_insert(cursor, db["queues"], rows, columns, binary=True)
    names = [row[0] for row in rows]
    queues = Queue.query.filter(Queue.name.in_(names)).order_by(Queue.priority)
    for queue, row in zip(queues, rows):
        assert queue.name == row[0]
        assert queue.priority == row[1]
        assert queue.scheduler_policy == row[2]
        assert queue.state == row[3]


@pytest.mark.skipif(
    "os.environ.get('DB_TYPE', '') != 'postgresql'", reason="need postgresql database"
)
def test_pg_bulk_insert_csv(minimal_db_initialization):
    session = minimal_db_initialization
    from oar.lib.psycopg2 import pg_bulk_insert

    cursor = session.session.bind.connection.cursor()
    columns = ("queue_name", "priority", "scheduler_policy", "state")
    rows = [
        ("old_test", -1, "LIFO", "Inactive"),
        ("default_test", 1, "FIFO", "Active"),
        ("vip_test", 10, "FIFO", "Active"),
    ]
    pg_bulk_insert(cursor, db["queues"], rows, columns, binary=False)
    names = [row[0] for row in rows]
    queues = Queue.query.filter(Queue.name.in_(names)).order_by(Queue.priority)
    for queue, row in zip(queues, rows):
        assert queue.name == row[0]
        assert queue.priority == row[1]
        assert queue.scheduler_policy == row[2]
        assert queue.state == row[3]
