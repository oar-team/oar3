# -*- coding: utf-8 -*-
import os
import pytest

from oar.lib.fixture import load_fixtures

REFTIME = 1437050120


@pytest.yield_fixture
def db(request):
    """Creates a new database session for a test."""
    from oar.lib import db

    connection = db.engine.connect()
    transaction = connection.begin()

    db.session.remove()
    db.session(bind=connection)

    try:
        here = os.path.abspath(os.path.dirname(__file__))
        filename = os.path.join(here, "data", "dataset_1.json")
        load_fixtures(db, filename, ref_time=REFTIME, clear=False)
        yield db
    finally:
        transaction.rollback()
        connection.close()
        db.session.remove()


@pytest.mark.skipif("os.environ.get('DB_TYPE', '') != 'postgresql'",
                    reason="need postgresql database")
def test_pg_bulk_insert_binary(db):
    from oar.lib.psycopg2 import pg_bulk_insert
    cursor = db.session.bind.connection.cursor()
    columns = ("queue_name", "priority", "scheduler_policy", "state")
    rows = [
        ("old_test", -1, "LIFO", "Inactive"),
        ("default_test", 1, "FIFO", "Active"),
        ("vip_test", 10, "FIFO", "Active"),
    ]
    pg_bulk_insert(cursor, db['queues'], rows, columns,
                   binary=True)
    names = [row[0] for row in rows]
    queues = db['Queue'].query\
                        .filter(db['Queue'].name.in_(names))\
                        .order_by(db['Queue'].priority)
    for queue, row in zip(queues, rows):
        assert queue.name == row[0]
        assert queue.priority == row[1]
        assert queue.scheduler_policy == row[2]
        assert queue.state == row[3]


@pytest.mark.skipif("os.environ.get('DB_TYPE', '') != 'postgresql'",
                    reason="need postgresql database")
def test_pg_bulk_insert_csv(db):
    from oar.lib.psycopg2 import pg_bulk_insert
    cursor = db.session.bind.connection.cursor()
    columns = ("queue_name", "priority", "scheduler_policy", "state")
    rows = [
        ("old_test", -1, "LIFO", "Inactive"),
        ("default_test", 1, "FIFO", "Active"),
        ("vip_test", 10, "FIFO", "Active"),
    ]
    pg_bulk_insert(cursor, db['queues'], rows, columns,
                   binary=False)
    names = [row[0] for row in rows]
    queues = db['Queue'].query\
                        .filter(db['Queue'].name.in_(names))\
                        .order_by(db['Queue'].priority)
    for queue, row in zip(queues, rows):
        assert queue.name == row[0]
        assert queue.priority == row[1]
        assert queue.scheduler_policy == row[2]
        assert queue.state == row[3]
