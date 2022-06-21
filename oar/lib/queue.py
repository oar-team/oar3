# coding: utf-8
""" Functions to handle queues"""
# QUEUES MANAGEMENT
# TODO is_waiting_job_specific_queue_present($$);
from itertools import groupby

from oar.lib import Queue, db, get_logger

logger = get_logger("oar.lib.queue")


def get_all_queue_by_priority():
    return db.query(Queue).order_by(Queue.priority.desc()).all()


def get_queues_groupby_priority():
    """Return queues grouped by priority, groups are sorted by priority (higher value in first)"""
    queues_ordered = db.query(Queue).order_by(Queue.priority.asc()).all()
    res = []

    for key, queues_group in groupby(queues_ordered, lambda q: q.priority):
        res_grp = []
        for queue in queues_group:
            res_grp.append(queue)
        res.append(res_grp)
    return res


def stop_queue(name):
    """Stop a queue"""
    db.query(Queue).filter(Queue.name == name).update(
        {Queue.state: "notActive"}, synchronize_session=False
    )
    db.commit()


def start_queue(name):
    """Start a queue"""
    db.query(Queue).filter(Queue.name == name).update(
        {Queue.state: "Active"}, synchronize_session=False
    )
    db.commit()


def stop_all_queues():
    """Stop all queues"""
    db.query(Queue).update({Queue.state: "notActive"}, synchronize_session=False)
    db.commit()


def start_all_queues():
    """Start all queues"""
    db.query(Queue).update({Queue.state: "Active"}, synchronize_session=False)
    db.commit()


def create_queue(name, priority, policy):
    Queue.create(name=name, priority=priority, scheduler_policy=policy, state="Active")
    db.commit()


def change_queue(name, priority, policy):
    db.query(Queue).filter(Queue.name == name).update(
        {Queue.priority: priority, Queue.scheduler_policy: policy},
        synchronize_session=False,
    )
    db.commit()


def remove_queue(name):
    db.query(Queue).filter(Queue.name == name).delete(synchronize_session=False)
    db.commit()
