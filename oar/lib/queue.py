# coding: utf-8
""" Functions to handle queues"""
# QUEUES MANAGEMENT
# TODO is_waiting_job_specific_queue_present($$);
from itertools import groupby

from oar.lib.globals import init_oar
from oar.lib.logging import get_logger
from oar.lib.models import Queue

_, _, logger = init_oar()

logger = get_logger(logger, "oar.lib.queue")


def get_all_queue_by_priority():
    return session.query(Queue).order_by(Queue.priority.desc()).all()


def get_queues_groupby_priority():
    """Return queues grouped by priority, groups are sorted by priority (higher value in first)"""
    queues_ordered = session.query(Queue).order_by(Queue.priority.desc()).all()
    res = []

    for key, queues_group in groupby(queues_ordered, lambda q: q.priority):
        res_grp = []
        for queue in queues_group:
            res_grp.append(queue)
        res.append(res_grp)
    return res


def stop_queue(session, name):
    """Stop a queue"""
    session.query(Queue).filter(Queue.name == name).update(
        {Queue.state: "notActive"}, synchronize_session=False
    )
    session.commit()


def start_queue(session, name):
    """Start a queue"""
    session.query(Queue).filter(Queue.name == name).update(
        {Queue.state: "Active"}, synchronize_session=False
    )
    session.commit()


def stop_all_queues(
    session,
):
    """Stop all queues"""
    session.query(Queue).update({Queue.state: "notActive"}, synchronize_session=False)
    session.commit()


def start_all_queues(
    session,
):
    """Start all queues"""
    session.query(Queue).update({Queue.state: "Active"}, synchronize_session=False)
    session.commit()


def create_queue(session, name, priority, policy):
    Queue.create(
        session, name=name, priority=priority, scheduler_policy=policy, state="Active"
    )
    session.commit()


def change_queue(session, name, priority, policy):
    session.query(Queue).filter(Queue.name == name).update(
        {Queue.priority: priority, Queue.scheduler_policy: policy},
        synchronize_session=False,
    )
    session.commit()


def remove_queue(session, name):
    session.query(Queue).filter(Queue.name == name).delete(synchronize_session=False)
    session.commit()
