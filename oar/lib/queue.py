# coding: utf-8
""" Functions to handle queues"""
# QUEUES MANAGEMENT
# TODO is_waiting_job_specific_queue_present($$);
from itertools import groupby
from typing import List

from sqlalchemy.orm import Session

from oar.lib.models import Queue


def get_all_queue_by_priority(session: Session) -> List[Queue]:
    return session.query(Queue).order_by(Queue.priority.desc()).all()


def get_queues_groupby_priority(session: Session) -> Queue:
    """Return queues grouped by priority, groups are sorted by priority (higher value in first)"""
    queues_ordered = session.query(Queue).order_by(Queue.priority.desc()).all()
    res = []

    for key, queues_group in groupby(queues_ordered, lambda q: q.priority):
        res_grp = []
        for queue in queues_group:
            res_grp.append(queue)
        res.append(res_grp)
    return res


def stop_queue(session: Session, name: str):
    """Stop a queue"""
    session.query(Queue).filter(Queue.name == name).update(
        {Queue.state: "notActive"}, synchronize_session=False
    )
    session.commit()


def start_queue(session: Session, name: str):
    """Start a queue"""
    session.query(Queue).filter(Queue.name == name).update(
        {Queue.state: "Active"}, synchronize_session=False
    )
    session.commit()


def stop_all_queues(
    session: Session,
):
    """Stop all queues"""
    session.query(Queue).update({Queue.state: "notActive"}, synchronize_session=False)
    session.commit()


def start_all_queues(
    session: Session,
):
    """Start all queues"""
    session.query(Queue).update({Queue.state: "Active"}, synchronize_session=False)
    session.commit()


def create_queue(session: Session, name: str, priority: int, policy: str):
    Queue.create(
        session, name=name, priority=priority, scheduler_policy=policy, state="Active"
    )
    session.commit()


def change_queue(session: Session, name: str, priority: int, policy: str):
    session.query(Queue).filter(Queue.name == name).update(
        {Queue.priority: priority, Queue.scheduler_policy: policy},
        synchronize_session=False,
    )
    session.commit()


def remove_queue(session: Session, name: str):
    session.query(Queue).filter(Queue.name == name).delete(synchronize_session=False)
    session.commit()
