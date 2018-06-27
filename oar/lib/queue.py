# coding: utf-8
""" Functions to handle queues"""
# QUEUES MANAGEMENT 
# TODO delete_a_queue($$);
# TODO is_waiting_job_specific_queue_present($$);
# TODO create_a_queue($$$$);
# TODO change_a_queue($$$$);

from sqlalchemy import (distinct, text)
from oar.lib import (db, Queue, get_logger)

logger = get_logger('oar.lib.queue')

def get_all_queue_by_priority():
    return db.query(Queue).order_by(text('priority DESC')).all()

def stop_a_queue(queue_name):
    """ Stop a queue"""
    db.query(Queue).filter(Queue.name == queue_name).update({Queue.state: 'notActive'},
    synchronize_session=False)
    db.commit()

def start_a_queue(queue_name):
    """ Start a queue"""
    db.query(Queue).filter(Queue.name == queue_name).update({Queue.state: 'Active'},
                                                            synchronize_session=False)
    db.commit()

def stop_all_queues():
    """ Stop all queues"""
    db.query(Queue).update({Queue.state: 'notActive'},
    synchronize_session=False)
    db.commit()

def start_all_queues():
    """ Start all queues"""
    db.query(Queue).update({Queue.state: 'Active'},synchronize_session=False)
    db.commit()
