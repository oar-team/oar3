# coding: utf-8
"""Extra metascheduling functions which can be called between each queue handling
"""
from oar.lib import Resource, db, get_logger

logger = get_logger("oar.extra_metasched")


def extra_metasched_default(
    prev_queue,
    plt,
    scheduled_jobs,
    all_slot_sets,
    job_security_time,
    queue,
    initial_time_sec,
    extra_metasched_config,
):
    pass


def extra_metasched_foo(
    prev_queue,
    plt,
    scheduled_jobs,
    all_slot_sets,
    job_security_time,
    queue,
    initial_time_sec,
    extra_metasched_config,
):

    if prev_queue is None:
        # set first resource deployable
        first_id = db.query(Resource).first().id
        db.query(Resource).filter(Resource.id == first_id).update(
            {Resource.deploy: "YES"}, synchronize_session=False
        )
        db.commit()
