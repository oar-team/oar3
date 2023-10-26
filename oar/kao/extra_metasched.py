# coding: utf-8
"""Extra metascheduling functions which can be called between each queue handling
"""
from oar.lib import Job, MoldableJobDescription, Resource, db, get_logger
from oar.lib.job_handling import (
    get_job_types,
    set_job_start_time_assigned_moldable_id,
    set_job_state,
)

logger = get_logger("oar.extra_metasched")


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


def extra_metasched_evolving(
    prev_queue,
    plt,
    scheduled_jobs,
    all_slot_sets,
    job_security_time,
    queues,
    initial_time_sec,
    extra_metasched_config,
):

    logger.info("Extra metasched for queue(s): {}".format(queues))
    # set first resource deployable
    waiting_jobs, waiting_jids, nb_waiting_jobs = plt.get_waiting_jobs(
        [q.name for q in queues]
    )
    for jid in waiting_jids:
        job = waiting_jobs[jid]
        job_types = get_job_types(jid)

        logger.debug(
            "waiting_jid: {} {} type: {}".format(str(jid), job.__dict__, job_types)
        )
        if "envelop" in job_types:
            logger.debug("Spotted envelop type job, what are we going do with you ?")
            from oar.kao.meta_sched import notify_to_run_job

            # Setting the state to prevent job for being scheduled
            set_job_state(job.id, "toLaunch")

            # Trick to assign the job a moldable Job description
            result = (
                db.query(MoldableJobDescription)
                .filter(MoldableJobDescription.job_id == job.id)
                .one()
            )
            db.query(Job).filter(Job.id == job.id).update(
                {Job.assigned_moldable_job: result.id}
            )
            set_job_start_time_assigned_moldable_id(job.id, initial_time_sec, result.id)

            notify_to_run_job(job.id)
            pass
