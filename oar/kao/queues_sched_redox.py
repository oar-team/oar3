import os

from oar.kao.queues_sched import handle_waiting_reservation_jobs
from oar.kao.slot import SlotSet
from oar.lib.globals import get_logger
from oar.lib.job_handling import (
    get_waiting_moldable_of_reservations_already_scheduled,
    gantt_flush_tables,
    get_jobs_in_multiple_states,
)
from oar.lib.plugins import find_plugin_function
from oar.lib.queue import get_queues_groupby_priority


logger = get_logger("oar.kao.queues_sched_redox")


def queues_schedule_redox(
    session,
    config,
    _mode,
    plt,
    resource_set,
    initial_time_sec,
    _initial_time_sql,
    current_time_sec,
    job_security_time,
):
    import oar_scheduler_redox

    scheduled_jobs, besteffort_rid2job = gantt_init_with_running_jobs(
        session, config, plt, initial_time_sec, job_security_time
    )

    # Init redox with session, config, platform
    redox_platform = oar_scheduler_redox.build_redox_platform(
        session, config, plt, initial_time_sec, scheduled_jobs
    )
    redox_slot_sets = oar_scheduler_redox.build_redox_slot_sets(redox_platform)

    # Iterate over grouped queues and queues
    prev_queues = None
    for queues in get_queues_groupby_priority(session):
        # Plugin hook: extra_metasched_func
        prev_queues = queues
        active_queues = [q.name for q in queues if q.state == "Active"]

        oar_scheduler_redox.schedule_cycle_internal(
            redox_platform, redox_slot_sets, active_queues
        )

        for queue in active_queues:
            handle_waiting_reservation_jobs(
                session,
                config,
                queue,
                resource_set,
                job_security_time,
                current_time_sec,
            )
            oar_scheduler_redox.check_reservation_jobs(
                redox_platform, redox_slot_sets, queue
            )

    return besteffort_rid2job


def gantt_init_with_running_jobs(
    session, config, plt, initial_time_sec, job_security_time
):
    """
    Initialize gantt tables with scheduled reservation jobs, Running jobs,
    toLaunch jobs and Launching jobs.

    :param oar.kao.platform.Platform plt: \
        Scheduling Platform to schedule jobs.
    :param int scheduled_jobs: \
        Time from which to schedule.
    :param int job_security_time: \
        Job security time.
    """
    #
    # Determine Global Resource Intervals and Initial Slot
    #
    resource_set = plt.resource_set(session, config)

    logger.debug("Processing of processing of already handled reservations")
    moldable_ids = get_waiting_moldable_of_reservations_already_scheduled(session)
    gantt_flush_tables(session, moldable_ids)

    # TODO Can we remove this step, below ???
    #  why don't use: assigned_resources and job start_time ??? in get_scheduled_jobs ???
    logger.debug("Processing of current jobs")
    current_jobs = get_jobs_in_multiple_states(
        session,
        ["Running", "toLaunch", "Launching", "Finishing", "Suspended", "Resuming"],
        resource_set,
    )
    plt.save_assigns(session, current_jobs, resource_set)  # TODO to verify

    #
    # Get already scheduled jobs advanced reservations and jobs from more higher priority queues
    #
    # TODO?: Remove resources of the type specified in
    # SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE
    scheduled_jobs = plt.get_scheduled_jobs(
        session, resource_set, job_security_time, initial_time_sec
    )

    # retrieve resources used by besteffort jobs
    besteffort_rid2job = {}

    for job in scheduled_jobs:
        #  print("job.id:", job.id, job.queue_name, job.types, job.res_set, job.start_time)
        if "besteffort" in job.types:
            for r_id in list(job.res_set):
                besteffort_rid2job[r_id] = job

    return (scheduled_jobs, besteffort_rid2job)
