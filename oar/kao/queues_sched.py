import os
import re
from typing import Dict

from procset import ProcSet

import oar.lib.tools as tools
from oar.kao.kamelot import internal_schedule_cycle
from oar.kao.platform import Platform
from oar.kao.quotas import Quotas
from oar.kao.scheduling import (
    find_resource_hierarchies_job,
    set_slots_with_prev_scheduled_jobs,
)
from oar.kao.slot import (
    MAX_TIME,
    SlotSet,
    intersec_itvs_slots,
    intersec_ts_ph_itvs_slots,
)

# for walltime change requests
from oar.lib.configuration import Configuration
from oar.lib.event import add_new_event
from oar.lib.globals import get_logger
from oar.lib.job_handling import (
    ALLOW,
    NO_PLACEHOLDER,
    JobPseudo,
    gantt_flush_tables,
    get_after_sched_no_AR_jobs,
    get_jobs_in_multiple_states,
    get_waiting_moldable_of_reservations_already_scheduled,
    get_waiting_scheduled_AR_jobs,
    remove_gantt_resource_job,
    save_assigns,
    set_gantt_job_start_time,
    set_job_message,
    set_job_resa_state,
    set_job_state,
)
from oar.lib.plugins import find_plugin_function
from oar.lib.queue import get_queues_groupby_priority, stop_queue
from oar.lib.tools import PIPE

# for quotas


logger = get_logger("oar.kao.queues_sched")
EXTRA_METASCHED_FUNC_ENTRY_POINT = "oar.extra_metasched_func"


def queues_schedule(
    session,
    config,
    mode,
    plt,
    resource_set,
    initial_time_sec,
    initial_time_sql,
    current_time_sec,
    job_security_time,
):

    all_slot_sets, scheduled_jobs, besteffort_rid2job = gantt_init_with_running_jobs(
        session, config, plt, initial_time_sec, job_security_time
    )

    # Path for user of external schedulers
    if "OARDIR" in os.environ:
        binpath = os.environ["OARDIR"] + "/"
    else:
        binpath = "/usr/local/lib/oar"
        logger.warning(
            "OARDIR env variable must be defined, " + binpath + " is used by default"
        )

    if ("EXTRA_METASCHED" in config) and (config["EXTRA_METASCHED"] != "default"):
        extra_metasched_func = find_plugin_function(
            EXTRA_METASCHED_FUNC_ENTRY_POINT, config["EXTRA_METASCHED"]
        )
        if "EXTRA_METASCHED_CONFIG" in config:
            extra_metasched_config = config["EXTRA_METASCHED_CONFIG"]
        else:
            extra_metasched_config = ""
    else:

        def extra_metasched_func(*args):  # null function
            pass

        extra_metasched_config = ""

    prev_queues = None

    for queues in get_queues_groupby_priority(session):
        extra_metasched_func(
            session,
            prev_queues,
            plt,
            scheduled_jobs,
            all_slot_sets,
            job_security_time,
            queues,
            initial_time_sec,
            extra_metasched_config,
        )

        logger.debug(
            "Queue(s): {},  Launching scheduler, at time: {}, ({})".format(
                " ".join([q.name for q in queues]), initial_time_sql, initial_time_sec
            )
        )

        prev_queues = queues

        # filter active queues
        active_queues = [q for q in queues if q.state == "Active"]

        # Only internal scheduler support non-strict priorities between queues
        if mode == "internal":
            call_internal_scheduler(
                session,
                config,
                plt,
                scheduled_jobs,
                all_slot_sets,
                job_security_time,
                active_queues,
                initial_time_sec,
            )
            for queue in active_queues:
                handle_waiting_reservation_jobs(
                    session,
                    config,
                    queue.name,
                    resource_set,
                    job_security_time,
                    current_time_sec,
                )
                # handle_new_AR_jobs
                check_reservation_jobs(
                    session,
                    config,
                    plt,
                    resource_set,
                    queue.name,
                    all_slot_sets,
                    current_time_sec,
                )
        else:
            for queue in active_queues:
                if mode == "external":  # pragma: no cover
                    call_external_scheduler(
                        session,
                        binpath,
                        scheduled_jobs,
                        all_slot_sets,
                        resource_set,
                        job_security_time,
                        queue,
                        initial_time_sec,
                        initial_time_sql,
                    )
                elif mode == "batsim_sched_proxy":
                    call_batsim_sched_proxy(
                        plt,
                        scheduled_jobs,
                        all_slot_sets,
                        job_security_time,
                        queue,
                        initial_time_sec,
                    )
                else:
                    logger.error("Specified mode is unknown: " + mode)

                handle_waiting_reservation_jobs(
                    session,
                    config,
                    queue.name,
                    resource_set,
                    job_security_time,
                    current_time_sec,
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
    initial_slot_set = SlotSet((resource_set.roid_itvs, initial_time_sec))

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
    #  Resource availabilty (Available_upto field) is integrated through pseudo job
    #
    pseudo_jobs = []
    for t_avail_upto in sorted(resource_set.available_upto.keys()):
        itvs = resource_set.available_upto[t_avail_upto]
        j = JobPseudo()
        j.start_time = t_avail_upto
        j.walltime = MAX_TIME - t_avail_upto
        j.res_set = itvs
        j.ts = False
        j.ph = NO_PLACEHOLDER

        pseudo_jobs.append(j)

    if pseudo_jobs != []:
        initial_slot_set.split_slots_jobs(pseudo_jobs)

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

    # Create and fill gantt
    all_slot_sets = {"default": initial_slot_set}
    if scheduled_jobs != []:
        filter_besteffort = True
        set_slots_with_prev_scheduled_jobs(
            all_slot_sets,
            scheduled_jobs,
            job_security_time,
            initial_time_sec,
            filter_besteffort,
        )

    return (all_slot_sets, scheduled_jobs, besteffort_rid2job)


def call_external_scheduler(
    session,
    binpath,
    scheduled_jobs,
    all_slot_sets,
    resource_set,
    job_security_time,
    queue,
    initial_time_sec,
    initial_time_sql,
):  # pragma: no cover
    """
    Call scheduler from command line.

        :param int binpath: \
            Base path of the schedulers folder.
        :param List[Job] scheduled_jobs: \
            TODO: Not used (or rather overridden).
        :param SlotSet all_slot_sets: \
            TODO:
        :param List resource_set: \
            TODO:
        :param int job_security_time: \
            Job security time (TODO: Link to conf documentation).
        :param Queue queue: \
            Queue to operate on.
        :param int initial_time_sec: \
            Time to from which to begin the scheduling (TODO: verify explanation).
        :param int initial_time_sql: \
            Minimun time at which jobs will be retrieved (TODO: verify explanation).
    """
    cmd_scheduler = binpath + "schedulers/" + queue.scheduler_policy

    child_launched = True
    # TODO TO CONFIRM
    sched_exit_code = 0
    sched_signal_num = 0
    sched_dumped_core = 0
    try:
        child = tools.Popen(
            [cmd_scheduler, queue.name, str(initial_time_sec), initial_time_sql],
            stdout=PIPE,
        )

        for line in iter(child.stdout.readline, ""):
            logger.debug("Read on the scheduler output:" + str(line.rstrip()))

        # TODO SCHEDULER_LAUNCHER_OPTIMIZATION
        # if
        # ((get_conf_with_default_param('SCHEDULER_LAUNCHER_OPTIMIZATION',
        # 'yes') eq 'yes') and

        rc = child.wait()

        sched_exit_code, sched_signal_num, sched_dumped_core = (
            rc >> 8,
            rc & 0x7F,
            bool(rc & 0x80),
        )

    except OSError as e:
        child_launched = False
        logger.warning(
            str(e)
            + " Cannot run: "
            + cmd_scheduler
            + " "
            + queue.name
            + " "
            + str(initial_time_sec)
            + " "
            + initial_time_sql
        )

    if (not child_launched) or (sched_signal_num != 0) or (sched_dumped_core != 0):
        logger.error(
            "Execution of {}".format(queue.scheduler_policy)
            + f" failed (signal={sched_signal_num})"
            + " Disabling queue {} (see `oarnotify')".format(queue.name)
        )
        # stop queue
        stop_queue(session, queue.name)

    if sched_exit_code != 0:
        logger.error(
            "Scheduler "
            + queue.scheduler_policy
            + f" returned a bad value: {sched_exit_code}"
            + f", at time {initial_time_sec}"
            + ". Disabling queue "
            + "{} (see `oarnotify')".format(queue.name)
        )
        # stop queue
        stop_queue(session, queue.name)

    # retrieve jobs and assignement decision from previous scheduling step
    scheduled_jobs = get_after_sched_no_AR_jobs(
        session, queue.name, resource_set, job_security_time, initial_time_sec
    )

    if scheduled_jobs != []:
        if queue.name == "besteffort":
            filter_besteffort = False
        else:
            filter_besteffort = True

        set_slots_with_prev_scheduled_jobs(
            all_slot_sets,
            scheduled_jobs,
            job_security_time,
            initial_time_sec,
            filter_besteffort,
        )


def call_batsim_sched_proxy(
    plt, scheduled_jobs, all_slot_sets, job_security_time, queue, now
):
    from oar.kao.batsim_sched_proxy import BatsimSchedProxy

    global batsim_sched_proxy
    batsim_sched_proxy = BatsimSchedProxy(
        plt, scheduled_jobs, all_slot_sets, job_security_time, queue, now
    )
    batsim_sched_proxy.ask_schedule()


def call_internal_scheduler(
    session, config, plt, scheduled_jobs, all_slot_sets, job_security_time, queues, now
):
    """
    Internal scheduling phase. The scheduler is not loaded from an external command,
    so it can shares states with the metascheduler and between scheduling phases (on each queues).
    """

    # Place running besteffort jobs if their queue is considered
    if (len(queues) == 1) and (queues[0].name == "besteffort"):
        set_slots_with_prev_scheduled_jobs(
            all_slot_sets, scheduled_jobs, job_security_time, now, False, True
        )

    internal_schedule_cycle(
        session,
        config,
        plt,
        now,
        all_slot_sets,
        job_security_time,
        [q.name for q in queues],
    )


def handle_waiting_reservation_jobs(
    session, config, queue_name, resource_set, job_security_time, current_time_sec
):
    reservation_waiting_timeout = int(config["RESERVATION_WAITING_RESOURCES_TIMEOUT"])
    logger.debug(
        "Queue " + queue_name + ": begin processing accepted Advance Reservations"
    )

    ar_jobs = get_waiting_scheduled_AR_jobs(
        session, queue_name, resource_set, job_security_time, current_time_sec
    )

    for job in ar_jobs:
        moldable_id = job.moldable_id
        walltime = job.walltime

        # Test if AR job is expired and handle it
        if current_time_sec > (job.start_time + walltime):
            logger.warning(
                "["
                + str(job.id)
                + "] set job state to Error: avdance reservation expired and couldn't be started"
            )
            set_job_state(session, config, job.id, "Error")
            set_job_message(
                session, job.id, "Reservation expired and couldn't be started."
            )
        else:
            # Determine current available resources
            avail_res = resource_set.roid_itvs & job.res_set

            # Test if the AR job is waiting to be launched due to nodes' unavailabilities
            if (len(avail_res) == 0) and (job.start_time < current_time_sec):
                logger.warning(
                    "[%s] advance reservation is waiting because no resource is present"
                    % str(job.id)
                )

                # Delay launching time
                set_gantt_job_start_time(session, moldable_id, current_time_sec + 1)
            elif job.start_time < current_time_sec:
                # TODO: not tested
                if (job.start_time + reservation_waiting_timeout) > current_time_sec:
                    if avail_res != job.res_set:
                        # The expected resources are not all available,
                        # wait the specified timeout
                        logger.warning(
                            "["
                            + str(job.id)
                            + "] advance reservation is waiting because not all \
                                       resources are available yet (timeout in "
                            + (
                                job.start_time
                                + reservation_waiting_timeout
                                - current_time_sec
                            )
                            + " seconds)"
                        )
                        set_gantt_job_start_time(
                            session, moldable_id, current_time_sec + 1
                        )
                else:
                    # It's time to launch the AR job, remove missing resources
                    missing_resources_itvs = job.res_set - avail_res
                    remove_gantt_resource_job(
                        session, moldable_id, missing_resources_itvs, resource_set
                    )
                    logger.warning(
                        "["
                        + str(job.id)
                        + "remove some resources assigned to this advance reservation, \
                                   because there are not Alive"
                    )

                    add_new_event(
                        session,
                        "SCHEDULER_REDUCE_NB_RESSOURCES_FOR_RESERVATION",
                        job.id,
                        "[MetaSched] Reduce the number of resources for the job "
                        + str(job.id),
                    )

                    nb_res = len(job.res_set) - len(missing_resources_itvs)
                    new_message = re.sub(r"R=\d+", "R=" + str(nb_res), job.message)
                    if new_message != job.message:
                        set_job_message(session, job.id, new_message)

    logger.debug(
        "Queue "
        + queue_name
        + ": end processing of reservations with missing resources"
    )


def check_reservation_jobs(
    session,
    config: Configuration,
    plt: Platform,
    resource_set: ProcSet,
    queue_name: str,
    all_slot_sets: Dict[str, SlotSet],
    current_time_sec,
):
    """Processing of new Advance Reservations"""

    logger.debug("Queue " + queue_name + ": begin processing of new reservations")

    ar_jobs_scheduled = {}

    ar_jobs, ar_jids, nb_ar_jobs = plt.get_waiting_jobs(
        queue_name, "toSchedule", session=session
    )
    logger.debug("nb_ar_jobs:" + str(nb_ar_jobs))

    if nb_ar_jobs > 0:
        job_security_time = int(config["SCHEDULER_JOB_SECURITY_TIME"])
        plt.get_data_jobs(session, ar_jobs, ar_jids, resource_set, job_security_time)

        logger.debug("Try and schedule new Advance Reservations")
        for jid in ar_jids:
            job = ar_jobs[jid]
            logger.debug("Find resource for Advance Reservation job:" + str(job.id))

            # It is a reservation, we take care only of the first moldable job
            moldable_id, walltime, hy_res_rqts = job.mld_res_rqts[0]

            # test if reservation is too old
            if current_time_sec >= (job.start_time + walltime):
                logger.warning(
                    "[" + str(job.id) + "] Canceling job: reservation is too old"
                )
                set_job_message(session, job.id, "Reservation too old")
                set_job_state(session, config, job.id, "toError")
                continue
            else:
                if job.start_time < current_time_sec:
                    # TODO update to DB ????
                    job.start_time = current_time_sec

            ss_name = "default"

            # TODO container
            # if 'inner' in job.types:
            #    ss_name = job.types['inner']

            # TODO: test if container is an AR job

            slots_set = all_slot_sets[ss_name]

            t_e = job.start_time + walltime - job_security_time
            sid_left, sid_right = slots_set.get_encompassing_slots(job.start_time, t_e)

            slots = slots_set.slots

            if job.ts or (job.ph == ALLOW):
                itvs_avail = intersec_ts_ph_itvs_slots(slots, sid_left, sid_right, job)
            else:
                itvs_avail = intersec_itvs_slots(slots, sid_left, sid_right)

            itvs = find_resource_hierarchies_job(
                itvs_avail, hy_res_rqts, resource_set.hierarchy
            )

            if Quotas.enabled:
                nb_res = len(itvs & resource_set.default_itvs)
                res = Quotas.check_slots_quotas(
                    slots, sid_left, sid_right, job, nb_res, walltime
                )
                print(f"res: {res}")
                (quotas_ok, quotas_msg, rule, value) = res
                if not quotas_ok:
                    itvs = ProcSet()
                    logger.info(
                        f"Quotas limitation reached, job:{str(job.id)}, {quotas_msg}, rule: {str(rule)}, value: {str(value)}"
                    )
                    set_job_state(session, config, job.id, "toError")
                    set_job_message(
                        session,
                        job.id,
                        "This advance reservation cannot run due to quotas",
                    )

            if len(itvs) == 0:
                # not enough resource available
                logger.warning(
                    "["
                    + str(job.id)
                    + "] advance reservation cannot be validated, not enough resources"
                )
                set_job_state(session, config, job.id, "toError")
                set_job_message(session, job.id, "This advance reservation cannot run")
            else:
                # The reservation can be scheduled
                logger.debug("[" + str(job.id) + "] advance reservation is validated")
                job.moldable_id = moldable_id
                job.res_set = itvs
                job.walltime = walltime
                ar_jobs_scheduled[job.id] = job

                (sid_left, sid_right) = all_slot_sets[ss_name].get_encompassing_range(
                    job.start_time, job.start_time + job.walltime
                )

                # print(f"what should: {(a, b)}, what is: {(sid_left, sid_right)} security: {job_security_time}")
                print(
                    f"check for yourself {job.start_time} + {job.walltime} = {job.start_time + job.walltime}:\n{all_slot_sets[ss_name]}"
                )
                all_slot_sets[ss_name].split_slots(sid_left, sid_right, job)
                set_job_state(session, config, job.id, "toAckReservation")

            set_job_resa_state(session, job.id, "Scheduled")

    if ar_jobs_scheduled != []:
        logger.debug("Save AR jobs' assignements in database")
        save_assigns(session, ar_jobs_scheduled, resource_set)

    logger.debug("Queue " + queue_name + ": end processing of new reservations")
