# coding: utf-8
"""
This module defines the top level scheduler of OAR.
It iteratively calls scheduling algorithms on the different queues based on their priorities.
"""

import os
import re
import sys

from procset import ProcSet

import oar.lib.tools as tools
from oar.kao.kamelot import internal_schedule_cycle
from oar.kao.platform import Platform

# for quotas
from oar.kao.quotas import Quotas
from oar.kao.scheduling import (
    find_resource_hierarchies_job,
    get_encompassing_slots,
    set_slots_with_prev_scheduled_jobs,
)
from oar.kao.slot import (
    MAX_TIME,
    SlotSet,
    intersec_itvs_slots,
    intersec_ts_ph_itvs_slots,
)

# for walltime change requests
from oar.kao.walltime_change import process_walltime_change_requests
from oar.lib.event import add_new_event, get_job_events
from oar.lib.globals import init_oar
from oar.lib.job_handling import (
    ALLOW,
    NO_PLACEHOLDER,
    JobPseudo,
    add_resource_job_pairs,
    frag_job,
    gantt_flush_tables,
    get_after_sched_no_AR_jobs,
    get_cpuset_values,
    get_current_not_waiting_jobs,
    get_gantt_jobs_to_launch,
    get_gantt_waiting_interactive_prediction_date,
    get_jobs_in_multiple_states,
    get_jobs_on_resuming_job_resources,
    get_waiting_moldable_of_reservations_already_scheduled,
    get_waiting_scheduled_AR_jobs,
    is_timesharing_for_two_jobs,
    remove_gantt_resource_job,
    resume_job_action,
    save_assigns,
    set_gantt_job_start_time,
    set_job_message,
    set_job_resa_state,
    set_job_start_time_assigned_moldable_id,
    set_job_state,
    set_moldable_job_max_time,
)
from oar.lib.logging import get_logger
from oar.lib.models import GanttJobsPredictionsVisu, GanttJobsResourcesVisu
from oar.lib.node import (
    get_gantt_hostname_to_wake_up,
    get_last_wake_up_date_of_node,
    get_next_job_date_on_node,
    search_idle_nodes,
)
from oar.lib.plugins import find_plugin_function
from oar.lib.queue import get_queues_groupby_priority, stop_queue
from oar.lib.tools import PIPE, TimeoutExpired, duration_to_sql, local_to_sql
from oar.modules.hulot import HulotClient

# FIXME global config
config, _, log = init_oar()

# Constant duration time of a besteffort job *)
besteffort_duration = 300  # TODO conf ???

EXTRA_METASCHED_FUNC_ENTRY_POINT = "oar.extra_metasched_func"

# TODO : not used, to confirm
# timeout for validating reservation
# reservation_validation_timeout = 30

# waiting time when a reservation has not all of its nodes
reservation_waiting_timeout = int(config["RESERVATION_WAITING_RESOURCES_TIMEOUT"])

config["LOG_FILE"] = ":stderr:"
# Log category
logger = get_logger(log, "oar.kao.meta_sched")

exit_code = 0

# stock the job ids that where already send to almighty
to_launch_jobs_already_treated = {}

# order_part = config['SCHEDULER_RESOURCE_ORDER']


batsim_sched_proxy = None


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


def notify_to_run_job(jid):
    """
    Tell bipbip commander to run a job. It can also notifies oar2 almighty if METASCHEDULER_OAR3_WITH_OAR2 configuration variable is set to yes.
    """

    if jid not in to_launch_jobs_already_treated:
        if 0:  # TODO OAR::IO::is_job_desktop_computing
            logger.debug(str(jid) + ": Desktop computing job, I don't handle it!")
        else:
            if config["METASCHEDULER_OAR3_WITH_OAR2"] == "yes":
                logger.debug("OAR2 MODE")
                completed = tools.notify_oar2_almighty("OARRUNJOB_{}".format(jid))

                if completed > 0:
                    to_launch_jobs_already_treated[jid] = 1
                    logger.debug("Notify oar2 almighty to launch the job " + str(jid))
                else:
                    logger.warning(
                        "Not able to oar2 almighty to launch the job "
                        + str(jid)
                        + " (socket error)"
                    )
            else:
                completed = tools.notify_bipbip_commander(
                    {"job_id": int(jid), "cmd": "OARRUN", "args": []}
                )

                if completed:
                    to_launch_jobs_already_treated[jid] = 1
                    logger.debug(
                        "Notify bipbip commander to launch the job " + str(jid)
                    )
                else:
                    logger.warning(
                        "Not able to notify bipbip commander to launch the job "
                        + str(jid)
                        + " (socket error)"
                    )


def prepare_job_to_be_launched(session, job, current_time_sec):
    """
    Prepare a job to be run by bipbip
    """

    # TODO ???
    # my $running_date = $current_time_sec;
    # if ($running_date < $job_submission_time){
    #    $running_date = $job_submission_time;
    # }

    # OAR::IO::set_running_date_arbitrary($base, $job_id, $running_date);
    # OAR::IO::set_assigned_moldable_job($base, $job_id, $moldable_job_id);

    # set start_time an for jobs to launch
    set_job_start_time_assigned_moldable_id(
        session, job.id, current_time_sec, job.moldable_id
    )

    # fix resource assignement
    add_resource_job_pairs(session, job.moldable_id)

    set_job_state(session, job.id, "toLaunch")

    notify_to_run_job(job.id)


def handle_waiting_reservation_jobs(
    session, queue_name, resource_set, job_security_time, current_time_sec
):
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
            set_job_state(session, job.id, "Error")
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
    session, plt, resource_set, queue_name, all_slot_sets, current_time_sec
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
                set_job_state(session, job.id, "toError")
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

            slots = all_slot_sets[ss_name].slots

            t_e = job.start_time + walltime - job_security_time
            sid_left, sid_right = get_encompassing_slots(slots, job.start_time, t_e)

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
                (quotas_ok, quotas_msg, rule, value) = res
                if not quotas_ok:
                    itvs = ProcSet()
                    logger.info(
                        "Quotas limitation reached, job:"
                        + str(job.id)
                        + ", "
                        + quotas_msg
                        + ", rule: "
                        + str(rule)
                        + ", value: "
                        + str(value)
                    )
                    set_job_state(session, job.id, "toError")
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
                set_job_state(session, job.id, "toError")
                set_job_message(session, job.id, "This advance reservation cannot run")
            else:
                # The reservation can be scheduled
                logger.debug("[" + str(job.id) + "] advance reservation is validated")
                job.moldable_id = moldable_id
                job.res_set = itvs
                job.walltime = walltime
                ar_jobs_scheduled[job.id] = job
                # if 'container' in job.types
                #    slot = Slot(1, 0, 0, job.res_set[:], job.start_time,
                #                job.start_time + job.walltime - job_security_time)
                # slot.show()
                #    slots_sets[job.id] = SlotSet(slot)
                # Update the slotsets for the next AR to be scheduled within this loop
                all_slot_sets[ss_name].split_slots(sid_left, sid_right, job)
                set_job_state(session, job.id, "toAckReservation")

            set_job_resa_state(session, job.id, "Scheduled")

    if ar_jobs_scheduled != []:
        logger.debug("Save AR jobs' assignements in database")
        save_assigns(session, ar_jobs_scheduled, resource_set)

    logger.debug("Queue " + queue_name + ": end processing of new reservations")


def check_besteffort_jobs_to_kill(
    session,
    jobs_to_launch,
    rid2jid_to_launch,
    current_time_sec,
    besteffort_rid2job,
    resource_set,
):
    """
    Detect if there are besteffort jobs to kill
    return 1 if there is at least 1 job to frag otherwise 0
    """

    return_code = 0

    logger.debug("Begin processing of besteffort jobs to kill")

    fragged_jobs = []

    for rid, job_id in rid2jid_to_launch.items():
        if rid in besteffort_rid2job:
            be_job = besteffort_rid2job[rid]
            job_to_launch = jobs_to_launch[job_id]

            if is_timesharing_for_two_jobs(session, be_job, job_to_launch):
                logger.debug(
                    "Resource "
                    + str(rid)
                    + " is needed for  job "
                    + str(job_id)
                    + ", but besteffort job  "
                    + str(be_job.id)
                    + " can live, because timesharing compatible"
                )
            else:
                if be_job.id not in fragged_jobs:
                    skip_kill = 0
                    checkpoint_first_date = sys.maxsize
                    # Check if we must checkpoint the besteffort job
                    if be_job.checkpoint > 0:
                        for ev in get_job_events(be_job.id):
                            if ev.type == "CHECKPOINT":
                                if checkpoint_first_date > ev.date:
                                    checkpoint_first_date = ev.date

                        if (checkpoint_first_date == sys.maxsize) or (
                            current_time_sec
                            <= (checkpoint_first_date + be_job.checkpoint)
                        ):
                            skip_kill = 1
                            tools.send_checkpoint_signal(be_job)

                            logger.debug(
                                "Send checkpoint signal to the job " + str(be_job.id)
                            )

                    if not skip_kill:
                        logger.debug(
                            "Resource "
                            + str(rid)
                            + " need(s) to be freed for job "
                            + str(job_to_launch.id)
                            + ": killing besteffort job "
                            + str(be_job.id)
                        )

                        add_new_event(
                            session,
                            "BESTEFFORT_KILL",
                            be_job.id,
                            "kill the besteffort job " + str(be_job.id),
                        )
                        frag_job(session, be_job.id)

                    fragged_jobs.append(be_job.id)
                    return_code = 1

    logger.debug("End precessing of besteffort jobs to kill\n")

    return return_code


def handle_jobs_to_launch(
    session, jobs_to_launch_lst, current_time_sec, current_time_sql
):
    logger.debug("Begin processing jobs to launch (start time <= " + current_time_sql)

    return_code = 0

    for job in jobs_to_launch_lst:
        return_code = 1
        logger.debug(
            "Set job " + str(job.id) + " state to toLaunch at " + current_time_sql
        )

        #
        # Advance Reservation
        #
        walltime = job.walltime
        if (job.reservation == "Scheduled") and (job.start_time < current_time_sec):
            max_time = walltime - (current_time_sec - job.start_time)

            set_moldable_job_max_time(session, job.moldable_id, max_time)
            set_gantt_job_start_time(session, job.moldable_id, current_time_sec)
            logger.warning(
                "Reduce walltime of job "
                + str(job.id)
                + "to "
                + str(max_time)
                + "(was  "
                + str(walltime)
                + " )"
            )

            add_new_event(
                session,
                "REDUCE_RESERVATION_WALLTIME",
                job.id,
                "Change walltime from " + str(walltime) + " to " + str(max_time),
            )

            w_max_time = duration_to_sql(max_time)
            new_message = re.sub(r"W=\d+:\d+:\d+", "W=" + w_max_time, job.message)

            if new_message != job.message:
                set_job_message(session, job.id, new_message)

        prepare_job_to_be_launched(session, job, current_time_sec)

    logger.debug("End processing of jobs to launch")

    return return_code


def update_gantt_visualization(session):
    """
    Update the database with the new scheduling decisions for visualizations.
    """
    session.query(GanttJobsPredictionsVisu).delete()
    session.query(GanttJobsResourcesVisu).delete()
    session.commit()

    sql_queries = [
        "INSERT INTO gantt_jobs_predictions_visu SELECT * FROM gantt_jobs_predictions",
        "INSERT INTO gantt_jobs_resources_visu SELECT * FROM gantt_jobs_resources",
    ]
    for query in sql_queries:
        session.execute(query)
    session.commit()


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


def nodes_energy_saving(session, config, current_time_sec):
    """
    Energy saving mode.

    :param int current_time_sec: \
        Current time of the platform.
    :return dict: \
        Dict with two keys: `"halt"` and `"wakeup"` containing the list of node to, respectively, turn off and turn on.
    """
    nodes_2_halt = []
    nodes_2_wakeup = []

    if (
        ("SCHEDULER_NODE_MANAGER_SLEEP_CMD" in config)
        or (
            (config["ENERGY_SAVING_INTERNAL"] == "yes")
            and ("ENERGY_SAVING_NODE_MANAGER_SLEEP_CMD" in config)
        )
    ) and (
        ("SCHEDULER_NODE_MANAGER_SLEEP_TIME" in config)
        and ("SCHEDULER_NODE_MANAGER_IDLE_TIME" in config)
    ):
        # Look at nodes that are unused for a duration
        idle_duration = int(config["SCHEDULER_NODE_MANAGER_IDLE_TIME"])
        sleep_duration = int(config["SCHEDULER_NODE_MANAGER_SLEEP_TIME"])

        idle_nodes = search_idle_nodes(session, current_time_sec)
        tmp_time = current_time_sec - idle_duration

        # Determine nodes to halt
        nodes_2_halt = []
        for node, idle_duration in idle_nodes.items():
            if idle_duration < tmp_time:
                # Search if the node has enough time to sleep
                tmp = get_next_job_date_on_node(session, node)
                if (tmp is None) or (tmp - sleep_duration > current_time_sec):
                    # Search if node has not been woken up recently
                    wakeup_date = get_last_wake_up_date_of_node(session, node)
                    if (wakeup_date is None) or (wakeup_date < tmp_time):
                        nodes_2_halt.append(node)

    if ("SCHEDULER_NODE_MANAGER_SLEEP_CMD" in config) or (
        (config["ENERGY_SAVING_INTERNAL"] == "yes")
        and ("ENERGY_SAVING_NODE_MANAGER_SLEEP_CMD" in config)
    ):
        # Get nodes which the scheduler wants to schedule jobs to,
        # but which are in the Absent state, to wake them up
        wakeup_time = int(config["SCHEDULER_NODE_MANAGER_WAKEUP_TIME"])
        nodes_2_wakeup = get_gantt_hostname_to_wake_up(
            session, current_time_sec, wakeup_time
        )

    return {"halt": nodes_2_halt, "wakeup": nodes_2_wakeup}


def meta_schedule(session, config, mode="internal", plt=Platform()):
    """
    Meta scheduling phase.
    Run the scheduler on each queue dependeding on their priority order.
    It acts in several phases:

    #. Loops through queues order by priority and call the scheduler
    #. It is also responsible to detect best effort jobs that need to be killed
    #. If the energy saving mode is enabled, it calls :class:`Hulot`.
    """
    exit_code = 0

    job_security_time = int(config["SCHEDULER_JOB_SECURITY_TIME"])

    # Kill duration before starting jobs
    # So we kill best effort job in advance to give the time to occupied resources
    # to be in a good state
    if "SCHEDULER_BESTEFFORT_KILL_DURATION_BEFORE_RESERVATION" in config:
        kill_duration_before_reservation = int(
            config["SCHEDULER_BESTEFFORT_KILL_DURATION_BEFORE_RESERVATION"]
        )
    else:
        kill_duration_before_reservation = 0

    if ("QUOTAS" in config) and (config["QUOTAS"] == "yes"):
        Quotas.enable(config, plt.resource_set(session, config))

    if ("WALLTIME_CHANGE_ENABLED" in config) and (
        config["WALLTIME_CHANGE_ENABLED"] == "yes"
    ):
        process_walltime_change_requests(plt)

    tools.create_almighty_socket()

    logger.debug(
        "Retrieve information for already scheduled reservations from \
        database before flush (keep assign resources)"
    )

    # reservation ??.

    initial_time_sec = tools.get_date(session)  # time.time()
    initial_time_sql = local_to_sql(initial_time_sec)

    current_time_sec = initial_time_sec
    current_time_sql = initial_time_sql

    gantt_init_results = gantt_init_with_running_jobs(
        session, config, plt, initial_time_sec, job_security_time
    )
    all_slot_sets, scheduled_jobs, besteffort_rid2jid = gantt_init_results
    resource_set = plt.resource_set(session=session, config=config)

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
            "Queue(s): {},  Launching scheduler, at time: {} ".format(
                " ".join([q.name for q in queues]), initial_time_sql
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
                    queue.name,
                    resource_set,
                    job_security_time,
                    current_time_sec,
                )
                # handle_new_AR_jobs
                check_reservation_jobs(
                    session,
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
                    queue.name,
                    resource_set,
                    job_security_time,
                    current_time_sec,
                )

    (
        jobs_to_launch_with_security_time,
        jobs_to_launch_with_security_time_lst,
        rid2jid_to_launch,
    ) = get_gantt_jobs_to_launch(
        session,
        resource_set,
        job_security_time,
        current_time_sec,
        kill_duration_before_reservation=kill_duration_before_reservation,
    )

    # Filter jobs that are not yet ready to be scheduled, but present because of the
    # kill_duration_before_reservation=kill_duration_before_reservation parameter
    jobs_to_launch_lst = filter(
        lambda j: j.start_time <= current_time_sec,
        jobs_to_launch_with_security_time_lst,
    )

    if (
        check_besteffort_jobs_to_kill(
            session,
            jobs_to_launch_with_security_time,  # Jobs to launch or about to be launched
            rid2jid_to_launch,
            current_time_sec,
            besteffort_rid2jid,
            resource_set,
        )
        == 1
    ):
        # We must kill some besteffort jobs
        tools.notify_almighty("ChState")
        exit_code = 2
    elif (
        handle_jobs_to_launch(
            session, jobs_to_launch_lst, current_time_sec, current_time_sql
        )
        == 1
    ):
        exit_code = 0

    # Update visu gantt tables
    update_gantt_visualization(session)

    #
    # Manage dynamic node feature for energy saving:
    #
    if ("ENERGY_SAVING_MODE" in config) and config["ENERGY_SAVING_MODE"] != "":
        if config["ENERGY_SAVING_MODE"] == "metascheduler_decision_making":
            nodes_2_change = nodes_energy_saving(session, config, current_time_sec)
        elif config["ENERGY_SAVING_MODE"] == "batsim_scheduler_proxy_decision_making":
            nodes_2_change = batsim_sched_proxy.retrieve_pstate_changes_to_apply()
        else:
            logger.error(
                "Error ENERGY_SAVING_MODE unknown: " + config["ENERGY_SAVING_MODE"]
            )

        hulot = HulotClient(config, logger)

        flag_hulot = False
        timeout_cmd = int(config["SCHEDULER_TIMEOUT"])

        # Command Hulot to halt selected nodes
        nodes_2_halt = nodes_2_change["halt"]
        if nodes_2_halt != []:
            logger.debug(
                "Powering off some nodes (energy saving): " + str(nodes_2_halt)
            )
            # Using the built-in energy saving module to shut down nodes
            if config["ENERGY_SAVING_INTERNAL"] == "yes":
                hulot.halt_nodes(nodes_2_halt)
                # logger.error("Communication problem with the energy saving module (Hulot)\n")
                flag_hulot = True
            else:
                # Not using the built-in energy saving module to shut down nodes
                cmd = config["SCHEDULER_NODE_MANAGER_SLEEP_CMD"]
                if tools.fork_and_feed_stdin(cmd, timeout_cmd, nodes_2_halt):
                    logger.error(
                        "Command "
                        + cmd
                        + "timeouted ("
                        + str(timeout_cmd)
                        + "s) while trying to  poweroff some nodes"
                    )

        # Command Hulot to wake up selected nodes
        nodes_2_wakeup = nodes_2_change["wakeup"]
        if nodes_2_wakeup != []:
            logger.debug("Awaking some nodes: " + str(nodes_2_change))
            # Using the built-in energy saving module to wake up nodes
            if config["ENERGY_SAVING_INTERNAL"] == "yes":
                hulot.wake_up_nodes(nodes_2_wakeup)
                # logger.error("Communication problem with the energy saving module (Hulot)")
                flag_hulot = True
            else:
                # Not using the built-in energy saving module to wake up nodes
                cmd = config["SCHEDULER_NODE_MANAGER_WAKE_UP_CMD"]
                if tools.fork_and_feed_stdin(cmd, timeout_cmd, nodes_2_wakeup):
                    logger.error(
                        "Command "
                        + cmd
                        + "timeouted ("
                        + str(timeout_cmd)
                        + "s) while trying to wake-up some nodes "
                    )

        # Send CHECK signal to Hulot if needed
        if not flag_hulot and (config["ENERGY_SAVING_INTERNAL"] == "yes"):
            hulot.check_nodes()
            #    logger.error("Communication problem with the energy saving module (Hulot)")

    # Retrieve jobs according to their state and excluding job in 'Waiting' state.
    jobs_by_state = get_current_not_waiting_jobs(session)

    #
    # Search jobs to resume
    #

    #
    # TODO: TOFINISH
    #
    if "Resuming" in jobs_by_state:
        logger.warning("Resuming job is NOT ENTIRELY IMPLEMENTED")
        for job in jobs_by_state["Resuming"]:
            other_jobs = get_jobs_on_resuming_job_resources(session, job.id)
            # TODO : look for timesharing other jobs. What do we do?????
            if other_jobs == []:
                # We can resume the job
                logger.debug("[" + str(job.id) + "] Resuming job")
                if "noop" in job.types:
                    resume_job_action(session,job.id)
                    logger.debug("[" + str(job.id) + "] Resume NOOP job OK")
                else:
                    script = config["JUST_BEFORE_RESUME_EXEC_FILE"]
                    timeout = int(config["SUSPEND_RESUME_SCRIPT_TIMEOUT"])
                    if timeout is None:
                        timeout = tools.get_default_suspend_resume_script_timeout()
                    skip = 0
                    logger.debug(
                        "["
                        + str(job.id)
                        + "] Running post suspend script: `"
                        + script
                        + " "
                        + str(job.id)
                        + "'"
                    )
                    return_code = -1
                    try:
                        return_code = tools.call(
                            [script, str(job.id)], shell=True, timeout=timeout
                        )
                    except TimeoutExpired as e:
                        logger.error(
                            str(e) + "[" + str(job.id) + "] Suspend script timeouted"
                        )
                        add_new_event(session,
                            "RESUME_SCRIPT_ERROR", job.id, "Suspend script timeouted"
                        )
                    if return_code != 0:
                        str_error = (
                            "["
                            + str(job.id)
                            + "] Suspend script error, return code = "
                            + str(return_code)
                        )
                        logger.error(str_error)
                        add_new_event(session,"RESUME_SCRIPT_ERROR", job.id, str_error)
                        frag_job(session,job.id)
                        tools.notify_almighty("Qdel")
                    skip = 1

                cpuset_nodes = None
                if "JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD" in config:
                    cpuset_field = config["JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD"]
                else:
                    cpuset_field = ""
                if cpuset_field and (skip == 0):
                    # TODO
                    cpuset_name = job.user + "_" + str(job.id)
                    cpuset_nodes = get_cpuset_values(
                        cpuset_field, job.assigned_moldable_id
                    )
                    # TODO
                    suspend_data_hash = {  # noqa: F841
                        "name": cpuset_name,
                        "job_id": job.id,
                        "oarexec_pid_file": tools.get_oar_pid_file_name(job.id),
                    }
                if cpuset_nodes:
                    # TODO
                    taktuk_cmd = config["TAKTUK_CMD"]  # noqa: F841
                    if "SUSPEND_RESUME_FILE" in config:
                        suspend_file = config["SUSPEND_RESUME_FILE"]
                    else:
                        # TODO
                        suspend_file = (  # noqa: F841
                            tools.get_default_suspend_resume_file()
                        )

    #
    # TODO: TOFINISH
    #

    # Notify oarsub -I when they will be launched
    for j_info in get_gantt_waiting_interactive_prediction_date(session):
        job_id, job_info_type, job_start_time, job_message = j_info
        addr, port = job_info_type.split(":")
        new_start_prediction = local_to_sql(job_start_time)
        logger.debug(
            "["
            + str(job_id)
            + "] Notifying user of the start prediction: "
            + new_start_prediction
            + "("
            + job_message
            + ")"
        )
        tools.notify_tcp_socket(
            addr,
            port,
            "["
            + initial_time_sql
            + "] Start prediction: "
            + new_start_prediction
            + " ("
            + job_message
            + ")",
        )

    # Run the decisions
    # Process "toError" jobs
    if "toError" in jobs_by_state:
        for job in jobs_by_state["toError"]:
            addr, port = job.info_type.split(":")
            if job.type == "INTERACTIVE" or (
                job.type == "PASSIVE" and job.reservation == "Scheduled"
            ):
                logger.debug(
                    "Notify oarsub job (num:"
                    + str(job.id)
                    + ") in error; jobInfo="
                    + job.info_type
                )

                nb_sent1 = tools.notify_tcp_socket(addr, port, job.message + "\n")
                nb_sent2 = tools.notify_tcp_socket(addr, port, "BAD JOB" + "\n")
                if (nb_sent1 == 0) or (nb_sent2 == 0):
                    logger.warning(
                        "Cannot open connection to oarsub client for" + str(job.id)
                    )
            logger.debug("Set job " + str(job.id) + " to state Error")
            set_job_state(session, job.id, "Error")

    # Process toAckReservation jobs
    if "toAckReservation" in jobs_by_state:
        for job in jobs_by_state["toAckReservation"]:
            addr, port = job.info_type.split(":")
            logger.debug("Treate job" + str(job.id) + " in toAckReservation state")

            nb_sent = tools.notify_tcp_socket(addr, port, "GOOD RESERVATION" + "\n")

            if nb_sent == 0:
                logger.warning(
                    "Frag job "
                    + str(job.id)
                    + ", I cannot notify oarsub for the reservation"
                )
                add_new_event(
                    "CANNOT_NOTIFY_OARSUB",
                    str(job.id),
                    "Can not notify oarsub for the job " + str(job.id),
                )

                # TODO ???
                # OAR::IO::lock_table / OAR::IO::unlock_table($base)
                frag_job(job.id)

                exit_code = 2
            else:
                logger.debug(
                    "Notify oarsub for a RESERVATION (idJob="
                    + str(job.id)
                    + ") --> OK; jobInfo="
                    + job.info_type
                )
                set_job_state(session, job.id, "Waiting")
                if ((job.start_time - 1) <= current_time_sec) and (exit_code == 0):
                    exit_code = 1

    # Process toLaunch jobs
    if "toLaunch" in jobs_by_state:
        for job in jobs_by_state["toLaunch"]:
            notify_to_run_job(job.id)

    logger.debug("End of Meta Scheduler")

    return exit_code
