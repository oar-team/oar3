#!/usr/bin/env python
# coding: utf-8
import sys

from oar.lib import config, get_logger
from oar.kao.platform import Platform
from oar.lib.job_handling import NO_PLACEHOLDER, JobPseudo
from oar.kao.slot import SlotSet, MAX_TIME
from oar.kao.scheduling import (set_slots_with_prev_scheduled_jobs,
                                schedule_id_jobs_ct)
from oar.kao.karma import karma_jobs_sorting
from oar.kao.quotas import load_quotas_rules
import oar.kao.advanced_job_sorting

# Constant duration time of a besteffort job *)
besteffort_duration = 300  # TODO conf ???

# Set undefined config value to default one
DEFAULT_CONFIG = {
    'HIERARCHY_LABELS': 'resource_id,network_address',
    'SCHEDULER_RESOURCE_ORDER': "resource_id ASC",
    'SCHEDULER_JOB_SECURITY_TIME': '60',
    'SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE': 'default',
    'FAIRSHARING_ENABLED': 'no',
    'SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER': '30',
    'QUOTAS': 'no',
    'JOB_SORTING': 'default',
    'JOB_SORTING_CONFIG': ''
}


logger = get_logger("oar.kamelot")

if ('QUOTAS' in config) and (config['QUOTAS'] == 'yes'):
    if 'QUOTAS_FILE' not in config:
        config['QUOTAS_FILE'] = './quotas_conf.json'
    load_quotas_rules()


def karma_job_sorting(queue, now, waiting_jids, waiting_jobs, plt):

    waiting_ordered_jids = waiting_jids
    #
    # Karma job sorting (Fairsharing)
    #
    if "FAIRSHARING_ENABLED" in config:
        if config["FAIRSHARING_ENABLED"] == "yes":
            waiting_ordered_jids = karma_jobs_sorting(queue, now, waiting_jids, waiting_jobs, plt)

    #
    # Advanced job sorting
    #
    if ("JOB_SORTING" in config) and (config["JOB_SORTING"] != "default"):
        job_sorting_func = getattr(oar.kao.advanced_job_sorting,
                                   'job_sorting_%s' % config["JOB_SORTING"])
        if "JOB_SORTING_CONFIG" not in config:
            config["JOB_SORTING_CONFIG"] = "{}"

            waiting_ordered_jids = job_sorting_func(queue, now, waiting_jids,
                                                    waiting_jobs, config["JOB_SORTING_CONFIG"], plt)

    return waiting_ordered_jids


def internal_schedule_cycle(plt, now, all_slot_sets, job_security_time, queue):

    resource_set = plt.resource_set()

    #
    # Retrieve waiting jobs
    #
    waiting_jobs, waiting_jids, nb_waiting_jobs = plt.get_waiting_jobs(queue)

    if nb_waiting_jobs > 0:
        logger.info("nb_waiting_jobs:" + str(nb_waiting_jobs))
        for jid in waiting_jids:
            logger.debug("waiting_jid: " + str(jid))

        #
        # Get  additional waiting jobs' data
        #
        plt.get_data_jobs(
            waiting_jobs, waiting_jids, resource_set, job_security_time)

        waiting_ordered_jids = karma_job_sorting(queue, now, waiting_jids, waiting_jobs, plt)

        #
        # Scheduled
        #
        schedule_id_jobs_ct(all_slot_sets,
                            waiting_jobs,
                            resource_set.hierarchy,
                            waiting_ordered_jids,
                            job_security_time)

        #
        # Save assignement
        #
        logger.info("save assignement")

        plt.save_assigns(waiting_jobs, resource_set)
    else:
        logger.info("no waiting jobs")


def schedule_cycle(plt, now, queue="default"):

    logger.info("Begin scheduling....now: " + str(now) + ", queue: " + queue)

    #
    # Retrieve waiting jobs
    #
    waiting_jobs, waiting_jids, nb_waiting_jobs = plt.get_waiting_jobs(queue)

    if nb_waiting_jobs > 0:
        logger.info("nb_waiting_jobs:" + str(nb_waiting_jobs))
        for jid in waiting_jids:
            logger.debug("waiting_jid: " + str(jid))

        job_security_time = int(config["SCHEDULER_JOB_SECURITY_TIME"])

        #
        # Determine Global Resource Intervals and Initial Slot
        #
        resource_set = plt.resource_set()
        initial_slot_set = SlotSet((resource_set.roid_itvs, now))

        #
        #  Resource availabilty (Available_upto field) is integrated through pseudo job
        #
        pseudo_jobs = []
        for t_avail_upto in sorted(resource_set.available_upto.keys()):
            itvs = resource_set.available_upto[t_avail_upto]
            j = JobPseudo()
            # print t_avail_upto, max_time - t_avail_upto, itvs
            j.start_time = t_avail_upto
            j.walltime = MAX_TIME - t_avail_upto
            j.res_set = itvs
            j.ts = False
            j.ph = NO_PLACEHOLDER

            pseudo_jobs.append(j)

        if pseudo_jobs != []:
            initial_slot_set.split_slots_jobs(pseudo_jobs)
            
        #
        # Get  additional waiting jobs' data
        #
        plt.get_data_jobs(
            waiting_jobs, waiting_jids, resource_set, job_security_time)

        # Job sorting (karma and advanced)
        waiting_ordered_jids = karma_job_sorting(queue, now, waiting_jids, waiting_jobs, plt)

        #
        # Get already scheduled jobs advanced reservations and jobs from more higher priority queues
        #
        scheduled_jobs = plt.get_scheduled_jobs(resource_set, job_security_time, now)

        all_slot_sets = {'default': initial_slot_set}

        if scheduled_jobs != []:
            if queue == 'besteffort':
                filter_besteffort = False
            else:
                filter_besteffort = True
            set_slots_with_prev_scheduled_jobs(all_slot_sets,
                                               scheduled_jobs,
                                               job_security_time,
                                               now,
                                               filter_besteffort)
        #
        # Scheduled
        #
        schedule_id_jobs_ct(all_slot_sets,
                            waiting_jobs,
                            resource_set.hierarchy,
                            waiting_ordered_jids,
                            job_security_time)

        #
        # Save assignement
        #
        logger.info("save assignement")

        plt.save_assigns(waiting_jobs, resource_set)
    else:
        logger.info("no waiting jobs")

#
# Main function
#
def main():
    config['LOG_FILE'] = '/tmp/oar_kamelot.log'
    logger = get_logger("oar.kamelot", forward_stderr=True)
    config.setdefault_config(DEFAULT_CONFIG)

    plt = Platform()

    logger.debug("argv..." + str(sys.argv))

    if len(sys.argv) > 2:
        schedule_cycle(plt, int(float(sys.argv[2])), sys.argv[1])
    elif len(sys.argv) == 2:
        schedule_cycle(plt, plt.get_time(), sys.argv[1])
    else:
        schedule_cycle(plt, plt.get_time())

    logger.info("That's all folks")
    from oar.lib import db
    db.commit()


if __name__ == '__main__':  # pragma: no cover
    config['LOG_FILE'] = '/tmp/oar_kamelot.log'
    logger = get_logger("oar.kamelot", forward_stderr=True)
    main()
