#!/usr/bin/env python
# coding: utf-8
from oar.kao.platform import Platform
from oar.kao.scheduling_basic import schedule_id_jobs_ct
from oar.kao.slot import MAX_TIME, SlotSet
from oar.lib.globals import get_logger, init_oar
from oar.lib.job_handling import NO_PLACEHOLDER, JobPseudo

config, _, log, session_factory = init_oar()

logger = get_logger("oar.kamelot_basic")


def schedule_cycle(session, config, plt, queues=["default"]):
    now = plt.get_time()

    logger.info("Begin scheduling....", now)

    #
    # Retrieve waiting jobs
    #
    waiting_jobs, waiting_jids, nb_waiting_jobs = plt.get_waiting_jobs(
        queues, session=session
    )

    logger.info(waiting_jobs, waiting_jids, nb_waiting_jobs)

    if nb_waiting_jobs > 0:
        #
        # Determine Global Resource Intervals and Initial Slot
        #
        resource_set = plt.resource_set(session=session, config=config)
        initial_slot_set = SlotSet((resource_set.roid_itvs, now))

        #
        #  Resource availabilty (Available_upto field) is integrated through pseudo job
        #
        pseudo_jobs = []
        for t_avail_upto in sorted(resource_set.available_upto.keys()):
            itvs = resource_set.available_upto[t_avail_upto]
            j = JobPseudo()
            # logger.info(t_avail_upto, MAX_TIME - t_avail_upto, itvs)
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
        job_security_time = int(config["SCHEDULER_JOB_SECURITY_TIME"])
        plt.get_data_jobs(
            session, waiting_jobs, waiting_jids, resource_set, job_security_time
        )

        #
        # Get already scheduled jobs advanced reservations and jobs from more higher priority queues
        #
        scheduled_jobs = plt.get_scheduled_jobs(
            session, resource_set, job_security_time, now
        )

        if scheduled_jobs != []:
            initial_slot_set.split_slots_jobs(scheduled_jobs)

        # initial_slot_set.show_slots()

        all_slot_sets = {"default": initial_slot_set}

        #
        # Scheduled
        #
        schedule_id_jobs_ct(
            all_slot_sets, waiting_jobs, resource_set.hierarchy, waiting_jids, 0
        )

        #
        # Save assignement
        #
        plt.save_assigns(session, waiting_jobs, resource_set)
    else:
        logger.info("no waiting jobs")


#
# Main function
#
def main(session=None, config=None):
    config, _, log, session_factory = init_oar()

    logger = get_logger("oar.kamelot_basic", forward_stderr=True)
    plt = Platform()

    schedule_cycle(session, config, plt)
    logger.info("That's all folks")


if __name__ == "__main__":  # pragma: no cover
    logger = get_logger("oar.kamelot_basic", forward_stderr=True)
    main()
