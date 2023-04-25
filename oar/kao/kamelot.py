import sys

from oar.kao.karma import karma_jobs_sorting
from oar.kao.multifactor_priority import multifactor_jobs_sorting
from oar.kao.platform import Platform
from oar.kao.quotas import Quotas
from oar.kao.scheduling import schedule_id_jobs_ct, set_slots_with_prev_scheduled_jobs
from oar.kao.slot import MAX_TIME, SlotSet
from oar.lib.globals import init_oar
from oar.lib.job_handling import NO_PLACEHOLDER, JobPseudo
from oar.lib.logging import get_logger
from oar.lib.plugins import find_plugin_function

# Constant duration time of a besteffort job *)
besteffort_duration = 300  # TODO conf ???

_, _, log = init_oar()
logger = get_logger(log, "oar.kamelot")


def jobs_sorting(session, config, queues, now, waiting_jids, waiting_jobs, plt):
    waiting_ordered_jids = waiting_jids

    if "JOB_PRIORITY" in config:
        if config["JOB_PRIORITY"] == "FAIRSHARE":
            #
            # Karma job sorting (Fairsharing)
            #
            waiting_ordered_jids = karma_jobs_sorting(
                session, config, queues, now, waiting_jids, waiting_jobs, plt
            )
        elif config["JOB_PRIORITY"] == "MULTIFACTOR":
            waiting_ordered_jids = multifactor_jobs_sorting(session, config,
                queues, now, waiting_jids, waiting_jobs, plt
            )

        elif config["JOB_PRIORITY"] == "CUSTOM":
            custom_jobs_sorting_func = find_plugin_function(
                "oar.jobs_sorting_func",
                config["CUSTOM_JOB_SORTING"],
            )
            if "CUSTOM_JOB_SORTING_CONFIG" not in config:
                config["CUSTOM_JOB_SORTING_CONFIG"] = "{}"

            waiting_ordered_jids = custom_jobs_sorting_func(
                queues,
                now,
                waiting_jids,
                waiting_jobs,
                config["CUSTOM_JOB_SORTING_CONFIG"],
                plt,
            )

    return waiting_ordered_jids


def internal_schedule_cycle(
    session, config, plt, now, all_slot_sets, job_security_time, queues
):
    resource_set = plt.resource_set(session, config)

    #
    # Retrieve waiting jobs
    #
    waiting_jobs, waiting_jids, nb_waiting_jobs = plt.get_waiting_jobs(
        queues, session=session
    )

    if nb_waiting_jobs > 0:
        logger.info("nb_waiting_jobs:" + str(nb_waiting_jobs))
        for jid in waiting_jids:
            logger.debug("waiting_jid: " + str(jid))

        #
        # Get  additional waiting jobs' data
        #
        plt.get_data_jobs(
            session, waiting_jobs, waiting_jids, resource_set, job_security_time
        )

        waiting_ordered_jids = jobs_sorting(
            session, config, queues, now, waiting_jids, waiting_jobs, plt
        )

        #
        # Scheduled
        #
        schedule_id_jobs_ct(
            all_slot_sets,
            waiting_jobs,
            resource_set.hierarchy,
            waiting_ordered_jids,
            job_security_time,
        )

        #
        # Save assignement
        #
        logger.info("save assignement")

        plt.save_assigns(session, waiting_jobs, resource_set)
    else:
        logger.info("no waiting jobs")


def schedule_cycle(session, config, plt, now, queues=["default"]):
    logger.info(
        "Begin scheduling....now: {}, queue(s): {}".format(
            now, " ".join([q for q in queues])
        )
    )
    #
    # Retrieve waiting jobs
    #
    waiting_jobs, waiting_jids, nb_waiting_jobs = plt.get_waiting_jobs(
        queues, session=session
    )

    if nb_waiting_jobs > 0:
        logger.info("nb_waiting_jobs:" + str(nb_waiting_jobs))
        for jid in waiting_jids:
            logger.debug("waiting_jid: " + str(jid))

        job_security_time = int(config["SCHEDULER_JOB_SECURITY_TIME"])

        #
        # Determine Global Resource Intervals and Initial Slot
        #
        resource_set = plt.resource_set(session, config)
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
            session, waiting_jobs, waiting_jids, resource_set, job_security_time
        )

        # Job sorting (karma and advanced)
        waiting_ordered_jids = jobs_sorting(
            session, config, queues, now, waiting_jids, waiting_jobs, plt
        )

        #
        # Get already scheduled jobs advanced reservations and jobs from more higher priority queues
        #
        scheduled_jobs = plt.get_scheduled_jobs(
            session, resource_set, job_security_time, now
        )

        all_slot_sets = {"default": initial_slot_set}

        if scheduled_jobs != []:
            if (len(queues) == 1) and (queues[0] == "besteffort"):
                filter_besteffort = False
            else:
                filter_besteffort = True
            set_slots_with_prev_scheduled_jobs(
                all_slot_sets, scheduled_jobs, job_security_time, now, filter_besteffort
            )
        #
        # Scheduled
        #
        schedule_id_jobs_ct(
            all_slot_sets,
            waiting_jobs,
            resource_set.hierarchy,
            waiting_ordered_jids,
            job_security_time,
        )

        #
        # Save assignement
        #
        logger.info("save assignement")

        plt.save_assigns(session, waiting_jobs, resource_set)
    else:
        logger.info("no waiting jobs")


#
# Main function
#
def main(session=None):
    config, _, log = init_oar()
    logger = get_logger(log, "oar.kamelot", forward_stderr=True)

    plt = Platform()

    if ("QUOTAS" in config) and (config["QUOTAS"] == "yes"):
        Quotas.enable(plt.resource_set())

    logger.debug("argv..." + str(sys.argv))

    if len(sys.argv) > 2:
        schedule_cycle(session, config, plt, int(float(sys.argv[2])), [sys.argv[1]])
    elif len(sys.argv) == 2:
        schedule_cycle(session, config, plt, plt.get_time(), [sys.argv[1]])
    else:
        schedule_cycle(session, config, plt, plt.get_time())

    logger.info("That's all folks")

    session.commit()


if __name__ == "__main__":  # pragma: no cover
    main()
