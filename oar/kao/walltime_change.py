from oar.lib.globals import init_oar
from oar.lib.job_handling import (
    change_walltime,
    get_job_suspended_sum_duration,
    get_job_types,
    get_jobs_with_walltime_change,
    get_possible_job_end_time_in_interval,
    get_running_job,
)
from oar.lib.logging import get_logger
from oar.lib.tools import duration_to_sql, duration_to_sql_signed
from oar.lib.walltime import get_conf, update_walltime_change_request

_, _, log = init_oar()

logger = get_logger(log, "oar.kao.walltime_change")


def process_walltime_change_requests(session, config, plt):
    now = plt.get_time()
    walltime_change_apply_time = config["WALLTIME_CHANGE_APPLY_TIME"]
    walltime_increment = config["WALLTIME_INCREMENT"]

    job_wtcs = get_jobs_with_walltime_change(session)

    for job_id, job in job_wtcs.items():
        suspended = get_job_suspended_sum_duration(session, job_id, now)
        fit = job.pending
        if fit > 0:
            apply_time = get_conf(
                walltime_change_apply_time, job.queue, job.walltime - job.granted, 0
            )
            increment = get_conf(
                walltime_increment, job.queue, job.walltime - job.granted, 0
            )

            logger.debug(
                "[{}] walltime change: pending={}s delay_next_jobs={} force={} apply_time={}s increment={}s".format(
                    job_id, fit, job.delay_next_jobs, job.force, apply_time, increment
                )
            )

            if (not job.force) or (job.force != "YES"):
                delay = job.start_time + job.walltime + suspended - apply_time - now
                if apply_time > 0 and delay > 0:
                    logger.debug(
                        "[job_id:{}] walltime change could apply in {}s".format(
                            job_id, delay
                        )
                    )
                    continue  # next if activation time is set and activation date in the future
                if increment > 0 and increment < fit:
                    fit = increment

            logger.debug(
                "[{}] walltime change try: {}/{}s".format(job_id, fit, job.pending)
            )
            from_ = job.start_time + job.walltime + suspended
            to = from_ + fit
            job_types = get_job_types(session, job_id)

            if "inner" in job_types:
                container_job = get_running_job(session, int(job_types["inner"]))
                if container_job:
                    if (
                        container_job.start_time + container_job.moldable_walltime
                    ) < to:
                        to = (
                            container_job.start_time + container_job.moldable_walltime
                        )  # container should never be suspended, makes no sense
                        logger.debug(
                            "[{}] walltime change for inner job limited to the container's boundaries: {}s".format(
                                job_id, to - from_
                            )
                        )

                else:
                    logger.debug(
                        "[{}] walltime change for inner job but container is not found ?".format(
                            job_id
                        )
                    )

            # TODO
            # if (exists($types->{deadline})) {
            #     my $deadline = OAR::IO::sql_to_local($types->{deadline});
            #     if ($deadline < $to) {
            #         $to = $deadline;
            #         oar_debug("[MetaSched] [$jobid] walltime change for job limited by its dealdine to ".($to - $from)."s\n");
            #     }
            # }

            fit = (
                get_possible_job_end_time_in_interval(
                    session,
                    from_,
                    to,
                    job.rids,
                    int(config["SCHEDULER_JOB_SECURITY_TIME"]),
                    job.delay_next_jobs,
                    job_types,
                    job.user,
                    job.name,
                )
                - from_
            )
            if fit <= 0:
                logger.debug(
                    "[{}] walltime cannot be changed for now (pending: {})".format(
                        job_id, duration_to_sql_signed(job.pending)
                    )
                )
                continue

        elif fit < 0:
            job_remaining_time = job.start_time + job.walltime + suspended - now
            if job_remaining_time < -fit:
                fit = -job_remaining_time

        new_walltime = job.walltime + fit
        new_pending = job.pending - fit

        message = "walltime changed: {} (granted: {}/pending: {}{}{})".format(
            duration_to_sql(new_walltime),
            duration_to_sql_signed(fit),
            duration_to_sql_signed(new_pending),
            "/force" if (job.force == "YES") else "",
            "/delay next jobs" if (job.delay_next_jobs == "YES") else "",
        )

        logger.debug("[{}] {}".format(job_id, message))

        update_walltime_change_request(
            session,
            job_id,
            new_pending,
            "NO" if (new_pending == 0) else None,
            "NO" if (new_pending == 0) else None,
            job.granted + fit,
            (job.granted_with_force + fit)
            if (job.force == "YES" and fit > 0)
            else None,
            (job.granted_with_delay_next_jobs + fit)
            if (job.delay_next_jobs == "YES" and fit > 0)
            else 0,
        )

        change_walltime(session, job_id, new_walltime, message)
