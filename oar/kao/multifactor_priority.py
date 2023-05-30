import yaml

from oar.kao.karma import evaluate_jobs_karma
from oar.lib.globals import get_logger, init_oar

config, db, log = init_oar(no_db=True)
logger = get_logger("oar.kao.priorty")


def evaluate_jobs_priority(session, config, queues, now, jids, jobs, plt):
    """
    Job's Priority = Sum of (criterion_weight * criterion_factor) where criteria are:
    age, queue, work, size, karma, qos, nice

    Criterion Factors:

    age_factor = max(1, age_coef * age)
    queue_factor = f(queue)
    work_factor = 1 / min(1, work) [smalls priortized],  1 - 1 / min(1, work) [bigs priortized]
    size_factor = 1 - (size / cluster_size) [smalls priortized],  (size / cluster_size) [bigs priortized]
    karma_factor = 1 / (1 + karma)
    qos_factor = 0.0 - 1.0 # must be fixed through admission rules
    nice_factor = max(nice, 1.0)

    Criterion Weights:

    Each criterion is positive integer

    Note: The priority approach is largely inspired by Slurm's one
    """

    age_weight = 0
    age_coef = 1.65e-06  # 7 days in seconds
    queue_weight = 0
    work_weight = 0
    work_mode = 0  # prioritize small jobs
    size_weight = 0
    size_mode = 0  # prioritize small jobs
    karma_weight = 0
    qos_weight = 0
    nice_weight = 0

    queue_coefs = {}

    with open(config["PRIORITY_CONF_FILE"], "r") as stream:
        try:
            yaml_priority = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.error(exc)

    if ("age_weight") in yaml_priority:
        age_weight = yaml_priority["age_weight"]
    if ("age_coef") in yaml_priority:
        age_coef = yaml_priority["age_coef"]
    if ("queue_weight") in yaml_priority:
        queue_weight = yaml_priority["queue_weight"]
    if ("queue_coefs") in yaml_priority:
        queue_coefs = yaml_priority["queue_coefs"]
    if ("work_weight") in yaml_priority:
        work_weight = yaml_priority["work_weight"]
    if ("work_mode") in yaml_priority:
        work_mode = yaml_priority["work_mode"]
    if ("size_weight") in yaml_priority:
        size_weight = yaml_priority["size_weight"]
    if ("size_mode") in yaml_priority:
        size_mode = yaml_priority["size_mode"]
    if ("karma_weight") in yaml_priority:
        karma_weight = yaml_priority["karma_weight"]
    if ("qos_weight") in yaml_priority:
        qos_weight = yaml_priority["qos_weight"]
    if ("nice_weight") in yaml_priority:
        nice_weight = yaml_priority["nice_weight"]

    # evalute and retrieve jobs' karma for fair-share
    evaluate_jobs_karma(session, config, queues, now, jids, jobs, plt)

    for job in jobs.values():
        job.priority = age_weight * max(1.0, age_coef * (now - job.submission_time))
        if queue_weight > 0.0:
            if job.queue_name in queue_coefs:
                job.priority += queue_weight * queue_coefs[job.queue_name]
            else:
                logger.warning(
                    "queue {} is define in queue_coefs but the queue_weight is.".format(
                        job.queue_name
                    )
                )
        if work_weight > 0.0:
            if work_mode:
                # prioritize big jobs over small ones (work = nb_resources * walltime)
                job.priority += work_weight * (1.0 - 1.0 / min(1.0, job.work))
            else:
                # prioritize small jobs over big ones  (work = nb_resources * walltime)
                job.priority += work_weight * 1.0 / min(1.0, job.work)
        if size_weight > 0.0:
            if size_mode:
                # prioritize big jobs over small ones
                job.priority += size_weight * (job.size / plt.nb_default_resources)
            else:
                # prioritize small jobs over big ones
                job.priority += size_weight * (
                    1.0 - (job.size / plt.nb_default_resources)
                )

        print("job id {} Karma {}".format(job.id, job.karma))
        job.priority += karma_weight * (1.0 / (1.0 + job.karma))
        if qos_weight > 0.0:
            job.priority += qos_weight * job.qos
        if nice_weight > 0.0:
            job.priority += nice_weight * max(1.0, job.nice)


def multifactor_jobs_sorting(session, config, queues, now, jids, jobs, plt):
    evaluate_jobs_priority(session, config, queues, now, jids, jobs, plt)

    ordered_jids = sorted(jids, key=lambda jid: jobs[jid].priority, reverse=True)
    # print("job priorty")
    # for job in jobs.values():
    #    print("job id: {} priority: {}".format(job.id, job.priority))
    # print(ordered_jids)
    return ordered_jids
