# coding: utf-8
"""Collection of Job Sorting functions to provide priority policies
"""
from oar.lib import (get_logger, config)

import json

logger = get_logger("oar.kamelot")

def job_sorting_simple_priority(queue, now, jids, jobs, str_config, plt):
    priority_config = json.loads(str_config)

    # import pdb; pdb.set_trace()
    if 'WAITING_TIME_WEIGHT' in config:
        waiting_time_weight = float(priority_config['WAITING_TIME_WEIGHT'])
    else:
        waiting_time_weight = 0.0


    #
    # establish  job priori 
    #

    for job in jobs.values():
        if 'priority' in job.types:
            try:
                priority = float(job.types['priority'])
            except ValueError:
                logger.warning("job priority failed to convert to float: " % job.types['priority'])
                priority = 0.0

        job.karma = priority + waiting_time_weight * float(now-job.submission_time) / float(now)


    # sort jids according to jobs' karma value
    # print jids
    karma_ordered_jids = sorted(jids, key=lambda jid: jobs[jid].karma, reverse=True)
    # print karma_ordered_jids
    return karma_ordered_jids
