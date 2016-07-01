# coding: utf-8
"""Extra metascheduling functions which can be called between each queue handling
"""
from __future__ import unicode_literals, print_function

from oar.lib import (get_logger, config)

import json

logger = get_logger("oar.extra_metasched")



def extra_metasched_foo(prev_queue, plt, scheduled_jobs, all_slot_sets,
                        job_security_time, queue, initial_time_sec,
                        extra_metasched_config):
    pass

