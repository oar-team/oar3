#!/usr/bin/env python
from oar.lib import get_logger
from oar.kao.meta_sched import meta_schedule

log = get_logger("oar.kao")


def kao():

    log.info("Starting Kao Meta Scheduler")
    meta_schedule()

if __name__ == '__main__':
    kao()
