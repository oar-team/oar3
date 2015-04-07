#!/usr/bin/env python 
from oar.lib import get_logger
from logging import getLogger
from oar.kao.meta_sched import meta_schedule

log = get_logger("oar.kao")

sa_logger = getLogger("sqlalchemy.engine")
sa_logger.setLevel(0)

#import click

def kao():

    log.info("Starting Kao Meta Scheduler")
    meta_schedule()

if __name__ == '__main__':
    kao()

