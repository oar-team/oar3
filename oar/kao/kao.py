#!/usr/bin/env python
# coding: utf-8

from oar.kao.meta_sched import meta_schedule
from oar.lib.globals import init_oar
from oar.lib.logging import get_logger

config, _, log = init_oar()

logger = get_logger(log, "oar.kao")


def main(session, config):
    logger.info("Starting Kao Meta Scheduler")
    return meta_schedule(session, config, config["METASCHEDULER_MODE"])


if __name__ == "__main__":  # pragma: no cover
    main()
