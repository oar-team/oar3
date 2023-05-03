#!/usr/bin/env python
# coding: utf-8

from oar.kao.meta_sched import meta_schedule
from oar.lib.globals import get_logger, init_oar

config, _, log, session_factory = init_oar()

logger = get_logger("oar.kao")


def main(session, config):
    logger.info("Starting Kao Meta Scheduler")
    return meta_schedule(session, config, config["METASCHEDULER_MODE"])


if __name__ == "__main__":  # pragma: no cover
    main()
