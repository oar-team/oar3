#!/usr/bin/env python
# coding: utf-8

from oar.kao.meta_sched import meta_schedule
from oar.lib.globals import init_oar
from oar.lib.logging import get_logger

config, _, log = init_oar()

logger = get_logger(log, "oar.kao")


def main():
    logger.info("Starting Kao Meta Scheduler")
    return meta_schedule(config["METASCHEDULER_MODE"])


if __name__ == "__main__":  # pragma: no cover
    logger = get_logger("oar.kao", forward_stderr=True)
    main()
