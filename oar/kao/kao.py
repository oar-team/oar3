#!/usr/bin/env python
# coding: utf-8

from oar.kao.meta_sched import meta_schedule
from oar.lib import config, get_logger

logger = get_logger("oar.kao")


def main():
    logger.info("Starting Kao Meta Scheduler")
    return meta_schedule(config["METASCHEDULER_MODE"])


if __name__ == "__main__":  # pragma: no cover
    logger = get_logger("oar.kao", forward_stderr=True)
    main()
