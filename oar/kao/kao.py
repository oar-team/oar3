#!/usr/bin/env python
# coding: utf-8
from __future__ import unicode_literals, print_function

from oar.lib import get_logger
from oar.kao.meta_sched import meta_schedule

logger = get_logger("oar.kao")


def main():
    logger.info("Starting Kao Meta Scheduler")
    meta_schedule()

if __name__ == '__main__':  # pragma: no cover
    logger = get_logger("oar.kao", forward_stderr=True)
    main()
