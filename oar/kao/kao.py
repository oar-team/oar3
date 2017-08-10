#!/usr/bin/env python
# coding: utf-8

from oar.lib import (config, get_logger)
from oar.kao.meta_sched import meta_schedule

DEFAULT_CONFIG = {
    'METASCHEDULER_MODE': 'internal'
}

config.setdefault_config(DEFAULT_CONFIG)

logger = get_logger("oar.kao")

def main():
    logger.info("Starting Kao Meta Scheduler")
    meta_schedule(config['METASCHEDULER_MODE'])

if __name__ == '__main__':  # pragma: no cover
    logger = get_logger("oar.kao", forward_stderr=True)
    main()
