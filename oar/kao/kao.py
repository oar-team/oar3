#!/usr/bin/env python
# coding: utf-8

import os
import sys

from oar.kao.meta_sched import meta_schedule
from oar.lib import config, get_logger

logger = get_logger("oar.kao")


def main():
    logger.info("Starting [{}]".format(os.path.basename(sys.argv[0])))
    return_code = meta_schedule(config["METASCHEDULER_MODE"])
    logger.info("Returning from [{}]".format(os.path.basename(sys.argv[0])))
    return return_code


if __name__ == "__main__":  # pragma: no cover
    main()
