# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import sys

from logging import (getLogger, NullHandler, FileHandler, StreamHandler,
                     Formatter, INFO, ERROR, WARN, DEBUG)

LEVELS = {0: ERROR, 1: WARN, 2: INFO, 3: DEBUG}


STREAM_HANDLER = {"stdout": None, "stderr": None}


def create_logger():
    """Creates a new logger object."""
    from . import config

    logger = getLogger("oar")
    del logger.handlers[:]

    logger.setLevel(LEVELS[config['LOG_LEVEL']])

    log_file = config.get('LOG_FILE', None)
    if log_file is not None:
        if log_file == ":stdout:":
            handler = get_global_stream_handler("stdout")
        if log_file == ":stderr:":
            handler = get_global_stream_handler("stderr")
        else:
            handler = FileHandler(log_file)

        if handler in logger.handlers:
            logger.addHandler(handler)
    else:
        logger.addHandler(NullHandler())

    logger.propagate = False

    return logger


def get_logger(*args, **kwargs):
    """ Returns sub logger once the root logger is configured."""
    global STREAM_HANDLER
    forward_stderr = kwargs.pop('forward_stderr', False)
    # Make sure that the root logger is configured
    sublogger = getLogger(*args, **kwargs)
    sublogger.propage = False
    if forward_stderr:
        stream_handler = get_global_stream_handler("stderr")
        if stream_handler not in logger.handlers:
            sublogger.addHandler(stream_handler)
    return sublogger


def get_global_stream_handler(output="stderr"):
    from . import config
    global STREAM_HANDLER
    if STREAM_HANDLER[output] is None:
        STREAM_HANDLER[output] = StreamHandler(getattr(sys, output, "stderr"))
        STREAM_HANDLER[output].setLevel(LEVELS[config['LOG_LEVEL']])
        STREAM_HANDLER[output].setFormatter(Formatter(config['LOG_FORMAT']))
    return STREAM_HANDLER[output]


logger = create_logger()
