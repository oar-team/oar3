# -*- coding: utf-8 -*-
import sys
from logging import (
    DEBUG,
    ERROR,
    INFO,
    WARN,
    FileHandler,
    Formatter,
    NullHandler,
    StreamHandler,
    getLogger,
)

from .utils import touch

LEVELS = {0: ERROR, 1: WARN, 2: INFO, 3: DEBUG}


STREAM_HANDLER = {"stdout": None, "stderr": None}


def create_logger(config):
    """Creates a new logger object."""
    # from . import config

    logger = getLogger("oar")
    del logger.handlers[:]

    # TODO: duck tape
    logger.config = config

    logger.setLevel(LEVELS[config["LOG_LEVEL"]])

    log_file = config.get("LOG_FILE", None)
    if log_file is not None:
        if log_file == ":stdout:":  # pragma: no cover
            handler = get_global_stream_handler(config, "stdout")
        if log_file == ":stderr:":
            handler = get_global_stream_handler(config, "stderr")
        else:  # pragma: no cover
            touch(log_file)
            handler = FileHandler(log_file)
            handler.setFormatter(Formatter(config["LOG_FORMAT"]))

        if handler not in logger.handlers:
            logger.addHandler(handler)
    else:  # pragma: no cover
        logger.addHandler(NullHandler())

    logger.propagate = False

    return logger


def get_global_stream_handler(config, output="stderr"):
    # from . import config

    global STREAM_HANDLER
    if STREAM_HANDLER[output] is None:
        STREAM_HANDLER[output] = StreamHandler(getattr(sys, output, "stderr"))
        print("haha", config["LOG_LEVEL"])
        STREAM_HANDLER[output].setLevel(LEVELS[config["LOG_LEVEL"]])
        STREAM_HANDLER[output].setFormatter(Formatter(config["LOG_FORMAT"]))
    return STREAM_HANDLER[output]
