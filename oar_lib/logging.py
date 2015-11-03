# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import sys

from logging import (getLogger, NullHandler, FileHandler, StreamHandler,
                     Formatter, INFO, ERROR, WARN, DEBUG)

LEVELS = {0: ERROR, 1: WARN, 2: INFO, 3: DEBUG}


class DeferredHandler(NullHandler):

    def __init__(self, logger):
        self.logger = logger
        super(NullHandler, self).__init__()

    def emit(self, record):
        from . import config

        self.logger.setLevel(LEVELS[config['LOG_LEVEL']])
        # configure the handler
        log_file = config.get('LOG_FILE', None)
        if log_file is not None:
            if log_file == ":stdout:":
                handler = StreamHandler(sys.stdout)
            if log_file == ":stderr:":
                handler = StreamHandler(sys.stderr)
            else:
                handler = FileHandler(log_file)
            handler.setLevel(LEVELS[config['LOG_LEVEL']])
            handler.setFormatter(Formatter(config['LOG_FORMAT']))

            self.logger.handlers = []
            self.logger.addHandler(handler)
            handler.emit(record)


def create_logger():
    """Creates a new logger object."""
    logger = getLogger("oar")
    logger.setLevel(DEBUG)

    # just in case that was not a new logger, get rid of all the handlers
    # already attached to it.
    del logger.handlers[:]

    logger.addHandler(DeferredHandler(logger))
    return logger


def get_logger(*args, **kwargs):
    """ Returns logger with attached StreamHandler if `stdout` is True."""
    from . import config
    kwargs.setdefault('stdout', False)
    if kwargs.pop('stdout'):
        logger = getLogger(*args, **kwargs)
        handler = StreamHandler()
        handler.setLevel(LEVELS[config['LOG_LEVEL']])
        handler.setFormatter(Formatter(config['LOG_FORMAT']))
        logger.addHandler(handler)
        return logger
    else:
        return getLogger(*args, **kwargs)
