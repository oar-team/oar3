# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from logging import (getLogger, FileHandler as BaseFileHandler, Formatter,
                     INFO, ERROR, WARN, DEBUG, StreamHandler)

LEVELS = {0: ERROR, 1: WARN, 2: INFO, 3: DEBUG}


class DeferredFileHandler(BaseFileHandler):

    def __init__(self, *args, **kwargs):
        self.callback = kwargs.pop("callback", lambda: None)
        kwargs['delay'] = True
        BaseFileHandler.__init__(self, "/dev/null", *args, **kwargs)

    def _open(self):
        # We import settings here to avoid a circular reference as this module
        # will be imported when settings.py is executed.
        self.callback(self)
        return BaseFileHandler._open(self)


def create_logger():
    """Creates a new logger object."""
    logger = getLogger("oar")
    logger.setLevel(DEBUG)

    # just in case that was not a new logger, get rid of all the handlers
    # already attached to it.
    del logger.handlers[:]

    handler = DeferredFileHandler(callback=_configure)
    logger.addHandler(handler)
    return logger


def _configure(handler):
    from . import config
    getLogger("oar").setLevel(LEVELS[config['LOG_LEVEL']])
    # configure the handler
    if config['LOG_FILE']:
        handler.baseFilename = config['LOG_FILE']
    handler.setLevel(LEVELS[config['LOG_LEVEL']])
    handler.setFormatter(Formatter(config['LOG_FORMAT']))


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
