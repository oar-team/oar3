# -*- coding: utf-8 -*-

import os
from logging import getLogger

from sqlalchemy.engine import Engine

from oar.lib.models import Model

from .configuration import Configuration
from .database import Database, EngineConnector, reflect_base
from .logging import create_logger, get_global_stream_handler
from .models import DeferredReflectionModel, setup_db


def init_config():
    config = Configuration()

    if "OARCONFFILE" in os.environ:  # pragma: no cover
        config.load_file(os.environ["OARCONFFILE"])
    else:
        config.load_default_config(silent=True)

    return config


def init_logger(config=None):
    if not config:
        config = init_config()

    return create_logger(config)


def get_logger(*args, config=None, **kwargs):
    """Returns sub logger once the root logger is configured."""

    logger = init_logger(config)

    global STREAM_HANDLER
    forward_stderr = kwargs.pop("forward_stderr", False)
    # Make sure that the root logger is configured
    sublogger = getLogger(*args, **kwargs)
    sublogger.propage = False
    if forward_stderr:
        stream_handler = get_global_stream_handler(logger.config, "stderr")
        if stream_handler not in logger.handlers:  # pragma: no cover
            sublogger.addHandler(stream_handler)
    return sublogger


def init_db(config, no_reflect=False) -> Engine:
    db = Database(config)

    engine = EngineConnector(db).get_engine()

    setup_db(db, engine)

    if not no_reflect:
        reflect_base(Model.metadata, DeferredReflectionModel, engine)

    return engine


def init_oar(config=None, no_db=False, no_reflect=False):
    if not config:
        config = init_config()

    logger = create_logger(config)
    if no_db:
        return config, None, logger
    else:
        engine = init_db(config, no_reflect=no_reflect)
        return config, engine, logger
