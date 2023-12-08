# -*- coding: utf-8 -*-

import os
from logging import Logger, getLogger
from typing import Optional, Tuple

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from oar.lib.models import Model

from .configuration import Configuration
from .database import Database, EngineConnector, reflect_base
from .logging import create_logger, get_global_stream_handler
from .models import DeferredReflectionModel, setup_db


def init_config() -> Configuration:
    config = Configuration()

    if "OARCONFFILE" in os.environ:  # pragma: no cover
        config.load_file(os.environ["OARCONFFILE"])
    else:
        config.load_default_config(silent=True)

    return config


def init_logger(config: Optional[Configuration] = None) -> Logger:
    if not config:
        config = init_config()

    return create_logger(config)


def get_logger(*args, config: Optional[Configuration] = None, **kwargs) -> Logger:
    """Returns sub logger once the root logger is configured."""

    logger = init_logger(config)

    global STREAM_HANDLER
    forward_stderr = kwargs.pop("forward_stderr", True)
    # Make sure that the root logger is configured
    sublogger = getLogger(*args, **kwargs)
    sublogger.propage = True
    if forward_stderr:
        stream_handler = get_global_stream_handler(logger.config, "stderr")
        if stream_handler not in logger.handlers:  # pragma: no cover
            sublogger.addHandler(stream_handler)
    return sublogger


def init_db(config: Optional[Configuration], no_reflect: bool = False) -> Engine:
    db = Database(config)

    engine = EngineConnector(db).get_engine()

    setup_db(db, engine)

    if not no_reflect:
        DeferredReflectionModel.prepare(engine)
        reflect_base(Model.metadata, DeferredReflectionModel, engine)

    return engine


def init_oar(
    config: Optional[Configuration] = None,
    no_db: bool = False,
    no_reflect: bool = False,
) -> Tuple[Configuration, Optional[Engine], Logger]:
    if not config:
        config = init_config()

    if no_db:
        return config, None
    else:
        engine = init_db(config, no_reflect=no_reflect)
        return config, engine


def init_and_get_session(config: Optional[Configuration] = None) -> Session:
    if not config:
        config = init_config()

    engine = init_db(config)

    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)
    return scoped()
