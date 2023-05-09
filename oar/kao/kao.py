#!/usr/bin/env python
# coding: utf-8

from oar.kao.meta_sched import meta_schedule
from sqlalchemy.orm import scoped_session, sessionmaker
from oar.lib.globals import get_logger, init_oar


def main(session, config):
    return meta_schedule(session, config, config["METASCHEDULER_MODE"])


if __name__ == "__main__":  # pragma: no cover
    config, engine, log = init_oar()
    logger = get_logger("oar.kao")

    session_factory = sessionmaker(bind=engine)
    # Legacy call
    scoped = scoped_session(session_factory)

    # Create a session  
    session = scoped()

    main(session, config)
