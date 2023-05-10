#!/usr/bin/env python
# coding: utf-8

from sqlalchemy.orm import scoped_session, sessionmaker

from oar.kao.meta_sched import meta_schedule
from oar.lib.globals import init_oar


def main():
    config, engine, log = init_oar()

    session_factory = sessionmaker(bind=engine)
    # Legacy call
    scoped = scoped_session(session_factory)

    # Create a session
    session = scoped()

    return meta_schedule(session, config, config["METASCHEDULER_MODE"])


if __name__ == "__main__":  # pragma: no cover
    main()
