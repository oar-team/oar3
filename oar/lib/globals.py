# -*- coding: utf-8 -*-

import os

from .configuration import Configuration
from .database import Database
from .models import setup_db
from .logging import create_logger


def init_oar():
    config = Configuration()
    db = Database(config)

    setup_db(db)

    if "OARCONFFILE" in os.environ:  # pragma: no cover
        config.load_file(os.environ["OARCONFFILE"])
    else:
        config.load_default_config(silent=True)

    logger = create_logger(config)

    return config, db, logger
