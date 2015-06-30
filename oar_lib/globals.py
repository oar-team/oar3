# -*- coding: utf-8 -*-

import os

from .configuration import Configuration
from .logging import create_logger
from .database import Database

logger = create_logger()
db = Database()
config = Configuration()


if 'OARCONFFILE' in os.environ:
    config.load_file(os.environ['OARCONFILE'])
else:
    config.load_default_config(silent=True)
