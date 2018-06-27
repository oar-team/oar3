# -*- coding: utf-8 -*-

import os

from .configuration import Configuration
from .database import Database

db = Database()
config = Configuration()


if 'OARCONFFILE' in os.environ: # pragma: no cover
    config.load_file(os.environ['OARCONFFILE'])
else:
    config.load_default_config(silent=True)
