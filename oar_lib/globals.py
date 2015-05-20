# -*- coding: utf-8 -*-

from .configuration import Configuration
from .logging import create_logger
from .database import Database

logger = create_logger()
db = Database()
config = Configuration()

config.load_default_config(silent=True)
