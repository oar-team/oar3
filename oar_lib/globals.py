# -*- coding: utf-8 -*-

from .configuration import Configuration
from .logging import create_logger
from .database import Database
from .query import BaseQuery

config = Configuration()
logger = create_logger()
db = Database()
db.query_class = BaseQuery
