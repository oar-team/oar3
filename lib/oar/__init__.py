# -*- coding: utf-8 -*-
__version__ = '2.6.dev'


from .configuration import Configuration
from .logging import create_logger
from .database import Database

config = Configuration()
logger = create_logger()
db = Database()

from . import utils

from .models import (
    Accounting, AdmissionRule, Challenge, EventLog,
    EventLogHostname, File, FragJob, GanttJobsPrediction,
    GanttJobsPredictionsLog, GanttJobsPredictionsVisu, GanttJobsResource,
    GanttJobsResourcesLog, GanttJobsResourcesVisu, Job,
    JobResourceDescription, JobResourceGroup, JobStateLog, JobType,
    MoldableJob, Queue, Resource, ResourceLog, Scheduler
)

from .exceptions import (
    OARException, InvalidConfiguration, DatabaseError, DoesNotExist,
)
