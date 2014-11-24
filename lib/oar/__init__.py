# -*- coding: utf-8 -*-
__version__ = '2.6.dev'


from .configuration import Configuration
from .database import Database

config = Configuration()
db = Database()

from . import utils

from .models import (
    Accounting, AdmissionRule, AssignedResource, Challenge, EventLog,
    EventLogHostname, File, FragJob, GanttJobsPrediction,
    GanttJobsPredictionsLog, GanttJobsPredictionsVisu, GanttJobsResource,
    GanttJobsResourcesLog, GanttJobsResourcesVisu, Job, JobDependency,
    JobResourceDescription, JobResourceGroup, JobStateLog, JobType,
    MoldableJobDescription, Queue, Resource, ResourceLog, Scheduler
)

from .exceptions import (
    OARException, InvalidConfiguration, DatabaseError, DoesNotExist,
)
