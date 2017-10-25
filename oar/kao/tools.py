# coding: utf-8

from sqlalchemy import distinct
from oar.lib import (db, config, get_logger, AssignedResource, Resource)
from oar.lib.event import (add_new_event, is_an_event_exists)
#from oar.lib.job_handling import get_job_types

logger = get_logger("oar.kao.tools")

# update_current_scheduler_priority
# Update the scheduler_priority field of the table resources
def update_current_scheduler_priority(job, value, state):
    """Update the scheduler_priority field of the table resources
    """
    # TODO: need to adress this s
    from oar.lib.job_handling import get_job_types
    # TO FINISH
    # TODO: MOVE TO resource.py ???

    logger.info("update_current_scheduler_priority " +
                " job.id: " + str(job.id) + ", state: " + state + ", value: "
                + str(value))

    if "SCHEDULER_PRIORITY_HIERARCHY_ORDER" in config:
        sched_priority = config["SCHEDULER_PRIORITY_HIERARCHY_ORDER"]
        
        try:
            job_types = job.types
        except AttributeError:
            job_types = get_job_types(job.id)
            
        if ((('besteffort' in job_types) or ('timesharing' in job_types)) and
           (((state == 'START') and
             is_an_event_exists(job.id, "SCHEDULER_PRIORITY_UPDATED_START") <= 0) or
           ((state == 'STOP') and is_an_event_exists(job.id, "SCHEDULER_PRIORITY_UPDATED_START") > 0))):

            coeff = 1
            if ('besteffort' in job_types) and ('timesharing' not in job_types):
                coeff = 10

            index = 0
            for f in sched_priority.split('/'):
                if f == '':
                    continue
                elif f == 'resource_id':
                    f = 'id'

                index += 1

                res = db.query(distinct(getattr(Resource, f)))\
                        .filter(AssignedResource.index == 'CURRENT')\
                        .filter(AssignedResource.moldable_id == job.assigned_moldable_job)\
                        .filter(AssignedResource.resource_id == Resource.id)\
                        .all()

                resources = tuple(r[0] for r in res)

                if resources == ():
                    return

                incr_priority = int(value) * index * coeff
                db.query(Resource)\
                  .filter((getattr(Resource, f)).in_(resources))\
                  .update({Resource.scheduler_priority: incr_priority}, synchronize_session=False)

            add_new_event('SCHEDULER_PRIORITY_UPDATED_' + state, job.id,
                          'Scheduler priority for job ' + str(job.id) +
                          'updated (' + sched_priority + ')')


def update_scheduler_last_job_date(date, moldable_id):
    db.query(Resource).filter(AssignedResource.Moldable_job_id == moldable_id)\
                      .filter(AssignedResource.Resource_id == Resource.resource_id)\
                      .update({Resource.last_job_date: date})
