# coding: utf-8
""" Functions to handle resource"""
import os

from sqlalchemy import distinct
from oar.lib import (db, config, Resource, ResourceLog, Job, AssignedResource,
                     EventLog, FragJob, get_logger)
from oar.lib.event import (add_new_event, is_an_event_exists)

import oar.lib.tools as tools


State_to_num = {'Alive': 1, 'Absent': 2, 'Suspected': 3, 'Dead': 4}

logger = get_logger('oar.lib.resource_handling')

def get_resource(resource_id):
    return db.query(Resource).filter(Resource.id == resource_id).one()


def set_resource_state(resource_id, state, finaud_decision):
    """set the state field of a resource"""
    db.query(Resource).filter(Resource.id == resource_id)\
                      .update({Resource.state: state,
                               Resource.finaud_decision: finaud_decision,
                               Resource.state_num: State_to_num[state]})

    date = tools.get_date()

    db.query(ResourceLog).filter(ResourceLog.date_stop == 0)\
                         .filter(ResourceLog.attribute == 'state')\
                         .filter(ResourceLog.resource_id == resource_id)\
                         .update({ResourceLog.date_stop: date})

    ins = ResourceLog.__table__.insert().values(
        {'resource_id': resource_id, 'attribute': 'state', 'value': state,
         'date_start': date, 'finaud_decision': finaud_decision})
    db.session.execute(ins)

def set_resource_nextState(resource_id, next_state):
    """Set the nextState field of a resource identified by its resource_id"""
    db.query(Resource).filter(Resource.id == resource_id)\
                      .update({Resource.next_state: next_state, Resource.next_finaud_decision: 'NO'})
    db.commit()

def remove_resource(resource_id, user=None):
    """Remove resource"""

    if not user: 
        if 'OARDO_USER' in os.environ:
            user = os.environ['OARDO_USER']
        else:
            user = os.environ['USER']

    if (user != 'oar') and (user != 'root'):
        return (4, 'Only the oar or root users can delete resources')
    
    # get resources
    res = db.query(Resource.state).filter(Resource.id == resource_id).one()
    state = res[0]
    if state == 'Dead':
        results = db.query(Job.id, Job.assigned_moldable_job)\
                    .filter(AssignedResource.resource_id == resource_id)\
                    .filter(AssignedResource.moldable_id == Job.assigned_moldable_job)\
                    .all()
        for job_mod_id in results:
            job_id, moldable_id = job_mod_id
            db.query(EventLog).where(EventLog.job_id == job_id).delete()
            db.query(FragJob).where(FragJob.job_id == job_id).delete()
            db.query(Job).where(Job.id == job_id).delete()
            db.query(AssignedResource)\
              .filter(AssignedResource.moldable_id == moldable_id).delete()

        db.query(AssignedResource).filter(AssignedResource.resource_id == resource_id).delete()
        db.query(ResourceLog).filter(ResourceLog.resource_id == resource_id).delete()
        db.query(Resource).filter(Resource.id == resource_id).delete()

        db.session.expire_all() #???
        return(0, None)
    else:
        return(3, 'Resource must be in DEAD state.')


def get_current_resources_with_suspended_job():
    """ Return the list of resources where there are Suspended jobs"""
    res = db.query(AssignedResource.resource_id).filter(AssignedResource.index == 'CURRENT')\
                                                .filter(Job.state == 'Suspended')\
                                                .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)\
                                                .all()
    # TODO REMOVE
    #return tuple(r for r in res)
    return res



def get_current_assigned_job_resources(moldable_id):
    """ Returns the current resources ref for a job"""
    res = db.query(Resource).filter(AssignedResource.index == 'CURRENT')\
                      .filter(AssignedResource.moldable_job_id == moldable_id)\
                      .filter(Resource.id == AssignedResource.resource_id)\
                      .all()
    return res
    
def  get_resources_change_state():
    """Get resource ids that will change their state"""
    res = db.query(Resource.id, Resource.next_state).filter(Resource.next_state != 'UnChanged').all()
    return {r.id: r.next_state for r in res}

                
def  get_expired_resources():
    """Get the list of resources whose expiry_date is in the past and which are not dead yet.
    0000-00-00 00:00:00 is always considered as in the future. Used for desktop computing schema."""
    # TODO: UNUSED (Desktop computing)
    date = tools.get_date()

    res = db.query(Resource.id).filter(Resource.state == 'Alive')\
                               .filter(Resource.expiry_date > 0)\
                               .filter(Resource.desktop_computing == 'YES')\
                               .filter(Resource.expiry_date < date)\
                               .all()
    return res

def get_absent_suspected_resources_for_a_timeout(timeout):
    date = tools.get_date()
    res = db.query(ResourceLog.resource_id).filter(ResourceLog.attribute == 'state')\
                                           .filter(ResourceLog.date_stop == 0)\
                                           .filter((ResourceLog.date_start + timeout) <  date)\
                                           .all()
    return res

def update_resource_nextFinaudDecision(resource_id, finaud_decision):
    """Update nextFinaudDecision field"""

    db.query(Resource).filter(Resource.id == resource_id)\
                      .update({Resource.next_finaud_decision: finaud_decision})
    db.commit()


def get_resource_job_to_frag():
    pass


def update_scheduler_last_job_date(date, moldable_id):
    db.query(Resource).filter(AssignedResource.Moldable_job_id == moldable_id)\
                      .filter(AssignedResource.Resource_id == Resource.resource_id)\
                      .update({Resource.last_job_date: date})

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
            
        if ((('besteffort' in job_types.keys()) or ('timesharing' in job_types.keys())) and
           (((state == 'START') and
             is_an_event_exists(job.id, "SCHEDULER_PRIORITY_UPDATED_START") <= 0) or
           ((state == 'STOP') and is_an_event_exists(job.id, "SCHEDULER_PRIORITY_UPDATED_START") > 0))):

            coeff = 1
            if ('besteffort' in job_types.keys()) and ('timesharing' not in job_types.keys()):
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
