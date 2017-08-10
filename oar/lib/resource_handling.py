# coding: utf-8
""" Functions to handle resource"""
import os

from oar.lib import (db, Resource, ResourceLog, Job, AssignedResource,
                     EventLog, FragJob, get_logger)
import oar.lib.tools as tools


State_to_num = {'Alive': 1, 'Absent': 2, 'Suspected': 3, 'Dead': 4}

logger = get_logger('oar.lib.resource_handling')

def set_resource_state(resource_id, state, finaud_decision):
    """sets the state field of a resource"""
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


