# coding: utf-8
""" Functions to handle job"""
# TODO move some functions from oar/kao/job.py
import os
import re
from sqlalchemy import (text, distinct)
from oar.lib import (db, Job, MoldableJobDescription, JobResourceDescription,
                     JobResourceGroup, Resource, GanttJobsPrediction,
                     JobDependencie, GanttJobsResource, JobType,
                     JobStateLog, AssignedResource, FragJob,
                     Resource, get_logger, config)

from oar.lib.event import add_new_event

import oar.lib.tools as tools

logger = get_logger('oar.lib.job_handling')

def get_array_job_ids(array_id):
    """ Get all the job_ids of a given array of job identified by its id"""
    results = db.query(Job.id).filter(Job.array_id == array_id)\
                .order_by(Job.id).all()
    job_ids = [r[0] for r in results]
    return job_ids

def get_job_ids_with_given_properties(sql_property):
    """Returns the job_ids with specified properties parameters : base, where SQL constraints."""
    results = db.query(Job.id).filter(text(sql_property))\
                .order_by(Job.id).all()
    return results

def get_job(job_id):  # pragma: no cover
    try:
        job = db.query(Job).filter(Job.id == job_id).one()
    except Exception as e:
        logger.warning("get_job(" + str(job_id) + ") raises execption: " + str(e))
        return None
    else:
        return job


def frag_job(job_id, user=None):
    """Sets the flag 'ToFrag' of a job to 'Yes' which will threshold job deletion"""
    if not user: 
        if 'OARDO_USER' in os.environ:
            user = os.environ['OARDO_USER']
        else:
            user = os.environ['USER']

    job = get_job(job_id)

    if not job:
        return -3

    if (user == job.user) or (user == 'oar') or (user == 'root'):
        res = db.query(FragJob).filter(FragJob.job_id == job_id).all()

        if len(res) == 0:

            date = tools.get_date()
            frajob = FragJob(job_id=job_id, date=date)
            db.add(frajob)
            db.commit()
            add_new_event("FRAG_JOB_REQUEST",
                          job_id, "User %s requested to frag the job %s"
                          % (user, str(job_id)))
            return 0
        else:
            # Job already killed
            return -2
    else:
        return -1

def ask_checkpoint_signal_job(job_id, signal=None, user=None):
    """Verify if the user is able to checkpoint the job
    returns : 0 if all is good, 1 if the user cannot do this,
    2 if the job is not running,
    3 if the job is Interactive"""

    if not user: 
        if 'OARDO_USER' in os.environ:
            user = os.environ['OARDO_USER']
        else:
            user = os.environ['USER']

    job = get_job(job_id)

    error_msg = 'Cannot checkpoint '
    if signal:
        error_msg = 'Cannot signal '
    error_msg += '{} ; '.format(job_id) 
    

    if job and (job.type == 'INTERACTIVE'):
        return (3, error_msg + 'The job is Interactive.')

    if job and ((user == job.user) or (user == 'oar') or (user == 'root')):
        if job.state == 'Running':
            if signal:
                add_new_event('CHECKPOINT', job_id,
                              'User {} requested a checkpoint on the job {}'.format(user, job_id))
            else:
                add_new_event('SIGNAL_{}'.format(signal), job_id,
                              "User {} requested the signal {} on the job {}"
                              .format(user, signal, job_id))
            return (0, None)
        else:
            return (2, error_msg + 'This job is not running.')
    else:
        return (1, error_msg + 'You are not the right user.')

def get_job_current_hostnames(job_id):
    """Returns the list of hosts associated to the job passed in parameter"""

    results = db.query(distinct(Resource.network_address))\
                .filter(AssignedResource.index == 'CURRENT')\
                .filter(MoldableJobDescription.index == 'CURRENT')\
                .filter(AssignedResource.resource_id == Resource.id)\
                .filter(MoldableJobDescription.id == AssignedResource.moldable_id)\
                .filter(MoldableJobDescription.job_id == job_id)\
                .filter(Resource.network_address != '')\
                .filter(Resource.type == 'default')\
                .order_by(Resource.network_address).all()

    return results

def get_job_types(job_id):
    """Returns a hash table with all types for the given job ID."""

    results = db.query(JobType.type).filter(JobType.id == job_id).all()

    res = {}
    for t in results:
        match = re.match(r'^\s*(token)\s*\:\s*(\w+)\s*=\s*(\d+)\s*$', t)
        if match:
            res[match.group(1)] = {match.group(2): match.group(3)}
        else:
            match = re.match(r'^\s*(\w+)\s*=\s*(.+)$', t)
            if match:
                res[match.group(1)] = match.group(2)
            else:
                res[t] = True
    return res

def hold_job(job_id, running, user=None):
    """sets the state field of a job to 'Hold'
    equivalent to set_job_state(base,jobid,"Hold") except for permissions on user
    parameters : jobid, running, user
    return value : 0 on success, -1 on error (if the user calling this method
    is not the user running the job)
    side effects : changes the field state of the job to 'Hold' in the table Jobs.
    """

    if not user:
        if 'OARDO_USER' in os.environ:
            user = os.environ['OARDO_USER']
        else:
            user = os.environ['USER']

    job = get_job(job_id)

    user_allowed_hold_resume = False
    if 'USERS_ALLOWED_HOLD_RESUME' in config and  config['USERS_ALLOWED_HOLD_RESUME'] == 'yes':
        user_allowed_hold_resume = True

    event_type = 'HOLD_WAITING_JOB'
    if running:
        event_type = 'HOLD_RUNNING_JOB'

    if job:
        if running and (not user_allowed_hold_resume) and (user != 'oar') and (user != 'root'):
            return -4
        elif (user == job.user) or (user == 'oar') or (user == 'root'):
            if ((job.state == 'Waiting') or (job.state == 'Resuming')) or\
               (running and (job.state == 'toLaunch' or job.state == 'Launching' or job.state == 'Running')):
                add_new_event(event_type, job_id,
                              'User {} launched oarhold on the job {}'.format(user, job_id))
                return 0
            else:
                return -3
        else:
            return -2
    else:
        return -1
    
    return 0

def resume_job(job_id, user=None):
    """Returns the state of the job from 'Hold' to 'Waiting'
    equivalent to set_job_state(base,jobid,"Waiting") except for permissions on
    user and the fact the job must already be in 'Hold' state
    parameters : jobid
    return value : 0 on success, -1 on error (if the user calling this method
    is not the user running the job)
    side effects : changes the field state of the job to 'Waiting' in the table
    Jobs
    """
    if not user:
        if 'OARDO_USER' in os.environ:
            user = os.environ['OARDO_USER']
        else:
            user = os.environ['USER']

    job = get_job(job_id)
    
    user_allowed_hold_resume = False
    if 'USERS_ALLOWED_HOLD_RESUME' in config and  config['USERS_ALLOWED_HOLD_RESUME'] == 'yes':
        user_allowed_hold_resume = True

    if job:
        if (job.state == 'Suspended') and (not user_allowed_hold_resume) and (user != 'oar') and (user != 'root'):
            return -4
        elif (user == job.user) or (user == 'oar') or (user == 'root'):
            if (job.state == 'Hold') or (job.state == 'Suspended'):
                add_new_event('RESUME_JOB', job_id,
                              'User {} launched oarresume on the job {}'.format(user, job_id))
                return 0
            return -3
        return -2
    else:
        return -1

