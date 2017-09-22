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

from oar.kao.tools import update_current_scheduler_priority
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

def get_job(job_id):
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

# log_job
# sets the index fields to LOG on several tables
# this will speed up future queries
# parameters : base, jobid
# return value : /


def log_job(job):  # pragma: no cover
    if db.dialect == "sqlite":
        return
    db.query(MoldableJobDescription)\
      .filter(MoldableJobDescription.index == 'CURRENT')\
      .filter(MoldableJobDescription.job_id == job.id)\
      .update({MoldableJobDescription.index: 'LOG'}, synchronize_session=False)

    db.query(JobResourceDescription)\
      .filter(MoldableJobDescription.job_id == job.id)\
      .filter(JobResourceGroup.moldable_id == MoldableJobDescription.id)\
      .filter(JobResourceDescription.group_id == JobResourceGroup.id) \
      .update({JobResourceDescription.index: 'LOG'}, synchronize_session=False)

    db.query(JobResourceGroup)\
      .filter(JobResourceGroup.index == 'CURRENT')\
      .filter(MoldableJobDescription.index == 'LOG')\
      .filter(MoldableJobDescription.job_id == job.id)\
      .filter(JobResourceGroup.moldable_id == MoldableJobDescription.id)\
      .update({JobResourceGroup.index: 'LOG'}, synchronize_session=False)

    db.query(JobType)\
      .filter(JobType.types_index == 'CURRENT')\
      .filter(JobType.job_id == job.id)\
      .update({JobType.types_index: 'LOG'}, synchronize_session=False)

    db.query(JobDependencie)\
      .filter(JobDependencie.index == 'CURRENT')\
      .filter(JobDependencie.job_id == job.id)\
      .update({JobDependencie.index: 'LOG'}, synchronize_session=False)

    if job.assigned_moldable_job != "0":
        db.query(AssignedResource)\
          .filter(AssignedResource.index == 'CURRENT')\
          .filter(AssignedResource.moldable_id == int(job.assigned_moldable_job))\
          .update({AssignedResource.index: 'LOG'},
                  synchronize_session=False)
    db.commit()

def set_job_state(jid, state):

    # TODO
    # TODO Later: notify_user
    # TODO Later: update_current_scheduler_priority

    result = db.query(Job).filter(Job.id == jid)\
                          .filter(Job.state != 'Error')\
                          .filter(Job.state != 'Terminated')\
                          .filter(Job.state != state)\
                          .update({Job.state: state})
    db.commit()

    if result == 1:  # OK for sqlite
        logger.debug(
            "Job state updated, job_id: " + str(jid) + ", wanted state: " + state)

        date = tools.get_date()

        # TODO: optimize job log
        db.query(JobStateLog).filter(JobStateLog.date_stop == 0)\
                             .filter(JobStateLog.job_id == jid)\
                             .update({JobStateLog.date_stop: date})
        db.commit()
        req = db.insert(JobStateLog).values(
            {'job_id': jid, 'job_state': state, 'date_start': date})
        db.session.execute(req)

        if state == "Terminated" or state == "Error" or state == "toLaunch" or \
           state == "Running" or state == "Suspended" or state == "Resuming":
            job = db.query(Job).filter(Job.id == jid).one()
            if state == "Suspend":
                tools.notify_user(job, "SUSPENDED", "Job is suspended.")
            elif state == "Resuming":
                tools.notify_user(job, "RESUMING", "Job is resuming.")
            elif state == "Running":
                tools.notify_user(job, "RUNNING", "Job is running.")
            elif state == "toLaunch":
                update_current_scheduler_priority(job, "+2", "START")
            else:  # job is "Terminated" or ($state eq "Error")
                if job.stop_time < job.start_time:
                    db.query(Job).filter(Job.id == jid)\
                                 .update({Job.stop_time: job.start_time})
                    db.commit()

                if job.assigned_moldable_job != "0":
                    # Update last_job_date field for resources used
                    update_scheduler_last_job_date(
                        date, int(job.assigned_moldable_job))

                if state == "Terminated":
                    tools.notify_user(job, "END", "Job stopped normally.")
                else:
                    # Verify if the job was suspended and if the resource
                    # property suspended is updated
                    if job.suspended == "YES":
                        r = get_current_resources_with_suspended_job()

                        if r != ():
                            db.query(Resource).filter(~Resource.id.in_(r))\
                                              .update({Resource.suspended_jobs: 'NO'})

                        else:
                            db.query(Resource).update(
                                {Resource.suspended_jobs: 'NO'})
                        db.commit()

                    tools.notify_user(
                        job, "ERROR", "Job stopped abnormally or an OAR error occured.")
                #import pdb; pdb.set_trace()
                update_current_scheduler_priority(job, "-2", "STOP")

                # Here we must not be asynchronously with the scheduler
                log_job(job)
                # $dbh is valid so these 2 variables must be defined
                nb_sent = tools.notify_almighty("ChState")
                if nb_sent == 0:
                    logger.warning("Not able to notify almighty to launch the job " +
                                   str(job.id) + " (socket error)")

    else:
        logger.warning("Job is already termindated or in error or wanted state, job_id: " +
                       str(jid) + ", wanted state: " + state)

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

