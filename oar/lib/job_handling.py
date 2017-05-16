# coding: utf-8
""" Functions to handle jobs """

# TODO move some function from oar/kao/job.py
from __future__ import unicode_literals, print_function

import os

from sqlalchemy import text
from oar.lib import (db, Job, MoldableJobDescription, JobResourceDescription,
                     JobResourceGroup, Resource, GanttJobsPrediction,
                     JobDependencie, GanttJobsResource, JobType,
                     JobStateLog, AssignedResource, FragJob,
                     get_logger, config)

from oar.lib.event import add_new_event

import oar.lib.tools as tools

logger = get_logger('oar.lib.job_handling')

def get_array_job_ids(array_id):
    """ Get all the job_ids of a given array of job identified by its id"""
    results = db.query(Job.id).filter(Job.array_id == array_id)\
                .order_by(Job.id).all()
    return results

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


def frag_job(jid):
    """Sets the flag 'ToFrag' of a job to 'Yes' which will threshold job deletion"""
    if 'OARDO_USER' in os.environ:
        luser = os.environ['OARDO_USER']
    else:
        luser = os.environ['USER']

    job = get_job(jid)

    if (job is not None) and ((luser == job.user)
                              or (luser == 'oar')
                              or (luser == 'root')):
        res = db.query(FragJob).filter(FragJob.job_id == jid).all()

        if len(res) == 0:

            date = tools.get_date()
            frajob = FragJob(job_id=jid, date=date)
            db.add(frajob)
            db.commit()
            add_new_event("FRAG_JOB_REQUEST",
                          jid, "User %s requested to frag the job %s"
                          % (luser, str(jid)))
            return 0
        else:
            # Job already killed
            return -2
    else:
        return -1
