# coding: utf-8
""" Functions to handle jobs """

# TODO move some function from oar/kao/job.py
from __future__ import unicode_literals, print_function
from oar.lib import (db, Job)


def get_array_job_ids(array_id):
    """ Get all the job_ids of a given array of job identified by its id"""
    results = db.query(Job.id)\
                .filter(Job.array_id == array_id)\
                .order_by(Job.id).all()
    return results
