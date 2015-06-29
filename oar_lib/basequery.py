# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import

from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Query, Load

from .exceptions import DoesNotExist
from . import db
from .models import (Job, MoldableJobDescription, AssignedResource,
                     GanttJobsPredictionsVisu, GanttJobsResourcesVisu,
                     Resource)
from .utils import render_query


__all__ = ['BaseQuery', 'BaseQueryCollection']


class BaseQuery(Query):

    def render(self):
        print(render_query(self))

    def get_or_error(self, uid):
        """Like :meth:`get` but raises an error if not found instead of
        returning `None`.
        """
        rv = self.get(uid)
        if rv is None:
            raise DoesNotExist()
        return rv

    def first_or_error(self):
        """Like :meth:`first` but raises an error if not found instead of
        returning `None`.
        """
        rv = self.first()
        if rv is None:
            raise DoesNotExist()
        return rv

    def filter_jobs_for_user(self, user, start_time, stop_time, states,
                             job_ids, array_id):
        if not states:
            states = ['Finishing', 'Running', 'Resuming', 'Suspended',
                      'Launching', 'toLaunch', 'Waiting', 'Hold',
                      'toAckReservation']
        q1_start, q3_start, q2_start = None, None, None
        if start_time is not None:
            # first sub query start time filter
            timeout = func(Job.start_time + MoldableJobDescription.walltime)
            q1_start = and_(timeout >= start_time, Job.state == 'Running')
            q1_start = or_(q1_start, Job.state.in_('Running', 'Resuming'))
            q1_start = and_(Job.stop_time == 0, q1_start)
            q1_start = or_(Job.stop_time >= start_time, q1_start)
            # second sub query start time filter
            timeout = func(GanttJobsPredictionsVisu.start_time
                           + MoldableJobDescription.walltime)
            q2_start = timeout >= start_time
            # third sub query start time filter
            q3_start = start_time <= Job.submission_time

        q1_stop, q3_stop, q2_stop = None, None, None
        if stop_time is not None:
            # first sub query stop time filter
            q1_stop = Job.start_time < stop_time
            # second sub query stop time filter
            q2_stop = GanttJobsPredictionsVisu.start_time < stop_time
            # third sub query stop time filter
            q3_stop = Job.submission_time < stop_time

        def apply_commons_filters(q, *criterion):
            q = q.filter(Job.user == user) if user else q
            q = q.filter(Job.state.in_(states)) if states else q
            q = q.filter(Job.array_id == array_id) if array_id else q
            q = q.filter(Job.id.in_(job_ids)) if job_ids else q
            for criteria in criterion:
                q = q.filter(criteria) if criteria is not None else q
            return q

        q1 = db.query(Job.id.label('job_id')).distinct()\
               .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)\
               .filter(MoldableJobDescription.job_id == Job.id)
        q1 = apply_commons_filters(q1, q1_start, q1_stop)

        q2 = db.query(Job.id.label('job_id')).distinct()\
               .filter(GanttJobsPredictionsVisu.moldable_id == GanttJobsResourcesVisu.moldable_id)\
               .filter(GanttJobsPredictionsVisu.moldable_id == MoldableJobDescription.id)\
               .filter(Job.id == MoldableJobDescription.job_id)
        q2 = apply_commons_filters(q2, q2_start, q2_stop)

        q3 = db.query(Job.id.label('job_id')).distinct()\
               .filter(Job.start_time == 0)
        q3 = apply_commons_filters(q3, q3_start, q3_stop)

        unionquery = q1.union(q2.union(q3)).subquery()
        return self.join(MoldableJobDescription, Job.assigned_moldable_job == MoldableJobDescription.id)\
                   .join(unionquery, Job.id == unionquery.c.job_id)


class BaseQueryCollection(object):
    """ Queries collection. """
    def get_jobs_for_user(self, user, start_time, stop_time, states, job_ids,
                          array_id, detailed=True):
        """ Get all distinct jobs for a user query. """
        if detailed:
            query = db.query(Job)
        else:
            columns = ("id", "name", "queue_name", "user", "submission_time",
                       "state")
            option = Load(Job).load_only(*columns)
            query = db.query(Job).options(option)
        return query.order_by(Job.id)\
                    .filter_jobs_for_user(user, start_time,
                                          stop_time, states,
                                          job_ids, array_id)

    def get_resources(self, network_address, detailed=True):
        if detailed:
            query = db.query(Resource)
        else:
            columns = ("id", "state", "available_upto", "network_address")
            option = Load(Resource).load_only(*columns)
            query = db.query(Resource).options(option)
        if network_address is not None:
            query = query.filter_by(network_address=network_address)
        return query.order_by(Resource.id.asc())

    def groupby_jobs_resources(self, jobs, query):
        jobs_resources = dict(((job.id, []) for job in jobs))
        for job_id, resource in query:
            jobs_resources[job_id].append(resource)
        return jobs_resources

    def get_assigned_jobs_resources(self, jobs):
        """Returns the list of assigned resources associated to the job passed
        in parameter."""
        columns = ("id",)
        job_id_column = AssignedResource.moldable_id.label('job_id')
        query = db.query(job_id_column, Resource)\
                  .options(Load(Resource).load_only(*columns))\
                  .join(Resource, Resource.id == AssignedResource.resource_id)\
                  .filter(job_id_column.in_([job.id for job in jobs]))\
                  .order_by(job_id_column.asc())
        return self.groupby_jobs_resources(jobs, query)

    def get_gantt_visu_scheduled_jobs_resources(self, jobs):
        """Returns network_address allocated to a (waiting) reservation."""
        columns = ("id",)
        job_id_column = MoldableJobDescription.moldable_id.label('job_id')
        query = db.query(job_id_column.label('job_id'), Resource)\
                  .options(Load(Resource).load_only(*columns))\
                  .filter(Resource.id == GanttJobsResourcesVisu.resource_id)\
                  .filter(job_id_column == GanttJobsResourcesVisu.moldable_id)\
                  .filter(job_id_column.in_([job.id for job in jobs]))\
                  .order_by(job_id_column.asc())
        return self.groupby_jobs_resources(jobs, query)

