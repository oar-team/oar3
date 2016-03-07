# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import

from sqlalchemy.orm import Query, Load

from .exceptions import DoesNotExist
from . import db
from .models import (Job, MoldableJobDescription, AssignedResource,
                     GanttJobsPredictionsVisu, GanttJobsResourcesVisu,
                     Resource)
from .utils import render_query
from .compat import str


__all__ = ['BaseQuery', 'BaseQueryCollection']


class BaseQuery(Query):

    def render(self):
        class QueryStr(str):
            # Useful for debug
            def __repr__(self):
                return self.replace(' \n', '\n').strip()

        return QueryStr(render_query(self))

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

    def filter_jobs_for_user(self, user, from_time, to_time, states,
                             job_ids, array_id):
        if not states:
            states = ['Finishing', 'Running', 'Resuming', 'Suspended',
                      'Launching', 'toLaunch', 'Waiting', 'Hold',
                      'toAckReservation']
        c1_from, c2_from, c3_from = None, None, None
        if from_time is not None:
            # first sub query start time filter
            c1 = (Job.start_time + MoldableJobDescription.walltime) >= from_time
            c2 = Job.state == 'Running'
            c3 = Job.state.in_(('Suspended', 'Resuming'))
            c4 = Job.stop_time == 0
            c5 = Job.stop_time >= from_time
            c1_from = c5 | (c4 & ((c2 & c1) | c3))
            # second sub query start time filter
            c2_from = GanttJobsPredictionsVisu.start_time + MoldableJobDescription.walltime >= from_time
            # third sub query start time filter
            c3_from = Job.submission_time >= from_time

        c1_to, c2_to, c3_to = None, None, None
        if to_time is not None:
            # first sub query stop time filter
            c1_to = Job.start_time < to_time
            # second sub query stop time filter
            c2_to = GanttJobsPredictionsVisu.start_time < to_time
            # third sub query stop time filter
            c3_to = Job.submission_time <= to_time

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
        q1 = apply_commons_filters(q1, c1_from, c1_to)

        q2 = db.query(Job.id.label('job_id')).distinct()\
               .filter(GanttJobsPredictionsVisu.moldable_id == GanttJobsResourcesVisu.moldable_id)\
               .filter(GanttJobsPredictionsVisu.moldable_id == MoldableJobDescription.id)\
               .filter(Job.id == MoldableJobDescription.job_id)
        q2 = apply_commons_filters(q2, c2_from, c2_to)

        q3 = db.query(Job.id.label('job_id')).distinct()\
               .filter(Job.stop_time == 0)
        q3 = apply_commons_filters(q3, c3_from, c3_to)

        unionquery = q1.union(q2, q3).subquery()
        return self.outerjoin(MoldableJobDescription, Job.assigned_moldable_job == MoldableJobDescription.id)\
                   .join(unionquery, Job.id == unionquery.c.job_id)


class BaseQueryCollection(object):
    """ Queries collection. """
    def get_jobs_for_user(self, user, from_time=None, to_time=None,
                          states=None, job_ids=None, array_id=None,
                          detailed=True):
        """ Get all distinct jobs for a user query. """
        if detailed:
            query = db.query(Job)
        else:
            columns = ("id", "name", "queue_name", "user", "submission_time",
                       "state")
            option = Load(Job).load_only(*columns)
            query = db.query(Job).options(option)
        return query.order_by(Job.id)\
                    .filter_jobs_for_user(user, from_time,
                                          to_time, states,
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
