# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import

from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Query

from .exceptions import DoesNotExist
from . import db
from .models import (Job, MoldableJobDescription, AssignedResource,
                     GanttJobsPredictionsVisu, GanttJobsResourcesVisu,
                     Resource)


__all__ = ['BaseQuery', 'BaseQueryCollection']


class BaseQuery(Query):

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

        q1 = db.query(Job.id).distinct()\
               .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)\
               .filter(MoldableJobDescription.job_id == Job.id)
        q1 = apply_commons_filters(q1, q1_start, q1_stop)

        q2 = db.query(Job.id).distinct()\
               .filter(GanttJobsPredictionsVisu.moldable_id == GanttJobsResourcesVisu.moldable_id)\
               .filter(GanttJobsPredictionsVisu.moldable_id == MoldableJobDescription.id)\
               .filter(Job.id == MoldableJobDescription.job_id)
        q2 = apply_commons_filters(q2, q2_start, q2_stop)

        q3 = db.query(Job.id).distinct().filter(Job.start_time == 0)
        q3 = apply_commons_filters(q3, q3_start, q3_stop)

        return self.join(MoldableJobDescription, Job.assigned_moldable_job == MoldableJobDescription.id)\
                   .filter(Job.id.in_(q1.union(q2.union(q3))))


class BaseQueryCollection(object):
    """ Queries collection. """
    def get_jobs_for_user(self, user, start_time, stop_time, states, job_ids,
                          array_id, detailed=True):
        """ Get all distinct jobs for a user query. """
        if detailed:
            query = db.query(Job)
        else:
            query = db.query(Job.id,
                             Job.name,
                             Job.queue_name,
                             Job.user,
                             Job.submission_time)
        return query.order_by(Job.id)\
                    .filter_jobs_for_user(user, start_time,
                                          stop_time, states,
                                          job_ids, array_id)

    def get_resources(self, network_address, detailed=True):
        if detailed:
            query = db.query(Resource)
        else:
            query = db.query(Resource.id,
                             Resource.state,
                             Resource.available_upto,
                             Resource.network_address)
        if network_address is not None:
            query = query.filter_by(network_address=network_address)
        return query.order_by(Resource.id.asc())

    def get_job_resources(self, moldable_id, detailed=False):
        """ Returns the list of resources associated to the job passed in
        parameter """
        if detailed:
            query = db.query(Resource)
        else:
            query = db.query(Resource.id)
        query = query.join(AssignedResource,
                           AssignedResource.resource_id == Resource.id)\
                     .filter(AssignedResource.moldable_id == moldable_id)
        return query.order_by(Resource.id.asc())
