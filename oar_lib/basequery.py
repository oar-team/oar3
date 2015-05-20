# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import


from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Query

from .exceptions import DoesNotExist
from . import db
from .models import (Job, MoldableJobDescription, AssignedResource,
                     GanttJobsPredictionsVisu, GanttJobsResourcesVisu)

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

    def filter_jobs_for_user(self, user, from_, to, states, array_id, ids):
        q1_from, q3_from, q2_from = None, None, None
        if from_ is not None:
            # first sub query start time filter
            timeout = func(Job.start_time + MoldableJobDescription.walltime)
            q1_from = and_(timeout >= from_, Job.state == 'Running')
            q1_from = or_(q1_from, Job.state.in_('Running', 'Resuming'))
            q1_from = and_(Job.stop_time == 0, q1_from)
            q1_from = or_(Job.stop_time >= from_, q1_from)
            # second sub query start time filter
            timeout = func(GanttJobsPredictionsVisu.start_time + MoldableJobDescription.walltime)
            q2_from = timeout >= from_
            # third sub query start time filter
            q3_from = from_ <= Job.submission_time

        q1_to, q3_to, q2_to = None, None, None
        if to is not None:
            # first sub query stop time filter
            q1_to = Job.start_time < to
            # second sub query stop time filter
            q2_to = GanttJobsPredictionsVisu.start_time < to
            # third sub query stop time filter
            q3_to = Job.submission_time < to

        def apply_commons_filters(q, *criterion):
            q = q.filter(Job.user == user) if user else q
            q = q.filter(Job.state.in_(states)) if states else q
            q = q.filter(Job.array_id == array_id) if array_id else q
            q = q.filter(Job.id.in_(ids)) if ids else q
            for criteria in criterion:
                q = q.filter(criteria) if criteria is not None else q
            return q

        q1 = db.query(Job.id).distinct()\
               .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)\
               .filter(MoldableJobDescription.job_id == Job.id)
        q1 = apply_commons_filters(q1, q1_from, q1_to)

        q2 = db.query(Job.id).distinct()\
               .filter(GanttJobsPredictionsVisu.moldable_id == GanttJobsResourcesVisu.moldable_id)\
               .filter(GanttJobsPredictionsVisu.moldable_id == MoldableJobDescription.id)\
               .filter(Job.id == MoldableJobDescription.job_id)
        q2 = apply_commons_filters(q2, q2_from, q2_to)

        q3 = db.query(Job.id).distinct().filter(Job.start_time == 0)
        q3 = apply_commons_filters(q3, q3_from, q3_to)

        return self.join(MoldableJobDescription, Job.assigned_moldable_job == MoldableJobDescription.id)\
                   .filter(Job.id.in_(q1.union(q2.union(q3))))


class BaseQueryCollection(object):
    """ Queries collection. """
    def get_all_jobs(self):
        db.query(Job)
