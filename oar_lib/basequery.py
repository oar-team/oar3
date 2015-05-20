# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import


from sqlalchemy.orm import Query

from .exceptions import DoesNotExist
from .models import Job
from . import db

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


class BaseQueryCollection(object):
    """ Queries collection. """
    def get_all_jobs(self):
        db.query(Job)
