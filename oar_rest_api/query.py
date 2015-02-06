# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import

from collections import OrderedDict
from math import ceil

from flask import abort, current_app, request, url_for
from oar.lib.database import BaseQuery


class APIBaseQuery(BaseQuery):

    def get_or_404(self, ident):
        try:
            return self.get_or_error(ident)
        except:
            abort(404)

    def first_or_404(self):
        try:
            return self.first_or_error()
        except:
            abort(404)

    def paginate(self, offset, limit, error_out=True):
        if limit is None:
            limit = current_app.config.get("API_DEFAULT_MAX_ITEMS_NUMBER")
        if error_out and offset < 0:
            abort(404)
        items = self.limit(limit).offset(offset).all()
        if not items and offset != 0 and error_out:
            abort(404)
        # No need to count if we're on the first page and there are fewer
        # items than we expected.
        if offset == 0 and len(items) < limit:
            total = len(items)
            limit = total
        else:
            total = self.order_by(None).count()
        return Pagination(offset, limit, total, items)


class Pagination(object):
    """Internal helper class returned by :meth:`APIBaseQuery.paginate`."""

    def __init__(self, offset, limit, total, items):
        self.offset = offset
        self.limit = limit or 0
        self.total = total
        self.items = items

    @property
    def current_page(self):
        """The number of the current page (1 indexed)"""
        if self.limit > 0 and self.offset > 0:
            return int(ceil(self.offset / float(self.limit))) + 1
        return 1

    @property
    def pages(self):
        """The total number of pages"""
        if self.total > 0 and self.limit > 0:
            return int(ceil(self.total / float(self.limit)))
        return 1

    @property
    def has_next(self):
        """True if a next page exists."""
        return self.current_page < self.pages

    @property
    def next_url(self):
        """Returns the next url for the current endpoint."""
        if self.has_next:
            kwargs = {'offset': self.offset + self.limit, 'limit': self.limit}
            return url_for(request.endpoint, **kwargs)

    @property
    def url(self):
        """Returns the url for the current endpoint."""
        kwargs = {'offset': self.offset}
        if self.limit > 0:
            kwargs['limit'] = self.limit
        return url_for(request.endpoint, **kwargs)

    def __iter__(self):
        for item in self.items:
            result = OrderedDict()
            for key in item.keys():
                result[key] = getattr(item, key)
            yield result
