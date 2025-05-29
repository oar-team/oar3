# -*- coding: utf-8 -*-
from math import ceil

from fastapi import HTTPException

from oar.lib.basequery import BaseQuery, BaseQueryCollection

# from oar.lib.models import (db, Job, Resource)
from oar.lib.utils import row2dict

# from flask import abort, current_app
# TODO: This whole file is to review since it has been adapted from flask and now use in fastapi.
# Especially the error handling previously done with abort from flask


def paginate(query, offset, limit, error_out=True):
    if limit is None:
        raise Exception("Handle this case")
        # limit = current_app.config.get("API_DEFAULT_MAX_ITEMS_NUMBER")
    if error_out and offset < 0:
        raise HTTPException(status_code=404, detail="Pagination out of bounds")

    return PaginationQuery(query, offset, limit, error_out)


class APIQuery(BaseQuery):
    def __init__(self, session):
        super(APIQuery, self).__init__(session)

    def get_or_404(self, query, ident):
        return query.get_or_error(ident)

    def first_or_404(self):
        return self.first_or_error()

    def paginate(self, offset, limit, error_out=True):
        if limit is None:
            raise Exception("Handle this case")
            # limit = current_app.config.get("API_DEFAULT_MAX_ITEMS_NUMBER")
        return PaginationQuery(self, offset, limit, error_out)


class PaginationQuery(object):
    """Internal helper class returned by :meth:`APIBaseQuery.paginate`."""

    def __init__(self, query, offset, limit, error_out):
        self.query = query.limit(limit).offset(offset)
        print(self.query)
        self.items = self.query.all()
        self.offset = offset
        self.limit = limit

        print("offfset", offset, self.items)
        # No need to count if we're on the first page and there are fewer
        # items than we expected.
        if offset == 0 and len(self.items) < limit:
            self.total = len(self.items)
        else:
            self.total = query.order_by(None).count()

        if not self.items and offset != 0 and error_out:
            raise HTTPException(status_code=404, detail="Empty request")

    def render(self):
        self.query.render()

    def current_page(self):
        """The number of the current page (1 indexed)"""
        if self.limit > 0 and self.offset > 0:
            return int(ceil(self.offset / float(self.limit))) + 1
        return 1

    def pages(self):
        """The total number of pages"""
        if self.total > 0 and self.limit > 0:
            return int(ceil(self.total / float(self.limit)))
        return 1

    def has_next(self):
        """True if a next page exists."""
        return self.current_page < self.pages

    def has_previous(self):
        """True if a previous page exists."""
        return self.current_page > 1

    def __iter__(self):
        for item in self.items:
            if hasattr(item, "keys") and callable(getattr(item, "keys")):
                yield row2dict(item)
            elif hasattr(item, "asdict") and callable(getattr(item, "asdict")):
                yield item.asdict()
            else:
                yield item


class APIQueryCollection(BaseQueryCollection):
    pass
