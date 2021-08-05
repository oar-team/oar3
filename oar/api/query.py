# -*- coding: utf-8 -*-
from math import ceil

from flask import abort, current_app

from oar.lib.basequery import BaseQuery, BaseQueryCollection

# from oar.lib.models import (db, Job, Resource)
from oar.lib.utils import cached_property, row2dict

from .url_utils import replace_query_params


class APIQuery(BaseQuery):
    def get_or_404(self, ident):
        try:
            return self.get_or_error(ident)
        except Exception:
            abort(404)

    def first_or_404(self):
        try:
            return self.first_or_error()
        except Exception:
            abort(404)

    def paginate(self, request, offset, limit, error_out=True):
        if limit is None:
            limit = current_app.config.get("API_DEFAULT_MAX_ITEMS_NUMBER")
        if error_out and offset < 0:
            abort(404)
        return PaginationQuery(self, request, offset, limit, error_out)


class PaginationQuery(object):
    """Internal helper class returned by :meth:`APIBaseQuery.paginate`."""

    def __init__(self, query, request, offset, limit, error_out):
        self.request = request
        self.query = query.limit(limit).offset(offset)
        self.items = self.query.all()
        self.offset = offset
        self.limit = limit

        # No need to count if we're on the first page and there are fewer
        # items than we expected.
        if offset == 0 and len(self.items) < limit:
            self.total = len(self.items)
        else:
            self.total = query.order_by(None).count()

        if not self.items and offset != 0 and error_out:
            abort(404)

    def render(self):
        self.query.render()

    @cached_property
    def current_page(self):
        """The number of the current page (1 indexed)"""
        if self.limit > 0 and self.offset > 0:
            return int(ceil(self.offset / float(self.limit))) + 1
        return 1

    @cached_property
    def pages(self):
        """The total number of pages"""
        if self.total > 0 and self.limit > 0:
            return int(ceil(self.total / float(self.limit)))
        return 1

    @cached_property
    def has_next(self):
        """True if a next page exists."""
        return self.current_page < self.pages

    @cached_property
    def has_previous(self):
        """True if a previous page exists."""
        return self.current_page > 1

    @cached_property
    def next_url(self):
        """Returns the next url for the current endpoint."""
        if self.has_next:
            kwargs = dict(self.request.query_params)
            # kwargs.update(request.view_args.copy())
            kwargs["offset"] = self.offset + self.limit
            kwargs["limit"] = self.limit
            endpoint_url = self.request.url.path
            return replace_query_params(endpoint_url, kwargs)

    @cached_property
    def previous_url(self):
        """Returns the next previous for the current endpoint."""
        if self.has_previous:
            kwargs = dict(self.request.query_params)
            # kwargs.update(request.view_args.copy())
            kwargs["offset"] = self.offset - self.limit
            kwargs["limit"] = self.limit
            endpoint_url = self.request.app.url_path_for("index")
            return replace_query_params(endpoint_url, kwargs)

    @cached_property
    def current_url(self):
        """Returns the url for the current endpoint."""
        # kwargs = g.request_args.copy()
        # kwargs.update(request.view_args.copy())
        # kwargs["offset"] = self.offset
        # if self.limit > 0:
        #     kwargs["limit"] = self.limit
        # return url_for(request.endpoint, **kwargs)
        return "oops"

    @cached_property
    def links(self):
        links = []
        if self.has_previous:
            links.append({"rel": "previous", "href": self.previous_url})
        links.append({"rel": "self", "href": self.current_url})
        if self.has_next:
            links.append({"rel": "next", "href": self.next_url})
        return links

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
