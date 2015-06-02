# -*- coding: utf-8 -*-
from __future__ import division

import sys
import re
import click
import time
import logging

from functools import reduce
from sqlalchemy import func, and_, not_
from sqlalchemy import event
from sqlalchemy.engine import Engine
from oar.lib import Database
from oar.lib.compat import reraise
from functools import update_wrapper
from oar.lib.utils import cached_property


magenta = lambda x, **kwargs: click.style("%s" % x, fg="magenta", **kwargs)
yellow = lambda x, **kwargs: click.style("%s" % x, fg="yellow", **kwargs)
green = lambda x, **kwargs: click.style("%s" % x, fg="green", **kwargs)
cyan = lambda x, **kwargs: click.style("%s" % x, fg="cyan", **kwargs)
blue = lambda x, **kwargs: click.style("%s" % x, fg="blue", **kwargs)
red = lambda x, **kwargs: click.style("%s" % x, fg="red", **kwargs)


class Context(object):

    def __init__(self):
        self.archive_db_suffix = "archive"
        self.max_job_id = None
        self.enable_pagination = False
        self.debug = False
        self.force_yes = False

    def configure_log(self):
        logging.basicConfig()
        self.logger = logging.getLogger("oar.database")

        if not self.debug:
            self.logger.setLevel(logging.INFO)
            return

        self.logger.setLevel(logging.DEBUG)

        for handler in self.logger.root.handlers:
            handler.setFormatter(AnsiColorFormatter())

        @event.listens_for(Engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement,
                                  parameters, context, executemany):
            conn.info.setdefault('query_start_time', []).append(time.time())
            self.logger.debug("Start Query: \n%s\n" % statement)

        @event.listens_for(Engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement,
                                 parameters, context, executemany):
            total = time.time() - conn.info['query_start_time'].pop(-1)
            self.logger.debug("Query Complete!")
            self.logger.debug("Total Time: %f" % total)

    @cached_property
    def archive_db(self):
        return Database(uri=self.archive_db_url)

    @cached_property
    def current_db(self):
        from oar.lib import db
        db._cache["uri"] = self.current_db_url
        return db

    @cached_property
    def archive_db_name(self):
        return self.archive_db.engine.url.database

    @cached_property
    def current_db_name(self):
        return self.current_db.engine.url.database

    @cached_property
    def current_models(self):
        """ Return a namespace with all mapping classes"""
        from oar.lib.models import all_models  # avoid a circular import
        return dict(all_models())

    @cached_property
    def current_tables(self):
        """ Return a namespace with all tables classes"""
        self.current_db.reflect()
        sorted_tables = self.current_db.metadata.sorted_tables
        return dict((t.name, t) for t in sorted_tables)

    @cached_property
    def ignored_resources_criteria(self):
        criteria = []
        exclude = []
        include = []
        if "all" in self.ignore_resources:
            return
        for raw_state in self.ignore_resources:
            if raw_state.startswith("^"):
                exclude.append(raw_state.lstrip("^"))
            else:
                include.append(raw_state)
        if exclude:
            criteria.append(self.current_models["Resource"].state
                                                           .notin_(exclude))
        if include:
            criteria.append(self.current_models["Resource"].state
                                                           .in_(include))
        return reduce(and_, criteria)

    @cached_property
    def ignored_jobs_criteria(self):
        criteria = []
        exclude = []
        include = []
        for raw_state in self.ignore_jobs:
            if raw_state.startswith("^"):
                exclude.append(raw_state.lstrip("^"))
            else:
                include.append(raw_state)
        if exclude:
            criteria.append(self.current_models["Job"].state.notin_(exclude))
        if include:
            criteria.append(self.current_models["Job"].state.in_(include))
        return reduce(and_, criteria)

    @cached_property
    def max_job_to_sync(self):
        model = self.current_models["Job"]
        acceptable_max_job_id = self.current_db.query(func.min(model.id))\
                                    .filter(self.ignored_jobs_criteria)\
                                    .scalar()
        if acceptable_max_job_id is None:
            if self.max_job_id is None:
                # returns the real max job id
                return self.current_db.query(func.max(model.id)).scalar() or 0
            else:
                return self.max_job_id
        else:
            if self.max_job_id is not None:
                if self.max_job_id < acceptable_max_job_id:
                    return self.max_job_id
            return acceptable_max_job_id

    @cached_property
    def max_moldable_job_to_sync(self):
        moldable_model = self.current_models["MoldableJobDescription"]
        criteria = moldable_model.job_id < self.max_job_to_sync
        job_model = self.current_models["Job"]
        query = self.current_db.query(func.max(moldable_model.id))\
                               .join(job_model,
                                     moldable_model.job_id == job_model.id)\
                               .filter(criteria)
        return query.scalar() or 0

    @cached_property
    def resources_to_purge(self):
        if self.ignored_resources_criteria is None:
            return []
        assigned_model = self.current_models["AssignedResource"]
        resource_model = self.current_models["Resource"]
        max_moldable = self.max_moldable_job_to_sync
        query = self.current_db.query(resource_model.id)
        excludes = query.filter(not_(self.ignored_resources_criteria))\
                        .join(assigned_model,
                              resource_model.id == assigned_model.resource_id)\
                        .filter(assigned_model.moldable_id >= max_moldable)\
                        .group_by(resource_model.id).all()
        excludes_set = set([resource_id for resource_id, in excludes])
        resources = query.filter(not_(self.ignored_resources_criteria)).all()
        resources_set = set([resource_id for resource_id, in resources])
        return list(resources_set - excludes_set)

    def log(self, *args, **kwargs):
        """Logs a message to stderr."""
        kwargs.setdefault("file", sys.stderr)
        prefix = kwargs.pop("prefix", "")
        for msg in args:
            message = prefix + msg
            if self.debug:
                message = message.replace('\r', '')
                kwargs['nl'] = True
            click.echo(message, **kwargs)

    def confirm(self, message, **kwargs):
        if not self.force_yes:
            if not click.confirm(message, **kwargs):
                raise click.Abort()

    def handle_error(self):
        exc_type, exc_value, tb = sys.exc_info()
        if self.debug or isinstance(exc_value, (click.ClickException,
                                                click.Abort)):
            reraise(exc_type, exc_value, tb.tb_next)
        else:
            sys.stderr.write(u"\nError: %s\n" % exc_value)
            sys.exit(1)


def make_pass_decorator(ensure=False):
    def decorator(f):
        @click.pass_context
        def new_func(*args, **kwargs):
            ctx = args[0]
            if ensure:
                obj = ctx.ensure_object(Context)
            else:
                obj = ctx.find_object(Context)
            try:
                return ctx.invoke(f, obj, *args[1:], **kwargs)
            except:
                obj.handle_error()
        return update_wrapper(new_func, f)
    return decorator


pass_context = make_pass_decorator(ensure=True)

re_color_codes = re.compile(r'\033\[(\d;)?\d+m')

LEVELS = {
    'WARNING': red(' WARN'),
    'INFO': blue(' INFO'),
    'DEBUG': blue('DEBUG'),
    'CRITICAL': magenta(' CRIT'),
    'ERROR': red('ERROR'),
}


class AnsiColorFormatter(logging.Formatter):

    def __init__(self, msgfmt=None, datefmt=None):
        logging.Formatter.__init__(self, None, '%H:%M:%S')

    def format(self, record):
        """
        Format the specified record as text.

        The record's attribute dictionary is used as the operand to a
        string formatting operation which yields the returned string.
        Before formatting the dictionary, a couple of preparatory steps
        are carried out. The message attribute of the record is computed
        using LogRecord.getMessage(). If the formatting string contains
        "%(asctime)", formatTime() is called to format the event time.
        If there is exception information, it is formatted using
        formatException() and appended to the message.
        """
        message = record.getMessage()
        asctime = self.formatTime(record, self.datefmt)
        name = yellow(record.name)

        s = '%(timestamp)s %(levelname)s %(name)s ' % {
            'timestamp': green('%s,%03d' % (asctime, record.msecs), bold=True),
            'levelname': LEVELS[record.levelname],
            'name': name,
        }

        if "\n" in message:
            indent_length = len(re_color_codes.sub('', s))
            message = message.replace("\n", "\n" + ' ' * indent_length)

        s += message
