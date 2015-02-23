# -*- coding: utf-8 -*-
from __future__ import division

import sys
import click

from oar.lib import config, Database
from oar.lib.compat import reraise
from functools import update_wrapper
from oar.lib.utils import cached_property

magenta = lambda x: click.style("%s" % x, fg="magenta")
yellow = lambda x: click.style("%s" % x, fg="yellow")
green = lambda x: click.style("%s" % x, fg="green")
blue = lambda x: click.style("%s" % x, fg="blue")
red = lambda x: click.style("%s" % x, fg="red")


class Context(object):

    def __init__(self):
        self.archive_db_suffix = "archive"
        self.debug = False
        self.force_yes = False

    @cached_property
    def current_db_url(self):
        return config.get_sqlalchemy_uri()

    @cached_property
    def archive_db_url(self):
        return "%s_%s" % (self.current_db_url, self.archive_db_suffix)

    @cached_property
    def archive_db(self):
        return Database(uri=self.archive_db_url)

    @cached_property
    def current_db(self):
        from oar.lib import db
        return db

    @cached_property
    def archive_db_name(self):
        return self.archive_db.engine.url.database

    @cached_property
    def current_db_name(self):
        return self.current_db.engine.url.database

    def print_db_info(self):
        self.log("")
        self.log("OAR database: %s" % self.current_db_url)
        self.log("OAR archive database: %s" % self.archive_db_url)
        self.log("")

    def log(self, *args, **kwargs):
        """Logs a message to stderr."""
        kwargs.setdefault("file", sys.stderr)
        prefix = kwargs.pop("prefix", "")
        for msg in args:
            click.echo(prefix + msg, **kwargs)

    def confirm(self, message):
        if not self.force_yes:
            if not click.confirm(message):
                raise click.Abort()

    def handle_error(self):
        exc_type, exc_value, tb = sys.exc_info()
        if not self.debug:
            sys.stderr.write(u"\nError: %s\n" % exc_value)
            sys.exit(1)
        else:
            reraise(exc_type, exc_value, tb.tb_next)


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
