# -*- coding: utf-8 -*-
import logging
import re
import sys
import time
from functools import update_wrapper

import click
from sqlalchemy import event
from sqlalchemy.engine import Engine

from oar.lib import config
from oar.lib.utils import reraise


def red(x, **kwargs):
    return click.style("%s" % x, fg="red", **kwargs)


def blue(x, **kwargs):
    return click.style("%s" % x, fg="blue", **kwargs)


def cyan(x, **kwargs):
    return click.style("%s" % x, fg="cyan", **kwargs)


def green(x, **kwargs):
    return click.style("%s" % x, fg="green", **kwargs)


def yellow(x, **kwargs):
    return click.style("%s" % x, fg="yellow", **kwargs)


def magenta(x, **kwargs):
    return click.style("%s" % x, fg="magenta", **kwargs)


DATABASE_URL_PROMPT = """Please enter the url for your database.

For example:
PostgreSQL: postgresql://scott:tiger@localhost/foo
MySQL: mysql://scott:tiger@localhost/bar

For more information, see : http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls

OAR database URL"""  # noqa


def load_configuration_file(ctx, param, value):
    if value is not None:
        if not value == config.DEFAULT_CONFIG_FILE:
            config.load_file(value, clear=True)
        return config


def default_database_url():
    try:
        return config.get_sqlalchemy_uri()
    except Exception:
        pass


def config_default_value(value):
    def callback():
        return config.get(value)

    return callback


class Context(object):
    def __init__(self):
        self.debug = False
        self.verbose = False
        self.force_yes = False

    def configure_log(self):
        logging.basicConfig()
        self.logger = logging.getLogger("oar.cli.database")

        if not self.verbose:
            self.logger.setLevel(logging.INFO)
            return

        self.logger.setLevel(logging.DEBUG)

        for handler in self.logger.root.handlers:
            handler.setFormatter(AnsiColorFormatter())

        @event.listens_for(Engine, "before_cursor_execute")
        def before_cursor_execute(
            conn, cursor, statement, parameters, context, executemany
        ):
            conn.info.setdefault("query_start_time", []).append(time.time())
            self.logger.debug("Start Query: \n%s\n" % statement)

        @event.listens_for(Engine, "after_cursor_execute")
        def after_cursor_execute(
            conn, cursor, statement, parameters, context, executemany
        ):
            total = time.time() - conn.info["query_start_time"].pop(-1)
            self.logger.debug("Query Complete!")
            self.logger.debug("Total Time: %f" % total)

    def log(self, *args, **kwargs):
        """Logs a message to stderr."""
        kwargs.setdefault("file", sys.stderr)
        prefix = kwargs.pop("prefix", "")
        for msg in args:
            message = prefix + msg
            if self.debug:
                message = message.replace("\r", "")
                kwargs["nl"] = True
            click.echo(message, **kwargs)

    def confirm(self, message, **kwargs):
        if not self.force_yes:
            if not click.confirm(message, **kwargs):
                raise click.Abort()

    def update_options(self, **kwargs):
        self.__dict__.update(kwargs)
        if self.debug and not self.verbose:
            self.verbose = True

    def handle_error(self):
        exc_type, exc_value, tb = sys.exc_info()
        if isinstance(exc_value, (click.ClickException, click.Abort)) or self.debug:
            reraise(exc_type, exc_value, tb.tb_next)
        else:
            sys.stderr.write("\nError: %s\n" % exc_value)
            sys.exit(1)


def make_pass_decorator(context_klass, ensure=True):
    def decorator(f):
        @click.pass_context
        def new_func(*args, **kwargs):
            ctx = args[0]
            if ensure:
                obj = ctx.ensure_object(context_klass)
            else:
                obj = ctx.find_object(context_klass)
            try:
                return ctx.invoke(f, obj, *args[1:], **kwargs)
            except Exception:
                obj.handle_error()

        return update_wrapper(new_func, f)

    return decorator


re_color_codes = re.compile(r"\033\[(\d;)?\d+m")


class AnsiColorFormatter(logging.Formatter):

    LEVELS = {
        "WARNING": red(" WARN"),
        "INFO": blue(" INFO"),
        "DEBUG": blue("DEBUG"),
        "CRITICAL": magenta(" CRIT"),
        "ERROR": red("ERROR"),
    }

    def __init__(self, msgfmt=None, datefmt=None):
        logging.Formatter.__init__(self, None, "%H:%M:%S")

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

        s = "%(timestamp)s %(levelname)s %(name)s " % {
            "timestamp": green("%s,%03d" % (asctime, record.msecs), bold=True),
            "levelname": self.LEVELS[record.levelname],
            "name": name,
        }

        if "\n" in message:
            indent_length = len(re_color_codes.sub("", s))
            message = message.replace("\n", "\n" + " " * indent_length)

        s += message
        return s
