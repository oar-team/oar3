# -*- coding: utf-8 -*-
from __future__ import division

import sys
import click


magenta = lambda x: click.style("%s" % x, fg="magenta")
yellow = lambda x: click.style("%s" % x, fg="yellow")
green = lambda x: click.style("%s" % x, fg="green")
blue = lambda x: click.style("%s" % x, fg="blue")
red = lambda x: click.style("%s" % x, fg="red")


def log(*args, **kwargs):
    """Logs a message to stderr."""
    kwargs.setdefault("file", sys.stderr)
    prefix = kwargs.pop("prefix", "")
    for msg in args:
        click.echo(prefix + msg, **kwargs)
