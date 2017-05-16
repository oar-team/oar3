# -*- coding: utf-8 -*-
from __future__ import print_function, division, absolute_import, unicode_literals

import sys
import click

def print_warning(*objs):
    print('#WARNING: ', *objs, file=sys.stderr)


def print_error(*objs):
    print('#ERROR: ', *objs, file=sys.stderr)

def print_info(*objs):
    print('#INFO: ', *objs, file=sys.stderr)

def print_error_exit(error, show_usage=True):
    """Print error message, usage, and exit with the provided error code"""
    error_code, error_msg = error
    print_error(error_msg)
    if show_usage:
        usage()
    exit(error_code)

def usage():
    '''Print usage message.'''
    ctx = click.get_current_context()
    click.echo(ctx.get_help())
