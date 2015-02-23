# -*- coding: utf-8 -*-
from __future__ import division
import click

from .. import VERSION
from .operations import copy_db
from .helpers import pass_context


CONTEXT_SETTINGS = dict(auto_envvar_prefix='oar',
                        help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS, chain=True)
@click.version_option(version=VERSION)
@click.option('--script', is_flag=True, default=False,
              help="Never prompts for user intervention")
@click.option('--archive-db-suffix', default="archive")
@click.option('--debug', is_flag=True, default=False, help="Enable Debug.")
@pass_context
def cli(ctx, debug, archive_db_suffix, script):
    """Archive OAR database."""
    ctx.debug = debug
    ctx.archive_db_suffix = archive_db_suffix
    ctx.script = script
    ctx.print_db_info()


@cli.command()
@click.option('--chunk', type=int, default=10000, help="Chunk size")
@pass_context
def sync(ctx, chunk):
    ctx.chunk = chunk
    ctx.confirm("Continue to archive your database?")
    copy_db(ctx)
    ctx.log("up-to-date.")
