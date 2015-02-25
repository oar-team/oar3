# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals
import click

from oar.lib import config
from .. import VERSION
from .operations import sync_db, purge_db
from .helpers import pass_context


CONTEXT_SETTINGS = dict(auto_envvar_prefix='oar',
                        help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS, chain=True)
@click.version_option(version=VERSION)
@click.option('-y', '--force-yes', is_flag=True, default=False,
              help="Never prompts for user intervention")
@click.option('--db-suffix', default="archive", help="Archive database suffix")
@click.option('--debug', is_flag=True, default=False, help="Enable Debug.")
@pass_context
def cli(ctx, force_yes, db_suffix, debug):
    """Archive OAR database."""
    ctx.force_yes = force_yes
    ctx.archive_db_suffix = db_suffix
    ctx.debug = debug
    ctx.print_db_info()
    config["LOG_FORMAT"] = '[%(levelname)s]: %(message)s'


@cli.command()
@click.option('--chunk', type=int, default=10000, help="Chunk size")
@click.option('--ignore-jobs', default=["^Terminated", "^Error"],
              multiple=True)
@pass_context
def sync(ctx, chunk, ignore_jobs):
    """ Send all resources and finished jobs to archive database."""
    ctx.chunk = chunk
    ctx.ignore_jobs = ignore_jobs
    ctx.confirm("Continue to copy old resources and jobs to the archive "
                "database?")
    sync_db(ctx)


@cli.command('purge')
@click.option('--ignore-jobs', default=["^Terminated", "^Error"],
              multiple=True)
@click.option('--jobs-older-than', default="1Y")
@click.option('--ignore-resources', default=["^Dead"], multiple=True)
@pass_context
def purge(ctx, ignore_resources, ignore_jobs, jobs_older_than):
    """ Purge old resources and old jobs from your current database."""
    ctx._cache = {}  ## reset filters in case of chained commands
    ctx.ignore_resources = ignore_resources
    ctx.ignore_jobs = ignore_jobs
    ctx.jobs_older_than = jobs_older_than
    msg = "Continue to purge old resources and jobs "\
          "from your current database?"
    ctx.confirm(click.style(msg.upper(), underline=True, bold=True))
    purge_db(ctx)
