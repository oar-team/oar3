# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals
import click

from .. import VERSION
from .operations import copy_db
from .helpers import pass_context


CONTEXT_SETTINGS = dict(auto_envvar_prefix='oar',
                        help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS, chain=True)
@click.version_option(version=VERSION)
@click.option('--force-yes', is_flag=True, default=False,
              help="Never prompts for user intervention")
@click.option('--archive-db-suffix', default="archive")
@click.option('--debug', is_flag=True, default=False, help="Enable Debug.")
@pass_context
def cli(ctx, debug, archive_db_suffix, force_yes):
    """Archive OAR database."""
    ctx.debug = debug
    ctx.archive_db_suffix = archive_db_suffix
    ctx.force_yes = force_yes
    ctx.print_db_info()


@cli.command()
@click.option('--chunk', type=int, default=10000, help="Chunk size")
@pass_context
def sync(ctx, chunk):
    ctx.chunk = chunk
    ctx.confirm("Continue to copy data to the archive database?")
    copy_db(ctx)
    ctx.log("up-to-date.")


@cli.command('purge-resources')
@click.option('-s', '--status', multiple=True, default=["Dead"],
              help="Only resources with the giving status")
@pass_context
def purge_resources(ctx, table):
    """ Purge old resources and all attached jobs. """
    ctx.confirm("Continue to purge resources?")
    copy_db(ctx)
    ctx.log("up-to-date.")


@cli.command('purge-jobs')
@click.option('-s', '--status', multiple=True, default=['Terminated'],
              help="Only jobs with the giving status")
@click.option('--older-than', type=str, default=None)
@pass_context
def purge_jobs(ctx, status, older_than):
    import pdb; pdb.set_trace()
    ctx.confirm("Continue to purge resources?")
    copy_db(ctx)
    ctx.log("up-to-date.")
