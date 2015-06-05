# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals
import click

from tabulate import tabulate
from oar.lib import config

from .. import VERSION
from .operations import archive_db, purge_db, inspect_db
from .helpers import pass_context


def get_default_database_url():
    try:
        return config.get_sqlalchemy_uri()
    except:
        pass


def get_default_archive_database_url():
    default_database_url = get_default_database_url()
    if default_database_url:
        return get_default_database_url() + "_archive"


CONTEXT_SETTINGS = dict(auto_envvar_prefix='oar',
                        help_option_names=['-h', '--help'])


DATABASE_PROMPT = """Please enter the url for your database.

For example:
PostgreSQL: postgresql://scott:tiger@localhost/foo
MySQL: mysql://scott:tiger@localhost/bar

For more information, see : http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls

OAR database URL"""  # noqa


@click.group(context_settings=CONTEXT_SETTINGS, chain=True)
@click.version_option(version=VERSION)
@click.option('-y', '--force-yes', is_flag=True, default=False,
              help="Never prompts for user intervention")
@click.option('--debug', is_flag=True, default=False, help="Enable Debug.")
@pass_context
def cli(ctx, force_yes, debug):
    """Archive OAR database."""
    ctx.force_yes = force_yes
    ctx.debug = debug
    ctx.configure_log()


@cli.command()
@click.option('--chunk', type=int, default=10000, show_default=True,
              help="Chunk size")
@click.option('--ignore-jobs', default=["^Terminated", "^Error"],
              show_default=True, multiple=True, help='Ignore job state')
@click.option('--current-db-url', prompt=DATABASE_PROMPT,
              default=get_default_database_url(), show_default=True,
              help='The url for your current OAR database.')
@click.option('--archive-db-url', prompt="OAR archive database URL",
              default=get_default_archive_database_url(), show_default=True,
              help='The url for your archive OAR database.')
@click.option('--disable-pagination', is_flag=True, default=False,
              help='Enable query pagination during copy.')
@pass_context
def sync(ctx, chunk, ignore_jobs, current_db_url, archive_db_url,
         disable_pagination):
    """ Send all resources and finished jobs to archive database."""
    ctx.chunk = chunk
    ctx.ignore_jobs = ignore_jobs
    ctx.current_db_url = current_db_url
    ctx.archive_db_url = archive_db_url
    ctx.disable_pagination = disable_pagination
    ctx.confirm("Continue to copy old resources and jobs to the archive "
                "database?", default=True)
    archive_db(ctx)


@cli.command()
@click.option('--ignore-jobs', default=["^Terminated", "^Error"],
              show_default=True, multiple=True, help='Ignore job state')
@click.option('--max-job-id', type=int, default=None, show_default=True,
              help='Purge only jobs lower than this id')
@click.option('--ignore-resources', default=["^Dead"], show_default=True,
              help='Ignore resource state', multiple=True)
@click.option('--current-db-url', prompt=DATABASE_PROMPT,
              default=get_default_database_url(), show_default=True,
              help='The url for your current OAR database.')
@pass_context
def purge(ctx, ignore_resources, ignore_jobs, max_job_id, current_db_url):
    """ Purge old resources and old jobs from your current database."""
    ctx._cache = {}  # reset filters in case of chained commands
    ctx.ignore_resources = ignore_resources
    ctx.ignore_jobs = ignore_jobs
    ctx.max_job_id = max_job_id
    ctx.current_db_url = current_db_url
    msg = "Continue to purge old resources and jobs "\
          "from your current database?"
    ctx.confirm(click.style(msg.upper(), underline=True, bold=True))
    if purge_db(ctx) == 0:
        ctx.log("\nNothing to do.")


@cli.command()
@click.option('--current-db-url', prompt=DATABASE_PROMPT,
              default=get_default_database_url(),
              help='The url for your current OAR database.')
@click.option('--archive-db-url', prompt="OAR archive database URL",
              default=get_default_archive_database_url(),
              help='The url for your archive OAR database.')
@pass_context
def inspect(ctx, current_db_url, archive_db_url):
    """ Analyze all databases."""
    ctx.current_db_url = current_db_url
    ctx.archive_db_url = archive_db_url
    rows, headers = inspect_db(ctx)
    click.echo(tabulate(rows, headers=headers))
