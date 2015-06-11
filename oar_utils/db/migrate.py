# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals
import click

from oar.lib import config

from .. import VERSION
from .operations import migrate_db
from .helpers import pass_context


def get_default_database_url():
    try:
        return config.get_sqlalchemy_uri()
    except:
        pass


CONTEXT_SETTINGS = dict(auto_envvar_prefix='oar',
                        help_option_names=['-h', '--help'])


DATABASE_PROMPT = """Please enter the url for your database.

For example:
PostgreSQL: postgresql://scott:tiger@localhost/foo
MySQL: mysql://scott:tiger@localhost/bar

OAR database URL"""


@click.command(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=VERSION)
@click.option('-y', '--force-yes', is_flag=True, default=False,
              help="Never prompts for user intervention")
@click.option('--chunk', type=int, default=100000, help="Chunk size")
@click.option('--data-only', is_flag=True, default=False)
@click.option('--schema-only', is_flag=True, default=False)
@click.option('--current-db-url', prompt=DATABASE_PROMPT,
              default=get_default_database_url(),
              help='the url for your current OAR database.')
@click.option('--new-db-url', prompt="new OAR database URL",
              help='the url for your new OAR database.')
@click.option('--disable-pagination', is_flag=True, default=False,
              help='Split the query in small SQL queries during copy.')
@click.option('--pg-copy', is_flag=True, default=False,
              help='Use postgresql COPY clause to make the data transfert '
                   'faster')
@click.option('--debug', is_flag=True, default=False, help="Enable Debug.")
@pass_context
def cli(ctx, force_yes, chunk, data_only, schema_only, current_db_url,
        new_db_url, disable_pagination, pg_copy, debug):
    """Archive OAR database."""
    ctx.force_yes = force_yes
    ctx.chunk = chunk
    ctx.data_only = data_only
    ctx.schema_only = schema_only
    ctx.current_db_url = current_db_url
    ctx.archive_db_url = new_db_url
    ctx.disable_pagination = disable_pagination
    ctx.enable_pg_copy = pg_copy
    ctx.debug = debug
    ctx.confirm("Continue to migrate your database?", default=True)
    ctx.configure_log()
    migrate_db(ctx)
