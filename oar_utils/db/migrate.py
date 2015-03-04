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
@click.option('--chunk', type=int, default=10000, help="Chunk size")
@click.option('--current-db-url', prompt=DATABASE_PROMPT,
              default=get_default_database_url(),
              help='the url for your current OAR database.')
@click.option('--new-db-url', prompt="new OAR database URL",
              help='the url for your new OAR database.')
@click.option('--debug', is_flag=True, default=False, help="Enable Debug.")
@pass_context
def cli(ctx, force_yes, chunk, current_db_url, new_db_url, debug):
    """Archive OAR database."""
    ctx.force_yes = force_yes
    ctx.chunk = chunk
    ctx.debug = debug
    ctx.current_db_url = current_db_url
    ctx.archive_db_url = new_db_url
    ctx.confirm("Continue to migrate your database?", default=True)
    migrate_db(ctx)
