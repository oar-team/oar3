# -*- coding: utf-8 -*-
import click
click.disable_unicode_literals_warning = True

from oar.lib import config
from oar.lib import Database


from oar.lib.utils import cached_property

from ..helpers import (make_pass_decorator, Context, DATABASE_URL_PROMPT,
                       default_database_url, load_configuration_file)

from ..operations import create_db, drop_db, upgrade_db, reset_db, check_db


class ManageContext(Context):
    @cached_property
    def current_db(self):
        from oar.lib import db
        db._cache["uri"] = self.current_db_url
        return db

pass_context = make_pass_decorator(ManageContext)

CONTEXT_SETTINGS = dict(auto_envvar_prefix='oar_manage',
                        help_option_names=['-h', '--help'])

@click.group(context_settings=CONTEXT_SETTINGS, chain=False)
@click.option('-c', '--conf', callback=load_configuration_file,
              type=click.Path(writable=False, readable=False),
              help="Use a different OAR configuration file.", required=False,
              default=config.DEFAULT_CONFIG_FILE, show_default=True)
@click.option('-y', '--force-yes', is_flag=True, default=False,
              help="Never prompts for user intervention")
@click.option('--verbose', is_flag=True, default=False,
              help="Enables verbose output.")
@click.option('--debug', is_flag=True, default=False,
              help="Enables debug mode.")
@pass_context
def cli(ctx, **kwargs):
    """Manage OAR database."""
    ctx.update_options(**kwargs)

@cli.command()
@click.option('--current-db-url', prompt=DATABASE_URL_PROMPT,
              default=default_database_url,
              help='The url for your current OAR database.')
@pass_context
def create(ctx, **kwargs):
    """Create OAR database."""
    ctx.update_options(**kwargs)
    ctx.configure_log()
    create_db(ctx)

@cli.command()
@click.option('--current-db-url',
              default=default_database_url,
              help='The url for your current OAR database.')
@pass_context
def drop(ctx, **kwargs):
    """Drop OAR database."""
    ctx.update_options(**kwargs)
    ctx.configure_log()
    drop_db(ctx)

@cli.command()
@click.option('--current-db-url',
              default=default_database_url,
              help='The url for your current OAR database.')
@pass_context
def upgrade(ctx, **kwargs):
    """upgrade OAR database."""
    ctx.update_options(**kwargs)
    ctx.configure_log()
    upgrade_db(ctx)

@cli.command()
@click.option('--current-db-url',
              default=default_database_url,
              help='The url for your current OAR database.')
@pass_context
def reset(ctx, **kwargs):
    """Reset OAR database."""
    ctx.update_options(**kwargs)
    ctx.configure_log()
    reset_db(ctx)

@cli.command()
@click.option('--current-db-url',
              default=default_database_url,
              help='The url for your current OAR database.')
@pass_context
def check(ctx, **kwargs):
    """Check OAR database."""
    ctx.update_options(**kwargs)
    ctx.configure_log()
    check_db(ctx)

