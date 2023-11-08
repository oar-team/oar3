# -*- coding: utf-8 -*-
import click

from oar.lib import Database, config
from oar.lib.utils import cached_property

from ..helpers import (
    DATABASE_URL_PROMPT,
    Context,
    default_database_url,
    load_configuration_file,
    make_pass_decorator,
)
from ..operations import migrate_db

click.disable_unicode_literals_warning = True

CONTEXT_SETTINGS = dict(
    auto_envvar_prefix="oar_migrate", help_option_names=["-h", "--help"]
)


class MigrationContext(Context):
    @cached_property
    def new_db(self):
        return Database(uri=self.new_db_url)

    @cached_property
    def current_db(self):
        from oar.lib import db

        db._cache["uri"] = self.current_db_url
        return db

    @cached_property
    def new_db_name(self):
        return self.new_db.engine.url.database

    @cached_property
    def current_db_name(self):
        return self.current_db.engine.url.database

    @cached_property
    def current_models(self):
        """Return a namespace with all mapping classes"""
        from oar.lib.models import all_models  # avoid a circular import

        return dict(all_models())

    @cached_property
    def current_tables(self):
        """Return a namespace with all tables classes"""
        self.current_db.reflect()
        sorted_tables = self.current_db.metadata.sorted_tables
        return dict((t.name, t) for t in sorted_tables)


pass_context = make_pass_decorator(MigrationContext)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    "-c",
    "--conf",
    callback=load_configuration_file,
    type=click.Path(writable=False, readable=False),
    help="Use a different OAR configuration file.",
    required=False,
    default=config.DEFAULT_CONFIG_FILE,
    show_default=True,
)
@click.option(
    "--data-only",
    is_flag=True,
    default=False,
    help="Migrates only the data, not the schema",
)
@click.option(
    "--schema-only",
    is_flag=True,
    default=False,
    help="Migrates only the schema, no data",
)
@click.option(
    "--current-db-url",
    prompt=DATABASE_URL_PROMPT,
    default=default_database_url,
    help="the url for your current OAR database.",
)
@click.option(
    "--new-db-url",
    prompt="new OAR database URL",
    help="the url for your new OAR database.",
)
@click.option(
    "--chunk",
    type=int,
    default=100000,
    show_default=True,
    help="Defines the chunk size",
)
@click.option(
    "--pg-copy/--no-pg-copy",
    is_flag=True,
    default=True,
    help="Use postgresql COPY clause to make batch inserts faster",
)
@click.option(
    "--pg-copy-binary/--pg-copy-csv",
    is_flag=True,
    default=True,
    help="Use postgresql COPY with binary-format. "
    "It is somewhat faster than the text and CSV formats, but "
    "a binary-format file is less portable",
)
@click.option(
    "-y",
    "--force-yes",
    is_flag=True,
    default=False,
    help="Never prompts for user intervention",
)
@click.version_option()
@click.option("--verbose", is_flag=True, default=False, help="Enables verbose output.")
@click.option("--debug", is_flag=True, default=False, help="Enables debug mode.")
@pass_context
def cli(ctx, **kwargs):
    """Archive OAR database."""
    ctx.update_options(**kwargs)
    ctx.configure_log()
    ctx.confirm("Continue to migrate your database?", default=True)
    migrate_db(ctx)
