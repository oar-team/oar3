# -*- coding: utf-8 -*-
from functools import reduce

import click
from sqlalchemy import and_, func, not_
from tabulate import tabulate

from oar.lib import Database, config
from oar.lib.utils import cached_property

from ..helpers import (
    DATABASE_URL_PROMPT,
    Context,
    default_database_url,
    load_configuration_file,
    make_pass_decorator,
)
from ..operations import archive_db, inspect_db, purge_db

click.disable_unicode_literals_warning = True


CONTEXT_SETTINGS = dict(
    auto_envvar_prefix="oar_migrate", help_option_names=["-h", "--help"]
)


class ArchiveContext(Context):
    @cached_property
    def archive_db(self):
        return Database(uri=self.archive_db_url)

    @cached_property
    def current_db(self):
        from oar.lib import db

        db._cache["uri"] = self.current_db_url
        return db

    @cached_property
    def archive_db_name(self):
        return self.archive_db.engine.url.database

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

    @cached_property
    def ignored_resources_criteria(self):
        criteria = []
        exclude = []
        include = []
        if "all" in self.ignore_resources:
            return
        for raw_state in self.ignore_resources:
            if raw_state.startswith("^"):
                exclude.append(raw_state.lstrip("^"))
            else:
                include.append(raw_state)
        if exclude:
            criteria.append(self.current_models["Resource"].state.notin_(exclude))
        if include:
            criteria.append(self.current_models["Resource"].state.in_(include))
        return reduce(and_, criteria)

    @cached_property
    def ignored_jobs_criteria(self):
        criteria = []
        exclude = []
        include = []
        for raw_state in self.ignore_jobs:
            if raw_state.startswith("^"):
                exclude.append(raw_state.lstrip("^"))
            else:
                include.append(raw_state)
        if exclude:
            criteria.append(self.current_models["Job"].state.notin_(exclude))
        if include:
            criteria.append(self.current_models["Job"].state.in_(include))
        if criteria:
            return reduce(and_, criteria)

    @cached_property
    def max_job_to_sync(self):
        model = self.current_models["Job"]
        acceptable_max_job_id = None
        if self.ignored_jobs_criteria is not None:
            acceptable_max_job_id = (
                self.current_db.query(func.min(model.id))
                .filter(self.ignored_jobs_criteria)
                .scalar()
            )
        if acceptable_max_job_id is None:
            if self.max_job_id is None:
                # returns the real max job id
                return self.current_db.query(func.max(model.id)).scalar() or 0
            else:
                return self.max_job_id
        else:
            if self.max_job_id is not None:
                if self.max_job_id < acceptable_max_job_id:
                    return self.max_job_id
            return acceptable_max_job_id

    @cached_property
    def max_moldable_job_to_sync(self):
        moldable_model = self.current_models["MoldableJobDescription"]
        criteria = moldable_model.job_id < self.max_job_to_sync
        job_model = self.current_models["Job"]
        query = (
            self.current_db.query(func.max(moldable_model.id))
            .join(job_model, moldable_model.job_id == job_model.id)
            .filter(criteria)
        )
        return query.scalar() or 0

    @cached_property
    def resources_to_purge(self):
        if self.ignored_resources_criteria is None:
            return []
        assigned_model = self.current_models["AssignedResource"]
        resource_model = self.current_models["Resource"]
        max_moldable = self.max_moldable_job_to_sync
        query = self.current_db.query(resource_model.id)
        excludes = (
            query.filter(not_(self.ignored_resources_criteria))
            .join(assigned_model, resource_model.id == assigned_model.resource_id)
            .filter(assigned_model.moldable_id >= max_moldable)
            .group_by(resource_model.id)
            .all()
        )
        excludes_set = set([resource_id for resource_id, in excludes])
        resources = query.filter(not_(self.ignored_resources_criteria)).all()
        resources_set = set([resource_id for resource_id, in resources])
        return list(resources_set - excludes_set)


pass_context = make_pass_decorator(ArchiveContext)


def default_archive_database_url():
    url = default_database_url()
    if url:
        return url + "_archive"


CONTEXT_SETTINGS = dict(
    auto_envvar_prefix="oar_archive", help_option_names=["-h", "--help"]
)


@click.group(context_settings=CONTEXT_SETTINGS, chain=False)
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


@cli.command()
@click.option("--chunk", type=int, default=100000, show_default=True, help="Chunk size")
@click.option(
    "--ignore-jobs",
    default=["^Terminated", "^Error"],
    show_default=True,
    multiple=True,
    help="Ignore job state",
)
@click.option(
    "--current-db-url",
    prompt=DATABASE_URL_PROMPT,
    default=default_archive_database_url,
    help="The url for your current OAR database.",
)
@click.option(
    "--archive-db-url",
    prompt="OAR archive database URL",
    default=default_archive_database_url,
    help="The url for your archive OAR database.",
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
@pass_context
def sync(ctx, **kwargs):
    """Send all resources and finished jobs to archive database."""
    ctx.update_options(**kwargs)
    ctx.configure_log()
    ctx.confirm(
        "Continue to copy old resources and jobs to the archive " "database?",
        default=True,
    )
    archive_db(ctx)


@cli.command()
@click.option(
    "--ignore-jobs",
    default=["^Terminated", "^Error"],
    show_default=True,
    multiple=True,
    help="Ignore job state",
)
@click.option(
    "--max-job-id",
    type=int,
    default=None,
    show_default=True,
    help="Purge only jobs lower than this id",
)
@click.option(
    "--ignore-resources",
    default=["^Dead"],
    show_default=True,
    help="Ignore resource state",
    multiple=True,
)
@click.option(
    "--current-db-url",
    prompt=DATABASE_URL_PROMPT,
    default=default_database_url,
    help="The url for your current OAR database.",
)
@pass_context
def purge(ctx, **kwargs):
    """Purge old resources and old jobs from your current database."""
    ctx.update_options(**kwargs)
    msg = "Continue to purge old resources and jobs " "from your current database?"
    ctx.confirm(click.style(msg.upper(), underline=True, bold=True))
    if purge_db(ctx) == 0:
        ctx.log("\nNothing to do.")


@cli.command()
@click.option(
    "--current-db-url",
    prompt=DATABASE_URL_PROMPT,
    default=default_database_url,
    help="The url for your current OAR database.",
)
@click.option(
    "--archive-db-url",
    prompt="OAR archive database URL",
    default=default_archive_database_url,
    help="The url for your archive OAR database.",
)
@pass_context
def inspect(ctx, **kwargs):
    """Analyze all databases."""
    ctx.update_options(**kwargs)
    rows, headers = inspect_db(ctx)
    click.echo(tabulate(rows, headers=headers))
