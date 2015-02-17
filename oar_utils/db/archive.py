# -*- coding: utf-8 -*-
from __future__ import division
import sys
import click

from oar.lib import config
from oar.lib.compat import reraise

from oar.utils import VERSION


from .helpers import log
from .operations import copy_db


CONTEXT_SETTINGS = dict(auto_envvar_prefix='oar',
                        help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=VERSION)
@click.option('--script', is_flag=True, default=False,
              help="Never prompts for user intervention")
@click.option('--archive-db-suffix', default="archive")
@click.option('--debug', is_flag=True, default=False, help="Enable Debug.")
def cli(debug, archive_db_suffix, script):
    """Archive OAR database."""
    db_url = config.get_sqlalchemy_uri()
    archive_db_url = "%s_%s" % (db_url, archive_db_suffix)
    log("\nOAR database: %s" % db_url)
    log("OAR archive database: %s\n" % archive_db_url)

    if not script:
        if not click.confirm("continue to archive OAR database"):
            raise click.Abort()
    try:
        copy_db(archive_db_url, chunk_size=10000)
        log("up-to-date.")
    except Exception as e:
        if not debug:
            sys.stderr.write(u"\nError: %s\n" % e)
            sys.exit(1)
        else:
            exc_type, exc_value, tb = sys.exc_info()
            reraise(exc_type, exc_value, tb.tb_next)


def main(args=sys.argv[1:]):
    cli(args)
