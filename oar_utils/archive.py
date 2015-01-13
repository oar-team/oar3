# -*- coding: utf-8 -*-
import sys
import click

from oar.lib import Database
from oar.utils import VERSION


CONTEXT_SETTINGS = dict(auto_envvar_prefix='oar',
                        help_option_names=['-h', '--help'])


DATABASE_PROMPT = """Please enter the url for your database.

For example:
PostgreSQL: postgresql://scott:tiger@localhost/mydatabase
MySQL: mysql://scott:tiger@localhost/foo

OAR database URL"""


class Context(object):

    def __init__(self):
        self.version = VERSION
        self.db_url = None
        self.db_archive_url = None
        self.offline_mode = False

    def sync(self):
        self.db = Database(uri=self.db_url)
        self.db_archive = Database(uri=self.db_archive_url)
        pass

    def log(self, msg, *args):
        """Logs a message to stderr."""
        if args:
            msg %= args
        click.echo(msg, file=sys.stderr)

    def vlog(self, msg, *args):
        """Logs a message to stderr only if verbose is enabled."""
        if self.verbose:
            self.log(msg, *args)


pass_context = click.make_pass_decorator(Context, ensure=True)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option('--sql', default=1, help='Dump the SQL (offline mode)')
@click.option('--db-url', prompt=DATABASE_PROMPT,
              help='the url for your OAR database.')
@click.option('--db-archive-url', prompt="OAR archive database URL",
              help='the url for your archive database.')
@pass_context
def cli(ctx, db_url, db_archive_url, sql):
    """Archive OAR database."""
    ctx.db_url = db_url
    ctx.db_archive_url = db_archive_url
    ctx.offline_mode = sql
    ctx.sync()


def main(args=sys.argv[1:]):
    try:
        cli(args)
    except Exception as e:
        sys.stderr.write(u"\nError: %s\n" % e)
        sys.exit(1)
