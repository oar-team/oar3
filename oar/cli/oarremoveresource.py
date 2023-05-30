import click
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.lib.globals import init_oar
from oar.lib.resource_handling import remove_resource

from .utils import CommandReturns

click.disable_unicode_literals_warning = True


@click.command()
@click.argument("resource", nargs=-1, required=True, type=int)
def cli(resource):
    """Usage: oarremoveresource resource_id(s)
    WARNING : this command removes all records in the database
    about "resource_id(s)".

    So you will loose this resource history and jobs executed on this one
    """
    ctx = click.get_current_context()
    if ctx.obj:
        (session, config) = ctx.obj
    else:
        config, engine, log = init_oar()
        session_factory = sessionmaker(bind=engine)
        scoped = scoped_session(session_factory)
        session = scoped()

    resource_ids = resource
    cmd_ret = CommandReturns(cli)
    if resource_ids:
        for resource_id in resource_ids:
            error, error_msg = remove_resource(session, resource_id)
            cmd_ret.error(error_msg, error, error)
    cmd_ret.exit()
