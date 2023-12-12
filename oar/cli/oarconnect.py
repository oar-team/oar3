import click
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.cli.oarsub import connect_job
from oar.lib.globals import init_oar

from .utils import CommandReturns


@click.command()
@click.argument("job_id", nargs=1, required=True, type=int)
@click.pass_context
def cli(ctx, job_id):
    """Connect to a reservation in Running state."""

    ctx = click.get_current_context()
    if ctx.obj:
        (session, config) = ctx.obj
    else:
        config, engine = init_oar()

        session_factory = sessionmaker(bind=engine)
        scoped = scoped_session(session_factory)
        session = scoped()

    cmd_ret = CommandReturns()
    openssh_cmd = config["OPENSSH_CMD"]
    exit(connect_job(session, config, job_id, 0, openssh_cmd, cmd_ret))
