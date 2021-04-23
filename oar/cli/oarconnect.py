import click

from oar.cli.oarsub import connect_job
from oar.lib import config

from .utils import CommandReturns


@click.command()
@click.argument("job_id", nargs=1, required=True, type=int)
def cli(job_id):
    """Connect to a reservation in Running state."""
    cmd_ret = CommandReturns()
    openssh_cmd = config["OPENSSH_CMD"]
    exit(connect_job(job_id, 0, openssh_cmd, cmd_ret))
