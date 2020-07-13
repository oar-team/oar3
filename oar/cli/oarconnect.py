import os
import click

from oar.lib import config
from oar.cli.oarsub import connect_job 

from .utils import CommandReturns
                           
@click.command()
@click.argument('job_id', nargs=1, required=True,  type=int)
def cli(job_id):

    cmd_ret = CommandReturns()
    binpath = ''
    if 'OARDIR' in os.environ:
        binpath = os.environ['OARDIR'] + '/'
    else:
        cmd_ret.error('OARDIR environment variable is not defined.', 0, 1)
        cmd_ret.exit()

    openssh_cmd = config['OPENSSH_CMD']
    ssh_timeout = int(config['OAR_SSH_CONNECTION_TIMEOUT'])

    exit(connect_job(job_id, 0, openssh_cmd, cmd_ret));
