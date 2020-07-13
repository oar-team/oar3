import click
import os

from oar import VERSION

from .utils import CommandReturns
from oar.lib import config

import oar.lib.tools as tools

click.disable_unicode_literals_warning = True

def oarnotify(tag, version):
    cmd_ret = CommandReturns(cli)
    if version:
        cmd_ret.print_('OAR version : ' + VERSION)
        return cmd_ret

    user = os.environ['USER']
    if 'OARDO_USER' in os.environ:
        user = os.environ['OARDO_USER']
        
    if not (user=='oar' or user=='root'):
        comment = "You must be oar or root"
        cmd_ret.error(comment, 1, 8)
        return cmd_ret

    tools.notify_almighty(tag)

    return cmd_ret

@click.command()
@click.argument('tag', default='Term', type=click.STRING)
@click.option('-V', '--version', is_flag=True, help='Print OAR version.')
def cli(tag, version):
    """Send a message tag to OAR's Almighty"""
    cmd_ret = oarnotify(tag,version)
    cmd_ret.exit()

