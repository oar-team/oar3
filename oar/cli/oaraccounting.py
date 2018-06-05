# -*- coding: utf-8 -*-
import click

click.disable_unicode_literals_warning = True

from oar import VERSION
from .utils import CommandReturns

from oar.lib import (config)
from oar.lib.accounting import (delete_all_from_accounting,
                                delete_accounting_windows_before,
                                check_accounting_update)
import oar.lib.tools as tools

@click.command()
@click.option('--reinitialize', is_flag=True,
              help='Delete everything in the accounting table.')
@click.option('--delete-before', type=int,
              help='Delete every records the number of given seconds ago.')
@click.option('-V', '--version', is_flag=True, help='Print OAR version number.')
def cli(reinitialize, delete_before, version):
    """Feed accounting table to make usage statistics."""
    # Default window size
    delete_windows_before = delete_before
    window_size = 86400

    cmd_ret = CommandReturns(cli)
    
    if 'ACCOUNTING_WINDOW' in config:
        window_size = config['ACCOUNTING_WINDOW']


    if version:
        cmd_ret.print_('OAR version : ' + VERSION)
        return cmd_ret

        
    if reinitialize:
        print('Deleting all records from the acounting table...')
        delete_all_from_accounting()
    elif delete_before:
        print('Deleting records older than $Delete_windows_before seconds ago...')
        delete_windows_before = tools.get_date() - delete_windows_before
        delete_accounting_windows_before(delete_windows_before)
    else:
        check_accounting_update(window_size)
