import click
import os

from oar import VERSION

from .utils import CommandReturns
from oar.lib import config
from oar.lib.queue import (get_all_queue_by_priority, stop_queue, start_queue,
                           stop_all_queues, start_all_queues, create_queue, remove_queue,
                           change_queue)
click.disable_unicode_literals_warning = True

def oarqueue(list_all, enable, disable, enable_all, disable_all, add, change, remove,  version):
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
        
    if list_all:
        for queue in get_all_queue_by_priority():
            print(queue.name)
            print("priority = {}".format(queue.priority))
            print("policy = {}".format(queue.scheduler_policy))
            print("state = {}".format(queue.state))
        return cmd_ret

    if enable:
        start_queue(enable)
        return cmd_ret

    if disable:
        stop_queue(disable)
        return cmd_ret

    if enable_all:
        start_all_queues()
        return cmd_ret

    if disable_all:
        stop_all_queues()
        return cmd_ret

    if add:
        name, priority, policy = add.split(',')
        if priority:
            priority = int(priority)
        create_queue(name, priority, policy)
        return cmd_ret
    
    if change:
        name, priority, policy = change.split(',')
        if priority:
            priority = int(priority)
        change_queue(name, priority, policy)
        return cmd_ret

    if remove:
        remove_queue(remove)
        return cmd_ret
    
    return cmd_ret

@click.command()
@click.option('-l', '--list', is_flag=True, help='list all queues (default if no other option)')
@click.option('-e', '--enable', type=click.STRING, help='enable a queue, given its name')
@click.option('-d', '--disable', type=click.STRING, help='disable a queue, given its name')
@click.option('-E', '--enable-all', is_flag=True, help='enable all queues')
@click.option('-D', '--disable-all', is_flag=True, help='disable all queues')              
@click.option('--add', type=click.STRING, help='add a new queue (eg: "q1,3,fifo" or "q2,4,fairsharing")')
@click.option('--change', type=click.STRING,
              help='change the priority/policy of a queue, given its name')
@click.option('--remove', type=click.STRING, help='remove a queue, given its name')
@click.option('-V', '--version', is_flag=True, help='Print OAR version.')
def cli(list, enable, disable, enable_all, disable_all, add, change, remove, version):
    """List, create or change OAR's scheduler queues."""
    list_all = list
    if not ( list_all or enable or disable or enable_all or disable_all or
             add or change or remove or version):
        list_all = True

    cmd_ret = oarqueue(list_all, enable, disable, enable_all, disable_all, add, change, remove, version)
    cmd_ret.exit()

