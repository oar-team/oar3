import os

import click
from sqlalchemy.orm import scoped_session, sessionmaker

from oar import VERSION
from oar.lib.database import EngineConnector
from oar.lib.globals import init_oar
from oar.lib.models import Model
from oar.lib.queue import (
    change_queue,
    create_queue,
    get_all_queue_by_priority,
    remove_queue,
    start_all_queues,
    start_queue,
    stop_all_queues,
    stop_queue,
)

from .utils import CommandReturns

click.disable_unicode_literals_warning = True


def oarqueue(
    session,
    config,
    list_all,
    enable,
    disable,
    enable_all,
    disable_all,
    add,
    change,
    remove,
    version,
):
    cmd_ret = CommandReturns(cli)
    if version:
        cmd_ret.print_("OAR version : " + VERSION)
        return cmd_ret

    user = os.environ["USER"]
    if "OARDO_USER" in os.environ:
        user = os.environ["OARDO_USER"]

    if not (user == "oar" or user == "root"):
        comment = "You must be oar or root"
        cmd_ret.error(comment, 1, 8)
        return cmd_ret

    if list_all:
        for queue in get_all_queue_by_priority(session):
            print(queue.name)
            print("priority = {}".format(queue.priority))
            print("policy = {}".format(queue.scheduler_policy))
            print("state = {}".format(queue.state))
        return cmd_ret

    if enable:
        start_queue(session, enable)
        return cmd_ret

    if disable:
        stop_queue(session, disable)
        return cmd_ret

    if enable_all:
        start_all_queues(
            session,
        )
        return cmd_ret

    if disable_all:
        stop_all_queues(
            session,
        )
        return cmd_ret

    if add:
        name, priority, policy = add.split(",")
        if priority:
            priority = int(priority)
        create_queue(session, name, priority, policy)
        return cmd_ret

    if change:
        name, priority, policy = change.split(",")
        if priority:
            priority = int(priority)
        change_queue(session, name, priority, policy)
        return cmd_ret

    if remove:
        remove_queue(session, remove)
        return cmd_ret

    return cmd_ret


@click.command()
@click.option(
    "-l", "--list", is_flag=True, help="list all queues (default if no other option)"
)
@click.option(
    "-e", "--enable", type=click.STRING, help="enable a queue, given its name"
)
@click.option(
    "-d", "--disable", type=click.STRING, help="disable a queue, given its name"
)
@click.option("-E", "--enable-all", is_flag=True, help="enable all queues")
@click.option("-D", "--disable-all", is_flag=True, help="disable all queues")
@click.option(
    "--add",
    type=click.STRING,
    help='add a new queue (eg: "q1,3,fifo" or "q2,4,fairsharing")',
)
@click.option(
    "--change",
    type=click.STRING,
    help="change the priority/policy of a queue, given its name",
)
@click.option("--remove", type=click.STRING, help="remove a queue, given its name")
@click.option("-V", "--version", is_flag=True, help="Print OAR version.")
def cli(list, enable, disable, enable_all, disable_all, add, change, remove, version):
    """List, create or change OAR's scheduler queues."""

    ctx = click.get_current_context()
    if ctx.obj:
        (session, config) = ctx.obj
    else:
        config, engine, log = init_oar()
        session_factory = sessionmaker(bind=engine)
        scoped = scoped_session(session_factory)
        session = scoped()

    list_all = list
    if not (
        list_all
        or enable
        or disable
        or enable_all
        or disable_all
        or add
        or change
        or remove
        or version
    ):
        list_all = True

    cmd_ret = oarqueue(
        session,
        config,
        list_all,
        enable,
        disable,
        enable_all,
        disable_all,
        add,
        change,
        remove,
        version,
    )
    cmd_ret.exit()
