# -*- coding: utf-8 -*-
import click
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools as tools
from oar import VERSION
from oar.lib.accounting import (
    check_accounting_update,
    delete_accounting_windows_before,
    delete_all_from_accounting,
)
from oar.lib.database import EngineConnector
from oar.lib.globals import init_oar
from oar.lib.logging import get_logger
from oar.lib.models import Model

from .utils import CommandReturns

click.disable_unicode_literals_warning = True


@click.command()
@click.option(
    "--reinitialize", is_flag=True, help="Delete everything in the accounting table."
)
@click.option(
    "--delete-before",
    type=int,
    help="Delete every records the number of given seconds ago.",
)
@click.option("-V", "--version", is_flag=True, help="Print OAR version number.")
@click.pass_context
def cli(reinitialize, delete_before, version):
    """Feed accounting table to make usage statistics."""

    ctx = click.get_current_context()
    if ctx.obj:
        session = ctx.obj
    else:
        config, db, log = init_oar()
        engine = EngineConnector(db).get_engine()

        Model.metadata.drop_all(bind=engine)

        session_factory = sessionmaker(bind=engine)
        scoped = scoped_session(session_factory)
        # TODO
        session = scoped()

    # Default window size
    window_size = 86400

    delete_windows_before = delete_before

    cmd_ret = CommandReturns(cli)

    if "ACCOUNTING_WINDOW" in config:
        window_size = config["ACCOUNTING_WINDOW"]

    if version:
        cmd_ret.print_("OAR version : " + VERSION)
        cmd_ret.exit()

    if reinitialize:
        print("Deleting all records from the acounting table...")
        delete_all_from_accounting()
    elif delete_before:
        print("Deleting records older than $Delete_windows_before seconds ago...")
        delete_windows_before = tools.get_date() - delete_windows_before

        delete_accounting_windows_before(delete_windows_before)
    else:
        check_accounting_update(window_size)
