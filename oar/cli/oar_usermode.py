import os
import shutil
import sys

import click
from alembic.migration import MigrationContext
from alembic.operations import Operations
from ClusterShell.NodeSet import NodeSet
from sqlalchemy import Column, Integer
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker

import oar.tools as tools
from oar.lib.globals import init_oar
from oar.lib.models import DeferredReflectionModel, Model, Queue, Resource


def db_create(nodes, nb_core, skip):
    config, engine = init_oar(no_reflect=True)

    if config["USER_MODE"] == "NO":
        print("Error: user mode not enabled,", file=sys.stderr)
        print('hint: put USER_MODE="YES" in configuration file', file=sys.stderr)
        exit(1)

    # Model.metadata.drop_all(bind=engine)
    kw = {"nullable": True}

    Model.metadata.create_all(bind=engine)

    conn = engine.connect()
    context = MigrationContext.configure(conn)

    try:
        with context.begin_transaction():
            op = Operations(context)
            # op.execute("ALTER TYPE mood ADD VALUE 'soso'")
            op.add_column("resources", Column("core", Integer, **kw))
            op.add_column("resources", Column("cpu", Integer, **kw))
    except ProgrammingError:
        # if the columns already exist we continue
        pass

    session_factory = sessionmaker(bind=engine)

    DeferredReflectionModel.prepare(engine)

    with session_factory() as session:
        Queue.create(
            session,
            name="default",
            priority=0,
            scheduler_policy="kamelot",
            state="Active",
        )
        Queue.create(
            session,
            name="admin",
            priority=100,
            scheduler_policy="kamelot",
            state="Active",
        )

        nodeset = NodeSet(nodes)
        core = 1
        if skip:
            nodeset = nodeset[1:]
        for node in nodeset:
            for _ in range(nb_core):
                Resource.create(session, network_address=node, core=core)
                core += 1
        session.commit()

    # reflect_base(Model.metadata, DeferredReflectionModel, engine)
    # DeferredReflectionModel.prepare(engine)
    engine.dispose()


@click.command()
@click.option("-c", "--create-db", is_flag=True, help="Create database")
@click.option(
    "-b",
    "--base-configfile",
    is_flag=True,
    help="Copy base configuration file ('oar_usermode.conf')",
)
@click.option(
    "-n",
    "--nodes",
    type=click.STRING,
    help="nodes to declare in database following nodeset formate (ex: node[1,6-7])",
)
@click.option(
    "-s",
    "--skip",
    is_flag=True,
    help="skip the first node from nodes (usually reserved to OAR services",
)
@click.option(
    "-o",
    "--nb-core",
    type=click.INT,
    default=os.cpu_count(),
    help="Number of cores for each node",
)
def cli(base_configfile, create_db, nodes, skip, nb_core):
    if base_configfile:
        oar_configfile = "oar_usermode.conf"
        shutil.copyfile(tools.get_absolute_script_path(oar_configfile), oar_configfile)
    if create_db:
        if not nodes:
            nodes = "node[1-5]"
        db_create(nodes, nb_core, skip)
