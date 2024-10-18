import sys

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Column, Integer
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker

from oar.lib.globals import init_oar
from oar.lib.models import DeferredReflectionModel, Model, Queue, Resource


def db_create():
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

        for i in range(5):
            print(i)
            Resource.create(session, network_address="localhost" + str(int(i / 2)))
        session.commit()

    # reflect_base(Model.metadata, DeferredReflectionModel, engine)
    # DeferredReflectionModel.prepare(engine)
    engine.dispose()


if __name__ == "__main__":
    db_create()
