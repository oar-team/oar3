# -*- coding: utf-8 -*-
import threading
import time
from contextlib import contextmanager

import sqlalchemy
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect  # , exc
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import class_mapper, sessionmaker
from sqlalchemy.orm.exc import UnmappedClassError
from sqlalchemy.pool import StaticPool

from .utils import cached_property

__all__ = ["Database"]


def reflect_base(metadata, defered, engine):
    """Proxy for Model.prepare"""
    defered.prepare(engine)


def wait_db_ready(f, args=None, attempt=7):
    delay = 0.2
    while attempt > 0:
        try:
            if args:
                r = f(*args)
            else:
                r = f()
        # except exc.OperationalError as error:
        except Exception:
            time.sleep(delay)
            delay = 2 * delay
            attempt -= 1
            if not attempt:
                raise
        else:
            return r


class Database(object):
    """This class is used to instantiate a SQLAlchemy connection to
    a database.
    """

    Model = None
    query_class = None
    query_collection_class = None

    def __init__(self, config, uri=None, uri_ro=None, session_options=None):
        self.connector = None
        self._config = config
        self.config = config


class EngineConnector(object):
    def __init__(self, db):
        # from oar.lib import config

        self._db = db
        self._config = db.config
        self._engine = None
        self._connected_for = None
        self._lock = threading.Lock()

    def apply_pool_defaults(self, options):
        def _setdefault(optionkey, configkey):
            value = self._config.get(configkey, None)
            if value is not None:
                options[optionkey] = value

        _setdefault("pool_size", "SQLALCHEMY_POOL_SIZE")
        _setdefault("pool_timeout", "SQLALCHEMY_POOL_TIMEOUT")
        _setdefault("pool_recycle", "SQLALCHEMY_POOL_RECYCLE")
        _setdefault("max_overflow", "SQLALCHEMY_MAX_OVERFLOW")

    def apply_driver_hacks(self, info, options):
        """This method is called before engine creation and used to inject
        driver specific hacks into the options.
        """
        if info.drivername == "mysql":
            info.query.setdefault("charset", "utf8")
            options.setdefault("pool_size", 10)
            options.setdefault("pool_recycle", 3600)
            # TODO: More test
            # from MySQLdb.cursors import SSCursor as MySQLdb_SSCursor
            # if MySQLdb_SSCursor is not None:
            #     connect_args = options.get('connect_args', {})
            #     connect_args.update({'cursorclass': MySQLdb_SSCursor})
            #     options['connect_args'] = connect_args

        elif info.drivername == "sqlite":
            no_pool = options.get("pool_size") == 0
            memory_based = info.database in (None, "", ":memory:")
            if memory_based and no_pool:
                raise ValueError(
                    "SQLite in-memory database with an empty queue"
                    " (pool_size = 0) is not possible due to data loss."
                )
        return options

    def get_engine(self):
        uri = self._config.get_sqlalchemy_uri()
        echo = self._config.get("SQLALCHEMY_ECHO", False)

        if (uri, echo) == self._connected_for:
            return self._engine
        info = make_url(uri)
        options = {}
        self.apply_pool_defaults(options)
        self.apply_driver_hacks(info, options)
        options["echo"] = echo
        if self._config["DB_TYPE"] == "sqlite":
            options["connect_args"] = {"check_same_thread": False}
            options["poolclass"] = StaticPool

        self._engine = engine = create_engine(info, **options)
        self._connected_for = (uri, echo)
        return engine


@contextmanager
def ephemeral_session(scoped, engine, **kwargs):
    """Ephemeral session context manager.
    Will rollback the transaction at the end.
    """
    try:
        scoped.remove()
        connection = engine.connect()
        # begin a non-ORM transaction
        transaction = connection.begin()
        kwargs["bind"] = connection
        session = scoped(**kwargs)
        yield session
    finally:
        session.close()
        # rollback - everything that happened with the
        # Session above (including calls to commit())
        # is rolled back.
        transaction.rollback()
        # return connection to the Engine
        connection.close()
        scoped.remove()


# flake8: noqa: (TODO: remove this function, write a working equivalent ?)
# def delete_all(engine, bind=None, **kwargs):
#     """Drop all tables."""
#     if bind is None:
#         bind = self.engine
#     with bind.connect() as con:
#         trans = con.begin()
#         try:
#             if bind.dialect.name == "postgresql":
#                 con.execute(
#                     "TRUNCATE {} RESTART IDENTITY CASCADE;".format(
#                         ",".join(table.name for table in self.tables.values())
#                     )
#                 )
#             else:
#                 for table in self.tables.values():
#                     con.execute(table.delete())
#             trans.commit()
#         except Exception:
#             trans.rollback
#
