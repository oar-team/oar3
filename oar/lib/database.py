# -*- coding: utf-8 -*-
import sys
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager

import sqlalchemy
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect  # , exc
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import DeferredReflection
from sqlalchemy.orm import DeclarativeMeta, class_mapper, declarative_base, sessionmaker
from sqlalchemy.orm.exc import UnmappedClassError
from sqlalchemy.orm.state import InstanceState
from sqlalchemy.pool import StaticPool

from .utils import cached_property, get_table_name, merge_dicts, reraise, to_json

__all__ = ["Database"]


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


class BaseModel(object):
    __default_table_args__ = {"extend_existing": True, "sqlite_autoincrement": True}
    query = None

    @classmethod
    def create(cls, **kwargs):
        record = cls()
        for key, value in kwargs.items():
            setattr(record, key, value)
        try:
            cls._db.session.add(record)
            cls._db.session.commit()
            return record
        except Exception:
            exc_type, exc_value, tb = sys.exc_info()
            cls._db.session.rollback()
            reraise(exc_type, exc_value, tb.tb_next)

    def to_dict(self, ignore_keys=()):
        data = OrderedDict()
        for key in get_entity_loaded_propnames(self):
            if key not in ignore_keys:
                data[key] = getattr(self, key)
        return data

    asdict = to_dict

    def to_json(self, **kwargs):
        """Dump `self` to json string."""
        kwargs.setdefault("ignore_keys", ())
        obj = self.to_dict(kwargs.pop("ignore_keys"))
        return to_json(obj, **kwargs)

    def __iter__(self):
        """Return an iterable that supports .next()"""
        for key, value in (self.asdict()).items():
            yield (key, value)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, inspect(self).identity)


class SessionProperty(object):
    def __init__(self):
        self._sessions = {}

    def _create_scoped_session(self, db):
        options = db._session_options
        options.setdefault("bind", db.engine)
        if db.query_class is None:
            from .basequery import BaseQuery

            db.query_class = BaseQuery
        options.setdefault("query_cls", db.query_class)
        db.sessionmaker.configure(**options)
        scoped = ScopedSession(db.sessionmaker)
        scoped.db = db
        return scoped

    def __get__(self, obj, type):
        if obj is not None:
            if obj not in self._sessions:
                self._sessions[obj] = self._create_scoped_session(obj)
            if not obj._reflected:
                obj.reflect(bind=self._sessions[obj]().bind)
            return self._sessions[obj]
        return self


class QueryProperty(object):
    def __init__(self, db):
        self._db = db

    def __get__(self, obj, type):
        session = self._db.session()
        try:
            mapper = class_mapper(type)
            if mapper:
                return self._db.query_class(mapper, session=session)
        except UnmappedClassError:
            return self


class Database(object):
    """This class is used to instantiate a SQLAlchemy connection to
    a database.
    """

    session = SessionProperty()
    Model = None
    query_class = None
    query_collection_class = None

    def __init__(self, uri=None, uri_ro=None, session_options=None):
        self.connector = None
        self._reflected = False
        self._cache = {"uri": uri, "uri_ro": uri_ro}
        self._session_options = dict(session_options or {})
        self._session_options.setdefault("autoflush", True)
        self._session_options.setdefault("autocommit", False)
        self.sessionmaker = sessionmaker(**self._session_options)
        self._engine_lock = threading.Lock()
        # Include some sqlalchemy orm functions
        # _include_sqlalchemy(self)

        # self.Model.query = QueryProperty(self)
        # self.Model._db = self
        self.models = {}
        self.tables = {}

        # class DeferredReflectionModel(DeferredReflection, self.Model):
        #     __abstract__ = True

        # self.DeferredReflectionModel = DeferredReflectionModel

    @cached_property
    def uri(self):
        # from oar.lib import config

        return self.config.get_sqlalchemy_uri()

    @cached_property
    def uri_ro(self):
        from oar.lib import config

        return config.get_sqlalchemy_uri(read_only=True)

    @property
    def op(self):
        ctx = MigrationContext.configure(self.engine.connect())
        return Operations(ctx)

    @cached_property
    def queries(self):
        if self.query_collection_class is None:
            from .basequery import BaseQueryCollection

            self.query_collection_class = BaseQueryCollection
        return self.query_collection_class()

    @property
    def engine(self):
        """Gives access to the engine."""
        with self._engine_lock:
            if self.connector is None:
                self.connector = EngineConnector(self)
            return self.connector.get_engine()

    @cached_property
    def dialect(self):
        return self.engine.dialect.name

    @property
    def metadata(self):
        """Proxy for Model.metadata"""
        return self.Model.metadata

    @property
    def query(self):
        """Proxy for session.query"""
        return self.session.query

    def add(self, *args, **kwargs):
        """Proxy for session.add"""
        return self.session.add(*args, **kwargs)

    def flush(self, *args, **kwargs):
        """Proxy for session.flush"""
        return self.session.flush(*args, **kwargs)

    def commit(self):
        """Proxy for session.commit"""
        return self.session.commit()

    def rollback(self):
        """Proxy for session.rollback"""
        return self.session.rollback()

    def reflect(self, metadata, bind=None):
        """Proxy for Model.prepare"""
        from oar.lib.models import DeferredReflectionModel
        if not self._reflected:
            if bind is None:
                bind = self.engine
            self.create_all(metadata, bind=bind)
            # autoload all tables marked for autoreflect
            DeferredReflectionModel.prepare(bind)
            self._reflected = True

    def create_all(self, metadata, bind=None, **kwargs):
        """Creates all tables."""
        if bind is None:
            bind = self.engine
        metadata.create_all(bind=bind, **kwargs)

    def delete_all(self, bind=None, **kwargs):
        """Drop all tables."""
        if bind is None:
            bind = self.engine
        with bind.connect() as con:
            trans = con.begin()
            try:
                if bind.dialect.name == "postgresql":
                    con.execute(
                        "TRUNCATE {} RESTART IDENTITY CASCADE;".format(
                            ",".join(table.name for table in self.tables.values())
                        )
                    )
                else:
                    for table in self.tables.values():
                        con.execute(table.delete())
                trans.commit()
            except Exception:
                trans.rollback()
                raise

    def __contains__(self, member):
        return member in self.tables or member in self.models

    def __getitem__(self, name):
        if name in self:
            if name in self.tables:
                return self.tables[name]
            else:
                return self.models[name]
        else:
            raise KeyError(name)

    def close(self, **kwargs):
        """Proxy for Session.close"""
        self.session.close()
        with self._engine_lock:
            if self.connector is not None:
                self.connector.get_engine().dispose()
                self.connector = None

    def show(self):
        """Return small database content representation."""
        for model_name in sorted(self.models.keys()):
            data = [inspect(i).identity for i in self.models[model_name].query.all()]
            print(model_name.ljust(25), data)

    def __repr__(self):
        engine = None
        if self.connector is not None:
            engine = self.engine
        return "<%s engine=%r>" % (self.__class__.__name__, engine)


class EngineConnector(object):
    def __init__(self, db):
        # from oar.lib import config

        self._config = db.config
        self._db = db
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
        with self._lock:
            uri = self._db.uri
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


def _include_sqlalchemy(db):
    import sqlalchemy

    for module in sqlalchemy, sqlalchemy.orm:
        for key in module.__all__:
            if not hasattr(db, key):
                setattr(db, key, getattr(module, key))
    db.event = sqlalchemy.event
    # Note: db.Table does not attempt to be a SQLAlchemy Table class.

    def _make_table(db):
        def _make_table(*args, **kwargs):
            if len(args) > 1 and isinstance(args[1], db.Column):
                args = (args[0], db.metadata) + args[1:]
            kwargs.setdefault("extend_existing", True)
            info = kwargs.pop("info", None) or {}
            info.setdefault("autoreflect", False)
            kwargs["info"] = info
            table = sqlalchemy.Table(*args, **kwargs)
            db.tables[table.name] = table
            return table

        return _make_table

    db.Table = _make_table(db)

    class Column(sqlalchemy.Column):
        # Since SQLAlchemy 1.4, Column needs the attribute `inherit_cache`. Otherwise a warning is displayed.
        # https://docs.sqlalchemy.org/en/14/core/compiler.html#synopsis
        inherit_cache = True

        def __init__(self, *args, **kwargs):
            kwargs.setdefault("nullable", False)
            super(Column, self).__init__(*args, **kwargs)

    db.Column = Column


@contextmanager
def read_only_session(scoped, **kwargs):
    """Read-only session context manager.

    Will raise exception if we try to write in the database.
    """
    dialect = scoped.db.engine.dialect.name
    if dialect == "postgresql":
        try:
            kwargs["bind"] = create_engine(scoped.db.uri_ro)
            session = scoped.session_factory(**kwargs)
            old_session = None
            if scoped.registry.has():
                old_session = scoped.registry()
                scoped.registry.clear()
            scoped.registry.set(session)
            yield session
        finally:
            scoped.remove()
            if old_session is not None:
                scoped.registry.set(old_session)
    elif dialect == "sqlite":
        import sqlite3

        sqlite_path = scoped.db.engine.url.database
        if not sqlite_path or sqlite_path == ":memory:":
            yield scoped(**kwargs)
        else:
            try:

                def creator():
                    return sqlite3.connect("file:%s?mode=ro" % sqlite_path)

                kwargs["bind"] = create_engine(
                    "sqlite://",
                    creator=creator,
                    connect_args={"check_same_thread": False},
                )

                scoped.remove()
                session = scoped(**kwargs)
                yield session
            finally:
                session.close()
                scoped.remove()
    else:
        yield scoped(**kwargs)


@contextmanager
def ephemeral_session(scoped, **kwargs):
    """Ephemeral session context manager.
    Will rollback the transaction at the end.
    """
    try:
        scoped.remove()
        connection = scoped.db.engine.connect()
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


class ScopedSession(sqlalchemy.orm.scoped_session):
    def __call__(self, **kwargs):
        if kwargs.pop("read_only", False):
            return read_only_session(self, **kwargs)
        elif kwargs.pop("ephemeral", False):
            return ephemeral_session(self, **kwargs)
        else:
            return super(ScopedSession, self).__call__(**kwargs)
