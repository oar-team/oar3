# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import

import sys
import threading

from collections import OrderedDict
from contextlib import contextmanager

import sqlalchemy
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import (declarative_base, DeferredReflection,
                                        DeclarativeMeta)
from sqlalchemy.orm import sessionmaker, class_mapper
from sqlalchemy.orm.state import InstanceState
from sqlalchemy.orm.exc import UnmappedClassError

from alembic.migration import MigrationContext
from alembic.operations import Operations

from .utils import cached_property, merge_dicts, get_table_name, to_json
from .compat import iteritems, itervalues, iterkeys, reraise


__all__ = ['Database']


class BaseModel(object):

    __default_table_args__ = {
        'extend_existing': True,
        'sqlite_autoincrement': True
    }
    query = None

    @classmethod
    def create(cls, **kwargs):
        record = cls()
        for key, value in iteritems(kwargs):
            setattr(record, key, value)
        try:
            cls._db.session.add(record)
            cls._db.session.commit()
            return record
        except:
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
        kwargs.setdefault('ignore_keys', ())
        obj = self.to_dict(kwargs.pop('ignore_keys'))
        return to_json(obj, **kwargs)

    def __iter__(self):
        """Return an iterable that supports .next()"""
        for (k, v) in iteritems(self.asdict()):
            yield (k, v)

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, inspect(self).identity)


class SessionProperty(object):

    def __init__(self):
        self._sessions = {}

    def _create_scoped_session(self, db):
        options = db._session_options
        options.setdefault('bind', db.engine)
        if db.query_class is None:
            from .basequery import BaseQuery
            db.query_class = BaseQuery
        options.setdefault('query_cls', db.query_class)
        db.sessionmaker.configure(**options)
        scoped = scoped_session(db.sessionmaker)
        scoped.db = db
        return scoped

    def __get__(self, obj, type):
        if obj is not None:
            if obj not in self._sessions:
                self._sessions[obj] = self._create_scoped_session(obj)
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
        self._session_options.setdefault('autoflush', True)
        self._session_options.setdefault('autocommit', False)
        self.sessionmaker = sessionmaker(**self._session_options)
        self._engine_lock = threading.Lock()
        # Include some sqlalchemy orm functions
        _include_sqlalchemy(self)
        self.Model = declarative_base(cls=BaseModel, name='Model',
                                      metaclass=_BoundDeclarativeMeta)
        self.Model.query = QueryProperty(self)
        self.Model._db = self
        self.models = {}
        self.tables = {}

        class DeferredReflectionModel(DeferredReflection, self.Model):
            __abstract__ = True

        self.DeferredReflectionModel = DeferredReflectionModel

    @cached_property
    def uri(self):
        from oar.lib import config
        return config.get_sqlalchemy_uri()

    @cached_property
    def uri_ro(self):
        from oar.lib import config
        return config.get_sqlalchemy_uri(read_only=True)

    @property
    def op(self):
        ctx = MigrationContext.configure(self.session(reflect=False).bind)
        return Operations(ctx)

    @cached_property
    def queries(self):
        if self.query_collection_class is None:
            from .basequery import BaseQueryCollection
            self.query_collection_class = BaseQueryCollection
        return self.query_collection_class()

    @property
    def engine(self):
        """Gives access to the engine. """
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

    def reflect(self, bind=None):
        """Proxy for Model.prepare"""
        if not self._reflected:
            if bind is None:
                bind = self.session(reflect=False).bind
            self.create_all()
            # autoload all tables marked for autoreflect
            self.DeferredReflectionModel.prepare(self.session.bind)
            self._reflected = True

    def create_all(self, bind=None, **kwargs):
        """Creates all tables. """
        if bind is None:
            bind = self.session(reflect=False).bind
        self.metadata.create_all(bind=bind, **kwargs)

    def delete_all(self, bind=None, **kwargs):
        """Drop all tables. """
        if bind is None:
            bind = self.session(reflect=False).bind
        with bind.connect() as con:
            trans = con.begin()
            try:
                if bind.dialect.name == "postgresql":
                    con.execute('TRUNCATE {} RESTART IDENTITY CASCADE;'.format(
                        ','.join(table.name
                                 for table in self.tables.values())))
                else:
                    for table in itervalues(self.tables):
                        con.execute(table.delete())
                trans.commit()
            except:
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
        """ Return small database content representation."""
        for model_name in sorted(iterkeys(self.models)):
            data = [inspect(i).identity
                    for i in self.models[model_name].query.all()]
            print(model_name.ljust(25), data)

    def __repr__(self):
        engine = None
        if self.connector is not None:
            engine = self.engine
        return '<%s engine=%r>' % (self.__class__.__name__, engine)


class EngineConnector(object):

    def __init__(self, db):
        from oar.lib import config
        self._config = config
        self._db = db
        self._engine = None
        self._connected_for = None
        self._lock = threading.Lock()

    def apply_pool_defaults(self, options):
        def _setdefault(optionkey, configkey):
            value = self._config.get(configkey, None)
            if value is not None:
                options[optionkey] = value
        _setdefault('pool_size', 'SQLALCHEMY_POOL_SIZE')
        _setdefault('pool_timeout', 'SQLALCHEMY_POOL_TIMEOUT')
        _setdefault('pool_recycle', 'SQLALCHEMY_POOL_RECYCLE')
        _setdefault('max_overflow', 'SQLALCHEMY_MAX_OVERFLOW')
        _setdefault('convert_unicode', 'SQLALCHEMY_CONVERT_UNICODE')

    def apply_driver_hacks(self, info, options):
        """This method is called before engine creation and used to inject
        driver specific hacks into the options.
        """
        if info.drivername == 'mysql':
            info.query.setdefault('charset', 'utf8')
            options.setdefault('pool_size', 10)
            options.setdefault('pool_recycle', 3600)
            # TODO: More test
            # from MySQLdb.cursors import SSCursor as MySQLdb_SSCursor
            # if MySQLdb_SSCursor is not None:
            #     connect_args = options.get('connect_args', {})
            #     connect_args.update({'cursorclass': MySQLdb_SSCursor})
            #     options['connect_args'] = connect_args

        elif info.drivername == 'sqlite':
            no_pool = options.get('pool_size') == 0
            memory_based = info.database in (None, '', ':memory:')
            if memory_based and no_pool:
                raise ValueError(
                    'SQLite in-memory database with an empty queue'
                    ' (pool_size = 0) is not possible due to data loss.'
                )
        return options

    def get_engine(self):
        with self._lock:
            uri = self._db.uri
            echo = self._config.get('SQLALCHEMY_ECHO', False)
            if (uri, echo) == self._connected_for:
                return self._engine
            info = make_url(uri)
            options = {}
            self.apply_pool_defaults(options)
            self.apply_driver_hacks(info, options)
            options['echo'] = echo
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
            kwargs.setdefault('extend_existing', True)
            info = kwargs.pop('info', None) or {}
            info.setdefault('autoreflect', False)
            kwargs['info'] = info
            table = sqlalchemy.Table(*args, **kwargs)
            db.tables[table.name] = table
            return table
        return _make_table

    db.Table = _make_table(db)

    class Column(sqlalchemy.Column):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("nullable", False)
            super(Column, self).__init__(*args, **kwargs)

    db.Column = Column


class _BoundDeclarativeMeta(DeclarativeMeta):

    def __new__(cls, name, bases, d):
        if '__tablename__' not in d and '__table__' not in d and '__abstract__' not in d:
            d['__tablename__'] = get_table_name(name)
        default_table_args = d.pop('__default_table_args__',
                                   BaseModel.__default_table_args__)
        table_args = d.pop('__table_args__', {})
        if isinstance(table_args, dict):
            table_args = merge_dicts(default_table_args, table_args)
        elif isinstance(table_args, tuple):
            table_args = list(table_args)
            if isinstance(table_args[-1], dict):
                table_args[-1] = merge_dicts(default_table_args,
                                             table_args[-1])
            else:
                table_args.append(default_table_args)
            table_args = tuple(table_args)
        d['__table_args__'] = table_args
        return DeclarativeMeta.__new__(cls, name, bases, d)

    def __init__(self, name, bases, d):
        DeclarativeMeta.__init__(self, name, bases, d)
        if hasattr(bases[0], '_db'):
            bases[0]._db.models[name] = self
            bases[0]._db.tables[self.__table__.name] = self.__table__
            self._db = bases[0]._db


def get_entity_loaded_propnames(entity):
    """Get entity property names that are loaded (e.g. won't produce new
    queries)

    :param entity: SQLAlchemy entity
    :returns: List of entity property names
    """
    ins = entity if isinstance(entity, InstanceState) else inspect(entity)
    columns = ins.mapper.column_attrs.keys() + ins.mapper.relationships.keys()
    keynames = set(columns)
    # If the entity is not transient -- exclude unloaded keys
    # Transient entities won't load these anyway, so it's safe to include
    # all columns and get defaults
    if not ins.transient:
        keynames -= ins.unloaded

    # If the entity is expired -- reload expired attributes as well
    # Expired attributes are usually unloaded as well!
    if ins.expired:
        keynames |= ins.expired_attributes
    return sorted(keynames, key=lambda x: columns.index(x))


@contextmanager
def read_only_session(scoped, **kwargs):
    """Read-only session context manager.

    Will raise exception if we try to write in the database.
    """
    dialect = scoped.db.engine.dialect.name
    if dialect == 'postgresql':
        try:
            kwargs['bind'] = create_engine(scoped.db.uri_ro)
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
                    return sqlite3.connect('file:%s?mode=ro' % sqlite_path)

                kwargs['bind'] = create_engine('sqlite://', creator=creator)
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
        kwargs['bind'] = connection
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


class scoped_session(sqlalchemy.orm.scoped_session):  # noqa
    def __call__(self, **kwargs):
        if kwargs.pop('read_only', False):
            return read_only_session(self, **kwargs)
        elif kwargs.pop('ephemeral', False):
            return ephemeral_session(self, **kwargs)
        else:
            reflect = kwargs.pop('reflect', True)
            session = super(scoped_session, self).__call__(**kwargs)
            if reflect:
                self.db.reflect(bind=session.bind)
            return session
