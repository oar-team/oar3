# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import

import threading
import contextlib

from collections import OrderedDict

from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.orm import scoped_session, sessionmaker, Query, class_mapper
from sqlalchemy.orm.exc import UnmappedClassError

from .exceptions import DoesNotExist
from .utils import cached_property
from .compat import iteritems


__all__ = ['Database']


class BaseQuery(Query):

    def get_or_error(self, uid):
        """Like :meth:`get` but raises an error if not found instead of
        returning `None`.
        """
        rv = self.get(uid)
        if rv is None:
            raise DoesNotExist()
        return rv

    def first_or_error(self):
        """Like :meth:`first` but raises an error if not found instead of
        returning `None`.
        """
        rv = self.first()
        if rv is None:
            raise DoesNotExist()
        return rv


class BaseModel(object):

    query = None

    @classmethod
    def create(cls, **kwargs):
        record = cls(**kwargs)
        try:
            cls.db.session.add(record)
            cls.db.session.commit()
            return record
        except Exception:
            cls.db.session.rollback()
            raise

    def asdict(self):
        result = OrderedDict()
        for key in self.__mapper__.c.keys():
            result[key] = getattr(self, key)
        return result

    def __iter__(self):
        """Returns an iterable that supports .next()"""
        for (k, v) in iteritems(self.asdict()):
            yield (k, v)

    def __repr__(self):
        return '<%s>' % self.__class__.__name__


class SessionProperty(object):

    def __init__(self):
        self._sessions = {}

    def _create_scoped_session(self, db):
        options = db._session_options
        options.setdefault('autoflush', True)
        options.setdefault('autocommit', False)
        options.setdefault('bind', db.engine)
        options.setdefault('query_cls', db.query_class)
        return scoped_session(sessionmaker(**options))

    def __get__(self, obj, type):
        if obj is not None:
            if obj not in self._sessions:
                self._sessions[obj] = self._create_scoped_session(obj)
                obj.reflect()
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
                return type.query_class(mapper, session=session)
        except UnmappedClassError:
            return None


class Database(object):
    """This class is used to instantiate a SQLAlchemy connection to
    a database.
    """

    session = SessionProperty()
    query_class = BaseQuery

    def __init__(self, uri=None, session_options=None):
        self.connector = None
        self._reflected = False
        self._cache = {"uri": uri}
        self._session_options = dict(session_options or {})
        self._engine_lock = threading.Lock()
        # Include some sqlalchemy orm functions
        _include_sqlalchemy(self)
        self.Model = declarative_base(cls=BaseModel, name='Model')
        self.Model.query = QueryProperty(self)

    @cached_property
    def uri(self):
        from oar.lib import config
        return config.get_sqlalchemy_uri()

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

    def reflect(self, **kwargs):
        """Proxy for Model.prepare"""
        if not self._reflected:
            # avoid a circular import
            from oar.lib import models  # noqa
            self.create_all()
            # autoload all tables marked for autoreflect
            self.DeferredReflection.prepare(self.engine)
            self._reflected = True

    def create_all(self, bind=None, **kwargs):
        """Creates all tables. """
        if bind is None:
            bind = self.engine
        self.metadata.create_all(bind=bind, **kwargs)

    def delete_all(self, bind=None, **kwargs):
        """Drop all tables. """
        if bind is None:
            bind = self.engine
        with contextlib.closing(bind.connect()) as con:
            trans = con.begin()
            for table in reversed(self.metadata.sorted_tables):
                con.execute(table.delete())
            trans.commit()

    def close(self, **kwargs):
        """Proxy for Session.close"""
        if self.connector is not None:
            return self.session.close()

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
            options.setdefault('pool_recycle', 7200)

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
            echo = self._config['SQLALCHEMY_ECHO']
            if (uri, echo) == self._connected_for:
                return self._engine
            info = make_url(uri)
            options = {'convert_unicode': True}
            self.apply_pool_defaults(options)
            self.apply_driver_hacks(info, options)
            if echo:
                options['echo'] = True
            self._engine = engine = create_engine(info, **options)
            self._connected_for = (uri, echo)
            return engine


def _include_sqlalchemy(obj):
    import sqlalchemy

    for module in sqlalchemy, sqlalchemy.orm:
        for key in module.__all__:
            if not hasattr(obj, key):
                setattr(obj, key, getattr(module, key))
    obj.event = sqlalchemy.event
    # Note: obj.Table does not attempt to be a SQLAlchemy Table class.

    def _make_table(obj):
        def _make_table(*args, **kwargs):
            if len(args) > 1 and isinstance(args[1], obj.Column):
                args = (args[0], obj.metadata) + args[1:]
            info = kwargs.pop('info', None) or {}
            info.setdefault('autoreflect', False)
            kwargs['info'] = info
            return sqlalchemy.Table(*args, **kwargs)
        return _make_table

    obj.Table = _make_table(obj)

    class Column(sqlalchemy.Column):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("nullable", False)
            super(Column, self).__init__(*args, **kwargs)

    obj.Column = Column
    obj.DeferredReflection = DeferredReflection
