# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import

import functools
import threading
import datetime
import json

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.orm import scoped_session, sessionmaker, Query, class_mapper
from sqlalchemy.orm.exc import UnmappedClassError

from .exceptions import DoesNotExist, InvalidConfiguration
from .compat import string_types, itervalues

__all__ = ["Database"]


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


def get_entity_loaded_propnames(sa_instance):
    ins = inspect(sa_instance)
    keynames = set(
        ins.mapper.column_attrs.keys() +  # Columns
        ins.mapper.relationships.keys()  # Relationships
    )
    # If the sa_instance is not transient -- exclude unloaded keys
    # Transient entities won't load these anyway, so it's safe to include
    # all columns and get defaults
    if not ins.transient:
        keynames -= ins.unloaded

    # If the sa_instance is expired -- reload expired attributes as well
    # Expired attributes are usually unloaded as well!
    if ins.expired:
        keynames |= ins.expired_attributes
    return keynames


class BaseModel(object):

    query_class = BaseQuery
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

    def to_dict(self, exluded_keys=set()):
        keys = get_entity_loaded_propnames(self) - exluded_keys
        data = {}
        for k in keys:
            v = getattr(self, k)
            if isinstance(v, BaseModel) and v != self:
                v = v.to_dict()
            data[k] = v
        return data

    def to_json(self, exluded_keys=set()):
        data = {}
        for k, v in self.to_dict(exluded_keys).items():
            if isinstance(v, datetime.datetime):
                v = v.isoformat()
            data[k] = v


class BaseDeclarativeMeta(DeclarativeMeta):

    def __new__(cls, name, bases, d):
        return DeclarativeMeta.__new__(cls, name, bases, d)

    def __init__(self, name, bases, d):
        autoreflect = d.pop('__autoreflect__', None)
        DeclarativeMeta.__init__(self, name, bases, d)
        if autoreflect:
            self.__table__.info['autoreflect'] = True

        return json.dumps(data, sort_keys=True, indent=4, encoding="utf-8",
                          separators=(',', ': '))

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


class SessionProperty(object):

    def __init__(self):
        self._session = None

    def _create_scoped_session(self, db):
        options = db._session_options
        options.setdefault('autoflush', True)
        options.setdefault('autocommit', False)
        options.setdefault('bind', db.engine)
        return scoped_session(sessionmaker(**options))

    def __get__(self, obj, type):
        if obj is not None:
            if self._session is None:
                self._session = self._create_scoped_session(obj)
                if not obj._prepared:
                    obj.prepare()
            return self._session


class ModelProperty(object):

    def __init__(self):
        self._model = None

    def __get__(self, obj, type):
        if obj is not None:
            if self._model is None:
                base_model = declarative_base(cls=BaseModel, name='Model',
                                              metaclass=BaseDeclarativeMeta)
                self._model = automap_base(base_model)
                self._model.query = QueryProperty(obj)
                self._model.db = obj
            return self._model


class Database(object):
    """This class is used to instantiate a SQLAlchemy connection to
    a database.
    """

    session = SessionProperty()
    Model = ModelProperty()
    BaseQuery = BaseQuery

    def __init__(self, session_options=None):
        self.connector = None
        self._prepared = False
        self._uri = None
        self._session_options = dict(session_options or {})
        self._engine_lock = threading.Lock()

        # include sqlalchemy orm
        _include_sqlalchemy(self)

    @property
    def uri(self):
        if self._uri is None:
            from . import config
            try:
                db_conf = config.get_namespace("DB_")

                db_conf["type"] = db_conf["type"].lower()
                if db_conf["type"] in ("pg", "psql", "pgsql"):
                    db_conf["type"] = "postgresql"
                self._uri = "{type}://{base_login}:{base_passwd}" \
                            "@{hostname}:{port}/{base_name}".format(**db_conf)
            except KeyError as e:
                keys = tuple(('DB_%s' % i.upper() for i in e.args))
                raise InvalidConfiguration("Cannot find %s" % keys)
        return self._uri

    @property
    def engine(self):
        """Gives access to the engine. """
        with self._engine_lock:
            if self.connector is None:
                self.connector = EngineConnector(self)
            return self.connector.get_engine()

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

    def create_all(self):
        """Creates all tables. """
        self.Model.metadata.create_all(bind=self.engine)

    def drop_all(self):
        """Drops all tables. """
        self.Model.metadata.drop_all(bind=self.engine)

    def get_tables_for_autoreflect(self, bind=None):
        """Returns a list of all tables marked for autoreflect."""
        result = []
        for table in itervalues(self.Model.metadata.tables):
            if table.info.get('autoreflect'):
                result.append(table.name)
        return result

    def prepare(self, **kwargs):
        """Proxy for Model.prepare"""
        # a list of all tables marked for autoreflect
        autoreflect_tables = []
        for table in itervalues(self.Model.metadata.tables):
            if table.info.get('autoreflect'):
                autoreflect_tables.append(table.name)
        self.Model.metadata.reflect(
            self.engine,
            extend_existing=True,
            autoload_replace=False,
            only=autoreflect_tables,
        )
        self.Model.prepare(self.engine, **kwargs)
        self._prepared = True

    def __repr__(self):
        engine = None
        if self.connector is not None:
            engine = self.engine
        return '<%s engine=%r>' % (self.__class__.__name__,  engine)


class EngineConnector(object):

    def __init__(self, db):
        from oar import config
        self._config = config
        self._db = db
        self._engine = None
        self._connected_for = None
        self._lock = threading.Lock()

    def apply_pool_defaults(self, options):
        def _setdefault(optionkey, configkey):
            value = self._config[configkey]
            if value is not None:
                options[optionkey] = value
        _setdefault('pool_size', 'SQLALCHEMY_POOL_SIZE')
        _setdefault('pool_timeout', 'SQLALCHEMY_POOL_TIMEOUT')
        _setdefault('pool_recycle', 'SQLALCHEMY_POOL_RECYCLE')
        _setdefault('max_overflow', 'SQLALCHEMY_MAX_OVERFLOW')
        _setdefault('use_native_unicode', 'SQLALCHEMY_NATIVE_UNICODE')

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


def _include_sqlalchemy(db):
    import sqlalchemy
    for module in sqlalchemy, sqlalchemy.orm:
        for key in module.__all__:
            if not hasattr(db, key):
                setattr(db, key, getattr(module, key))
    db.event = sqlalchemy.event
    # Note: obj.Table does not attempt to be a SQLAlchemy Table class.
    def _make_table(db):
        def _make_table(*args, **kwargs):
            if len(args) > 1 and isinstance(args[1], db.Column):
                args = (args[0], db.metadata) + args[1:]
            info = kwargs.pop('info', None) or {}
            info.setdefault('autoreflect', False)
            kwargs['info'] = info
            return sqlalchemy.Table(*args, **kwargs)
        return _make_table

    db.Table = _make_table(db)

    def _set_default_query_class(d):
        if 'query_class' not in d:
            d['query_class'] = BaseQuery

    def _add_default_query_class(fn):
        @functools.wraps(fn)
        def newfn(*args, **kwargs):
            _set_default_query_class(kwargs)
            if "backref" in kwargs:
                backref = kwargs['backref']
                if isinstance(backref, string_types):
                    backref = (backref, {})
                _set_default_query_class(backref[1])
            return fn(*args, **kwargs)
        return newfn
    db.relationship = _add_default_query_class(db.relationship)
    db.relation = _add_default_query_class(db.relation)
    db.dynamic_loader = _add_default_query_class(db.dynamic_loader)
