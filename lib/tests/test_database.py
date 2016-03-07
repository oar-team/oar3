# -*- coding: utf-8 -*-
import pytest
import time
import datetime

from codecs import open

from sqlalchemy import event, Table
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError
from sqlalchemy.orm.util import object_state
from collections import OrderedDict

from oar.lib import fixture
from oar.lib.database import Database, SessionProperty, QueryProperty
from oar.lib.compat import StringIO, to_unicode, json
from oar.lib.utils import to_json
from . import assert_raises


class EngineListener(object):

    def __init__(self, engine, ignored=('PRAGMA')):
        self.engine = engine
        self.ignored = ignored
        self.buf = StringIO()

        @event.listens_for(engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement,
                                  parameters, context, executemany):
            sql = to_unicode(statement)
            for string in self.ignored:
                if sql.lower().startswith(string.lower()):
                    return
            sql = sql.replace(' \n', '\n').rstrip('\n')
            self.buf.write(sql.rstrip('\n') + ";" + '\n')

    @property
    def raw_sql(self):
        self.buf.seek(0)
        return self.buf.getvalue().replace('\t', '    ')\
                                  .rstrip('\n')


@pytest.fixture(scope='function')
def db(request):
    db = Database(uri='sqlite://')

    association_table = db.Table(
        'association',
        db.Column('movie_id', db.Integer, db.ForeignKey('movie.id')),
        db.Column('actor_id', db.Integer, db.ForeignKey('actor.id'))
    )

    class Movie(db.Model):
        __table_args__ = (
            db.UniqueConstraint('title', name='uix_1'),
            {'sqlite_autoincrement': True},
        )
        id = db.Column(db.Integer, primary_key=True)
        title = db.Column(db.String(20))

    class Actor(db.DeferredReflectionModel):
        __table_args__ = (
            db.Index('name', 'lastname', 'firstname'),
            db.UniqueConstraint('firstname', 'lastname', name='uix_1')
        )

        id = db.Column(db.Integer, primary_key=True)
        firstname = db.Column(db.String(20))
        lastname = db.Column(db.String(20))
        birthday = db.Column(db.DateTime, nullable=False,
                             default=datetime.datetime.utcnow)
        movies = db.relationship("Movie",
                                 secondary=association_table,
                                 backref="actors")
    return db


def test_sqlite_schema(db):
    engine_listener = EngineListener(db.engine)
    db.create_all()

    expected_schemas = """
CREATE TABLE movie (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(20) NOT NULL,
    CONSTRAINT uix_1 UNIQUE (title)
);

CREATE TABLE actor (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    firstname VARCHAR(20) NOT NULL,
    lastname VARCHAR(20) NOT NULL,
    birthday DATETIME NOT NULL,
    CONSTRAINT uix_1 UNIQUE (firstname, lastname)
);
CREATE INDEX name ON actor (lastname, firstname);

CREATE TABLE association (
    movie_id INTEGER NOT NULL,
    actor_id INTEGER NOT NULL,
    FOREIGN KEY(movie_id) REFERENCES movie (id),
    FOREIGN KEY(actor_id) REFERENCES actor (id)
);"""
    for schema in expected_schemas.split(';'):
        assert schema.strip() in engine_listener.raw_sql


def test_model_args(db):
    db.create_all()
    assert db['actor'].name == "actor"
    index_columns = list(list(db['actor'].indexes)[0].columns)
    assert index_columns[0].name == "lastname"
    assert index_columns[1].name == "firstname"


def test_collected_tables_and_models(db):
    db.create_all()
    model_names = ('Actor', 'Movie')
    table_names = ('actor', 'movie', 'association')
    for model_name in model_names:
        assert model_name in db
        assert isinstance(db[model_name], DeclarativeMeta)

    for table_name in table_names:
        assert table_name in db
        assert isinstance(db[table_name], Table)

    with assert_raises(KeyError, "totototo"):
        db['totototo']


def test_deferred_reflection(db):
    db.create_all()
    db.op.add_column('actor', db.Column('salary', db.Integer,
                                        nullable=True,
                                        default=1000000))
    db.reflect()
    db['Actor'].create(firstname="Ben", lastname="Affleck", salary=12000000)
    affleck = db['Actor'].query.first()
    keys = list(OrderedDict(affleck).keys())
    assert affleck.salary == 12000000
    assert ['id', 'firstname', 'lastname', 'birthday', 'salary'] == keys


def test_db_api_create_and_delete_all(db):
    db.create_all()
    db.reflect()
    dicaprio = db['Actor'].create(firstname="Leonardo", lastname="DiCaprio")
    ruffalo = db['Actor'].create(firstname="Mark", lastname="Ruffalo")
    shutter_island = db['Movie'].create(title="Shutter Island")
    shutter_island.actors.append(dicaprio)
    shutter_island.actors.append(ruffalo)

    dicaprio = db['Actor'].query.filter_by(firstname="Leonardo").first()
    assert dicaprio.lastname == "DiCaprio"
    assert dicaprio.movies[0].actors[0] is dicaprio
    assert dicaprio.movies[0].actors[1] is ruffalo

    with assert_raises(IntegrityError):
        db['Actor'].create(firstname="Leonardo", lastname="DiCaprio")

    db.delete_all()
    assert db['Actor'].query.count() == 0
    assert db['Movie'].query.count() == 0
    assert len(db.session.execute(db['association'].select()).fetchall()) == 0


def test_db_api_to_dict_json(db):
    db.create_all()
    db.reflect()
    Actor, Movie = db['Actor'], db['Movie']
    dt = datetime.datetime(2015, 7, 19, 9, 14, 22, 140921)
    a1 = Actor.create(firstname="Leonardo", lastname="DiCaprio", birthday=dt)
    a2 = Actor.create(firstname="Mark", lastname="Ruffalo")
    m1 = Movie.create(title="Shutter Island")
    m1.actors.append(a1)
    m1.actors.append(a2)

    item = Actor.query.filter_by(firstname="Leonardo").first()
    item_dict = OrderedDict([('id', 1),
                             ('firstname', 'Leonardo'),
                             ('lastname', 'DiCaprio'),
                             ('birthday', dt)])
    assert item.to_dict() == item_dict
    expected_json = """
{
    "id": 1,
    "firstname": "Leonardo",
    "lastname": "DiCaprio",
    "birthday": "2015-07-19T09:14:22.140921"
}""".strip()
    assert item.to_json() == expected_json
    assert to_json(item) == expected_json


def test_db_api_others(db):
    assert repr(db) == "<Database engine=None>"
    db.create_all()
    assert repr(db) == "<Database engine=Engine(sqlite://)>"
    assert db.metadata == db.Model.metadata
    movie = db['Movie'].create(title="Mad Max")
    assert repr(movie) == "<Movie (1,)>"

    assert db.query(db['Movie']).first().title == "Mad Max"
    assert db.dialect == "sqlite"


def test_db_api_add(db):
    db.create_all()
    movie = db['Movie'](title="Mad Max")
    db.add(movie)
    assert db['Movie'].query.first().title == "Mad Max"


def test_db_api_rollback(db):
    db.create_all()
    movie = db['Movie'](title="Mad Max")
    db.add(movie)
    db.rollback()
    assert db['Movie'].query.first() is None


def test_db_api_flush(db):
    db.create_all()
    movie = db['Movie'](title="Mad Max")
    db.add(movie)
    assert object_state(movie).pending is True
    db.flush()
    assert object_state(movie).persistent is True
    db.commit()


def test_db_api_close(db):
    assert db.connector is None
    db.create_all()
    db['Movie'].create(title="Mad Max")
    db.add(db['Movie'](title="Mad Max"))
    assert db.connector is not None
    session = db.session
    assert session.new
    db.close()
    assert db.connector is None
    assert not session.new


def test_internal_operations(db):
    assert isinstance(Database.session, SessionProperty)
    assert Database.Model is None
    assert Database.query_class is None
    assert Database.query_collection_class is None
    assert isinstance(db.Model.query, QueryProperty)


def test_load_fixtures(db, tmpdir):
    ts = int(time.time())

    db.create_all()

    db.op.add_column(
        table_name='actor',
        column=db.Column('start_time', db.Integer, nullable=True),
    )

    db.reflect()
    db.__time_columns__ = ["start_time"]

    Actor, Movie = db['Actor'], db['Movie']
    a1 = Actor.create(firstname="Leonardo", lastname="DiCaprio", start_time=ts)
    a2 = Actor.create(firstname="Mark", lastname="Ruffalo", start_time=ts)
    m1 = Movie.create(title="Shutter Island")
    m1.actors.append(a1)
    m1.actors.append(a2)

    assert Actor.query.order_by(Actor.id).first().start_time == ts

    filepath = tmpdir.join('fixtures.json').strpath
    fixture.dump_fixtures(db, filepath, ref_time=ts)

    data = {}
    with open(filepath, 'r', encoding='utf-8') as fd:
        data = json.load(fd)

    assert data['metadata']['ref_time'] == ts

    fixture.load_fixtures(db, filepath, clear=True, ref_time=None)
    assert Actor.query.order_by(Actor.id).first().start_time == ts

    fixture.load_fixtures(db, filepath, clear=True, ref_time=(ts - 10))
    assert Actor.query.order_by(Actor.id).first().start_time == (ts - 10)

    with assert_raises(IntegrityError):
        fixture.load_fixtures(db, filepath)


@pytest.mark.skipif("os.environ.get('DB_TYPE', '') == 'memory'",
                    reason="need persistent database")
def test_read_only_session():
    from oar.lib import db
    lenght = len(db['Resource'].query.all())
    if db.dialect == "sqlite":
        exception = OperationalError
    else:
        exception = ProgrammingError
    with assert_raises(exception):
        with db.session(read_only=True):
            assert len(db['Resource'].query.all()) == lenght
            db['Resource'].create()
    len(db['Resource'].query.all()) == lenght
