# -*- coding: utf-8 -*-
import datetime

from sqlalchemy import event

from oar.lib.database import Database
from oar.lib.compat import StringIO, to_unicode


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


def declare_schema(db):

    class Actor(db.Model, db.DeferredReflection):
        __table_args__ = (db.Index('name', 'lastname', 'firstname'), )

        id = db.Column(db.Integer, primary_key=True)
        firstname = db.Column(db.String(20))
        lastname = db.Column(db.String(20))
        birthday = db.Column(db.DateTime, nullable=False,
                             default=datetime.datetime.utcnow)

    category = db.Table('categories',
                        db.Column('name', db.String(255)),
                        db.Column('description', db.String(255)))

    return Actor, category


def test_sqlite_schema():
    db = Database(uri='sqlite://')
    engine_listener = EngineListener(db.engine)
    declare_schema(db)
    db.create_all()

    expected_schema = """
CREATE TABLE categories (
    name VARCHAR(255) NOT NULL,
    description VARCHAR(255) NOT NULL
);

CREATE TABLE actor (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    firstname VARCHAR(20) NOT NULL,
    lastname VARCHAR(20) NOT NULL,
    birthday DATETIME NOT NULL
);
CREATE INDEX name ON actor (lastname, firstname);"""
    assert engine_listener.raw_sql == expected_schema


def test_model_args():
    db = Database(uri='sqlite://')
    Actor, _ = declare_schema(db)
    db.create_all()
    assert Actor.__table__.name == "actor"
    index_columns = list(list(Actor.__table__.indexes)[0].columns)
    assert index_columns[0].name == "lastname"
    assert index_columns[1].name == "firstname"


def test_deferred_reflection():
    db = Database(uri='sqlite://')
    Actor, _ = declare_schema(db)
    db.create_all()
    db.op.add_column('actor', db.Column('salary', db.Integer,
                                        nullable=True,
                                        default=1000000))
    db.reflect()
    Actor.create(firstname="Ben", lastname="Affleck", salary=12000000)
    ben = Actor.query.first()
    keys = ben.asdict().keys()
    assert ben.salary == 12000000
    assert ['id', 'firstname', 'lastname', 'birthday', 'salary'] == keys


def test_collected_tables_and_models():
    db = Database(uri='sqlite://')
    Actor, category = declare_schema(db)
    db.create_all()
    db.op.add_column('actor', db.Column('salary', db.Integer,
                                        nullable=True,
                                        default=1000000))
    assert db.tables == {'actor': Actor.__table__, 'categories': category}
    assert db.models == {'Actor': Actor}
    assert "Actor" in db
    assert "actor" in db
    assert "categories" in db
