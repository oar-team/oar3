# coding: utf-8

import os
import shutil
import tempfile
from codecs import open

import pytest
from sqlalchemy import (
    Column,
    Integer,
    String,
    text,
)

from oar.lib.database import EngineConnector

# from oar.lib import config, db
from oar.lib.globals import init_oar
from oar.lib.models import Model

from . import DEFAULT_CONFIG

config, db, logger = init_oar()


@pytest.fixture(scope="session", autouse=True)
def setup_config(request):
    config.update(DEFAULT_CONFIG.copy())
    tempdir = tempfile.mkdtemp()
    config["LOG_FILE"] = os.path.join(tempdir, "oar.log")

    db_type = os.environ.get("DB_TYPE", "memory")
    os.environ.setdefault("DB_TYPE", db_type)

    if db_type not in ("memory", "sqlite", "postgresql"):
        pytest.exit("Unsupported database '%s'" % db_type)

    if db_type == "sqlite":
        config["DB_BASE_FILE"] = os.path.join(tempdir, "db.sqlite")
        config["DB_TYPE"] = "sqlite"
    elif db_type == "memory":
        config["DB_TYPE"] = "sqlite"
        config["DB_BASE_FILE"] = ":memory:"
    else:
        config["DB_TYPE"] = "Pg"
        config["DB_PORT"] = "5432"
        config["DB_BASE_NAME"] = os.environ.get("POSTGRES_DB", "oar")
        config["DB_BASE_PASSWD"] = os.environ.get("POSTGRES_PASSWORD", "oar")
        config["DB_BASE_LOGIN"] = os.environ.get("POSTGRES_USER", "oar")
        config["DB_BASE_PASSWD_RO"] = os.environ.get("POSTGRES_PASSWORD", "oar_ro")
        config["DB_BASE_LOGIN_RO"] = os.environ.get("POSTGRES_USER_RO", "oar_ro")
        config["DB_HOSTNAME"] = os.environ.get("POSTGRES_HOST", "localhost")

    def dump_configuration(filename):
        folder = os.path.dirname(filename)
        if not os.path.exists(folder):
            os.makedirs(folder)
        with open(filename, "w", encoding="utf-8") as fd:
            for key, value in config.items():
                if not key.startswith("SQLALCHEMY_"):
                    fd.write("%s=%s\n" % (key, str(value)))

    dump_configuration("/tmp/oar.conf")

    engine = EngineConnector(db).get_engine()

    Model.metadata.drop_all(bind=engine)
    db.create_all(Model.metadata, bind=engine)

    kw = {"nullable": True}

    db.op(engine).add_column("resources", Column("core", Integer, **kw))
    db.op(engine).add_column("resources", Column("cpu", Integer, **kw))
    db.op(engine).add_column("resources", Column("host", String(255), **kw))
    db.op(engine).add_column("resources", Column("mem", Integer, **kw))

    db.reflect(Model.metadata, bind=engine)

    yield config, db, engine

    db.close()
    shutil.rmtree(tempdir)
