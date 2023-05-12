# -*- coding: utf-8 -*-
import sys
from collections import OrderedDict

from sqlalchemy import (  # , exc
    BigInteger,
    CheckConstraint,
    Column,
    Index,
    Integer,
    String,
    Table,
    Text,
    inspect,
    text,
)
from sqlalchemy.ext.declarative import DeferredReflection
from sqlalchemy.orm import DeclarativeMeta, declarative_base
from sqlalchemy.orm.state import InstanceState

from oar.lib.database import Database

from .utils import reraise, to_json

# from .globals import db


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


class BaseModel(object):
    __default_table_args__ = {"extend_existing": True, "sqlite_autoincrement": True}
    query = None

    @classmethod
    def create(cls, session, **kwargs):
        record = cls()
        for key, value in kwargs.items():
            setattr(record, key, value)
        try:
            session.add(record)
            session.commit()
            return record
        except Exception:
            exc_type, exc_value, tb = sys.exc_info()
            session.rollback()
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


Model = declarative_base(cls=BaseModel, name="Model")


class DeferredReflectionModel(DeferredReflection, Model):
    __abstract__ = True


class Accounting(Model):
    __tablename__ = "accounting"

    window_start = Column(BigInteger, primary_key=True, autoincrement=False)
    window_stop = Column(
        BigInteger, primary_key=True, autoincrement=False, server_default="0"
    )
    user = Column(
        "accounting_user",
        String(255),
        primary_key=True,
        index=True,
        server_default="",
    )
    project = Column(
        "accounting_project",
        String(255),
        primary_key=True,
        index=True,
        server_default="",
    )
    queue_name = Column(String(100), primary_key=True, index=True, server_default="")
    consumption_type = Column(
        String(5), primary_key=True, index=True, server_default="ASKED"
    )
    consumption = Column(BigInteger, server_default="0")


class AdmissionRule(Model):
    __tablename__ = "admission_rules"

    id = Column(Integer, primary_key=True)
    rule = Column(Text)
    priority = Column(Integer, server_default="0")
    enabled = Column(String(3), server_default="YES")


class AssignedResource(Model):
    __tablename__ = "assigned_resources"

    moldable_id = Column(
        "moldable_job_id",
        Integer,
        primary_key=True,
        autoincrement=False,
        index=True,
        server_default="0",
    )
    resource_id = Column(Integer, primary_key=True, server_default="0")
    index = Column(
        "assigned_resource_index", String(7), index=True, server_default="CURRENT"
    )


class Challenge(Model):
    __tablename__ = "challenges"

    job_id = Column(
        Integer,
        primary_key=True,
        autoincrement=False,
        index=True,
        server_default="0",
    )
    challenge = Column(String(255), server_default="")
    ssh_private_key = Column(Text, server_default="")
    ssh_public_key = Column(Text, server_default="")


class EventLogHostname(Model):
    __tablename__ = "event_log_hostnames"

    event_id = Column(
        Integer, primary_key=True, autoincrement=False, server_default="0"
    )
    hostname = Column(String(255), primary_key=True, index=True, server_default="")


class EventLog(Model):
    __tablename__ = "event_logs"

    id = Column("event_id", Integer, primary_key=True)
    type = Column(String(100), index=True, server_default="")
    job_id = Column(Integer, index=True, server_default="0")
    date = Column(Integer, server_default="0")
    description = Column(String(255), server_default="")
    to_check = Column(String(3), index=True, server_default="YES")


class File(Model):
    __tablename__ = "files"

    id = Column("file_id", Integer, primary_key=True)
    md5sum = Column(String(255), index=True, nullable=True, server_default=text("NULL"))
    location = Column(String(255), nullable=True, server_default=text("NULL"))
    method = Column(String(255), nullable=True, server_default=text("NULL"))
    compression = Column(String(255), nullable=True, server_default=text("NULL"))
    size = Column(Integer, server_default="0")


class FragJob(Model):
    __tablename__ = "frag_jobs"

    job_id = Column("frag_id_job", Integer, primary_key=True, server_default="0")
    date = Column("frag_date", Integer, server_default="0")
    state = Column("frag_state", String(16), index=True, server_default="LEON")


class GanttJobsPrediction(Model):
    __tablename__ = "gantt_jobs_predictions"

    moldable_id = Column(
        "moldable_job_id",
        Integer,
        primary_key=True,
        autoincrement=False,
        server_default="0",
    )
    start_time = Column(Integer, server_default="0")


class GanttJobsPredictionsLog(Model):
    __tablename__ = "gantt_jobs_predictions_log"

    sched_date = Column(
        Integer, primary_key=True, autoincrement=False, server_default="0"
    )
    moldable_id = Column(
        "moldable_job_id", Integer, primary_key=True, server_default="0"
    )
    start_time = Column(Integer, server_default="0")


class GanttJobsPredictionsVisu(Model):
    __tablename__ = "gantt_jobs_predictions_visu"

    moldable_id = Column(
        "moldable_job_id",
        Integer,
        primary_key=True,
        autoincrement=False,
        server_default="0",
    )
    start_time = Column(Integer, server_default="0")


class GanttJobsResource(Model):
    __tablename__ = "gantt_jobs_resources"

    moldable_id = Column(
        "moldable_job_id",
        Integer,
        primary_key=True,
        autoincrement=False,
        server_default="0",
    )
    resource_id = Column(Integer, primary_key=True, server_default="0")


class GanttJobsResourcesLog(Model):
    __tablename__ = "gantt_jobs_resources_log"

    sched_date = Column(
        Integer, primary_key=True, autoincrement=False, server_default="0"
    )
    moldable_id = Column(
        "moldable_job_id", Integer, primary_key=True, server_default="0"
    )
    resource_id = Column(Integer, primary_key=True, server_default="0")


class GanttJobsResourcesVisu(Model):
    __tablename__ = "gantt_jobs_resources_visu"

    moldable_id = Column(
        "moldable_job_id",
        Integer,
        primary_key=True,
        autoincrement=False,
        server_default="0",
    )
    resource_id = Column(Integer, primary_key=True, server_default="0")


class JobDependencie(Model):
    __tablename__ = "job_dependencies"

    job_id = Column(
        Integer,
        primary_key=True,
        index=True,
        autoincrement=False,
        server_default="0",
    )
    job_id_required = Column(Integer, primary_key=True, server_default="0")
    index = Column(
        "job_dependency_index", String(7), index=True, server_default="CURRENT"
    )


class JobResourceDescription(Model):
    __tablename__ = "job_resource_descriptions"

    group_id = Column(
        "res_job_group_id",
        Integer,
        primary_key=True,
        autoincrement=False,
        index=True,
        server_default="0",
    )
    resource_type = Column(
        "res_job_resource_type", String(255), primary_key=True, server_default=""
    )
    value = Column("res_job_value", Integer, server_default="0")
    order = Column("res_job_order", Integer, primary_key=True, server_default="0")
    index = Column("res_job_index", String(7), index=True, server_default="CURRENT")


class JobResourceGroup(Model):
    __tablename__ = "job_resource_groups"

    id = Column("res_group_id", Integer, primary_key=True)
    moldable_id = Column(
        "res_group_moldable_id", Integer, index=True, server_default="0"
    )
    property = Column("res_group_property", Text, nullable=True)
    index = Column("res_group_index", String(7), index=True, server_default="CURRENT")


class JobStateLog(Model):
    __tablename__ = "job_state_logs"

    id = Column("job_state_log_id", Integer, primary_key=True)
    job_id = Column(Integer, index=True, server_default="0")
    job_state = Column(String(16), index=True, server_default="Waiting")
    date_start = Column(Integer, server_default="0")
    date_stop = Column(Integer, server_default="0")


class JobType(Model):
    __tablename__ = "job_types"

    id = Column("job_type_id", Integer, primary_key=True)
    job_id = Column(Integer, index=True, server_default="0")
    type = Column(String(255), index=True, server_default="")
    types_index = Column(String(7), index=True, server_default="CURRENT")


class Resource(DeferredReflectionModel):
    __tablename__ = "resources"

    id = Column("resource_id", Integer, primary_key=True)
    type = Column(String(100), index=True, server_default="default")
    network_address = Column(String(100), index=True, server_default="")
    state = Column(String(9), index=True, server_default="Alive")
    next_state = Column(String(9), index=True, server_default="UnChanged")
    finaud_decision = Column(String(3), server_default="NO")
    next_finaud_decision = Column(String(3), server_default="NO")
    state_num = Column(Integer, server_default="0")
    suspended_jobs = Column(String(3), index=True, server_default="NO")
    scheduler_priority = Column(BigInteger, server_default="0")
    cpuset = Column(String(255), server_default="0")
    besteffort = Column(String(3), server_default="YES")
    deploy = Column(String(3), server_default="NO")
    expiry_date = Column(Integer, server_default="0")
    desktop_computing = Column(String(3), server_default="NO")
    last_job_date = Column(Integer, server_default="0")
    available_upto = Column(Integer, server_default="2147483647")
    last_available_upto = Column(Integer, server_default="0")
    drain = Column(String(3), server_default="NO")


class Job(DeferredReflectionModel):
    __tablename__ = "jobs"
    __table_args__ = (Index("state_id", "state", "job_id"),)

    id = Column("job_id", Integer, primary_key=True)
    array_id = Column(Integer, index=True, server_default="0")
    array_index = Column(Integer, server_default="1")
    initial_request = Column(Text, nullable=True)
    name = Column("job_name", String(100), nullable=True)
    env = Column("job_env", Text, nullable=True)
    type = Column("job_type", String(11), server_default="PASSIVE")
    info_type = Column(String(255), nullable=True, server_default=text("NULL"))
    state = Column(String(16), index=True, server_default="Waiting")
    reservation = Column(String(10), index=True, server_default="None")
    message = Column(String(255), server_default="")
    scheduler_info = Column(String(255), server_default="")
    user = Column("job_user", String(255), server_default="")
    project = Column(String(255), server_default="")
    group = Column("job_group", String(255), server_default="")
    command = Column(Text, nullable=True)
    exit_code = Column(Integer, nullable=True)
    queue_name = Column(String(100), index=True, server_default="")
    properties = Column(Text, nullable=True)
    launching_directory = Column(Text)
    submission_time = Column(Integer, server_default="0")
    start_time = Column(Integer, server_default="0")
    stop_time = Column(Integer, server_default="0")
    file_id = Column(Integer, nullable=True)
    accounted = Column(String(3), index=True, server_default="NO")
    notify = Column(String(255), nullable=True, server_default=text("NULL"))
    # TODO assigned_moldable_job -> assigned_moldable_id
    assigned_moldable_job = Column(Integer, nullable=True, server_default="0")
    checkpoint = Column(Integer, server_default="0")
    checkpoint_signal = Column(Integer)
    stdout_file = Column(Text, nullable=True)
    stderr_file = Column(Text, nullable=True)
    resubmit_job_id = Column(Integer, server_default="0")
    suspended = Column(String(3), index=True, server_default="NO")


class MoldableJobDescription(Model):
    __tablename__ = "moldable_job_descriptions"

    id = Column("moldable_id", Integer, primary_key=True)
    job_id = Column("moldable_job_id", Integer, index=True, server_default="0")
    walltime = Column("moldable_walltime", Integer, server_default="0")
    index = Column("moldable_index", String(7), index=True, server_default="CURRENT")


class Queue(Model):
    __tablename__ = "queues"

    name = Column("queue_name", String(100), primary_key=True, server_default="")
    priority = Column(Integer, server_default="0")
    scheduler_policy = Column(String(100), server_default="")
    state = Column(String(9), server_default="Active")


class ResourceLog(Model):
    __tablename__ = "resource_logs"

    id = Column("resource_log_id", Integer, primary_key=True)
    resource_id = Column(Integer, index=True, server_default="0")
    attribute = Column(String(255), index=True, server_default="")
    value = Column(String(255), index=True, server_default="")
    date_start = Column(Integer, index=True, server_default="0")
    date_stop = Column(Integer, index=True, server_default="0")
    finaud_decision = Column(String(3), index=True, server_default="NO")


class Scheduler(Model):
    __tablename__ = "scheduler"

    name = Column(String(100), primary_key=True)
    script = Column(String(100))
    description = Column(String(255))


class WalltimeChange(Model):
    __tablename__ = "walltime_change"

    job_id = Column(
        Integer,
        primary_key=True,
        autoincrement=False,
        index=True,
        server_default="0",
    )
    pending = Column(Integer, server_default="0")
    force = Column(String(3), index=True, server_default="NO")
    delay_next_jobs = Column(String(3), index=True, server_default="NO")
    granted = Column(Integer, server_default="0")
    granted_with_force = Column(Integer, server_default="0")
    granted_with_delay_next_jobs = Column(Integer, server_default="0")

    __table_args__ = (
        CheckConstraint(force.in_(["NO", "YES"])),
        CheckConstraint(delay_next_jobs.in_(["NO", "YES"])),
    )


def all_models():
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and isinstance(obj, DeclarativeMeta):
            yield name, obj


def all_tables():
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and isinstance(obj, DeclarativeMeta):
            yield obj.__table__.name, obj.__table__
        elif isinstance(obj, Table):
            yield obj.name, obj


def setup_db(db: Database, engine):
    JOBS_TABLES = [
        {"jobs": "job_id"},
        {"challenges": "job_id"},
        {"event_logs": "job_id"},
        {"frag_jobs": "frag_id_job"},
        {"job_dependencies": "job_id"},
        {"job_dependencies": "job_id_required"},
        {"job_state_logs": "job_id"},
        {"job_types": "job_id"},
        {"moldable_job_descriptions": "moldable_job_id"},
    ]

    MOLDABLES_JOBS_TABLES = [
        {"moldable_job_descriptions": "moldable_id"},
        {"assigned_resources": "moldable_job_id"},
        {"job_resource_groups": "res_group_moldable_id"},
        {"gantt_jobs_predictions": "moldable_job_id"},
        {"gantt_jobs_predictions_log": "moldable_job_id"},
        {"gantt_jobs_predictions_visu": "moldable_job_id"},
        {"gantt_jobs_resources": "moldable_job_id"},
        {"gantt_jobs_resources_log": "moldable_job_id"},
        {"gantt_jobs_resources_visu": "moldable_job_id"},
    ]

    RESOURCES_TABLES = [
        {"resources": "resource_id"},
        {"assigned_resources": "resource_id"},
        {"resource_logs": "resource_id"},
        {"gantt_jobs_resources": "resource_id"},
        {"gantt_jobs_resources_log": "resource_id"},
        {"gantt_jobs_resources_visu": "resource_id"},
    ]

    db.__time_columns__ = [
        "window_start",
        "window_stop",
        "date_start",
        "date_stop",
        "last_job_date",
        "available_upto",
        "start_time",
        "date",
        "submission_time",
        "stop_time",
        "date",
    ]

    # schema = Table(
    #     "schema", Column("version", String(255)), Column("name", String(255))
    # )
