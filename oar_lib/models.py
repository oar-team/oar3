# -*- coding: utf-8 -*-
import sys
import inspect
from sqlalchemy.orm import configure_mappers
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy import Table
from . import db


JOBS_TABLES = [
    {'jobs': 'job_id'},
    {'challenges': 'job_id'},
    {'event_logs': 'job_id'},
    {'frag_jobs': 'frag_id_job'},
    {'job_dependencies': 'job_id'},
    {'job_dependencies': 'job_id_required'},
    {'job_state_logs': 'job_id'},
    {'job_types': 'job_id'},
    {'moldable_job_descriptions': 'moldable_job_id'},
]

MOLDABLES_JOBS_TABLES = [
    {'moldable_job_descriptions': 'moldable_id'},
    {'assigned_resources': 'moldable_job_id'},
    {'job_resource_groups': 'res_group_moldable_id'},
    {'gantt_jobs_predictions': 'moldable_job_id'},
    {'gantt_jobs_predictions_log': 'moldable_job_id'},
    {'gantt_jobs_predictions_visu': 'moldable_job_id'},
    {'gantt_jobs_resources': 'moldable_job_id'},
    {'gantt_jobs_resources_log': 'moldable_job_id'},
    {'gantt_jobs_resources_visu': 'moldable_job_id'},
]

RESOURCES_TABLES = [
    {'resources': 'resource_id'},
    {'assigned_resources': 'resource_id'},
    {'resource_logs': 'resource_id'},
    {'gantt_jobs_resources': 'resource_id'},
    {'gantt_jobs_resources_log': 'resource_id'},
    {'gantt_jobs_resources_visu': 'resource_id'},
]

schema = db.Table('schema',
                  db.Column('version', db.String(255)),
                  db.Column('name', db.String(255))
                  )


class Accounting(db.Model):
    __tablename__ = 'accounting'

    window_start = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    window_stop = db.Column(db.BigInteger, primary_key=True, autoincrement=False, server_default='0')
    user = db.Column('accounting_user', db.String(255), primary_key=True, index=True, server_default='')
    project = db.Column('accounting_project', db.String(255), primary_key=True, index=True, server_default='')
    queue_name = db.Column(db.String(100), primary_key=True, index=True, server_default='')
    consumption_type = db.Column(db.String(5), primary_key=True, index=True, server_default='ASKED')
    consumption = db.Column(db.BigInteger, server_default='0')


class AdmissionRule(db.Model):
    __tablename__ = 'admission_rules'

    id = db.Column(db.Integer, primary_key=True)
    priority = db.Column(db.Integer, server_default='0')
    enabled = db.Column(db.String(3), server_default='YES')
    rule = db.Column(db.Text)


class AssignedResource(db.Model):
    __tablename__ = 'assigned_resources'

    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True, autoincrement=False, index=True, server_default='0')
    resource_id = db.Column(db.Integer, primary_key=True, server_default='0')
    assigned_resource_index = db.Column(db.String(7), index=True, server_default='CURRENT')


class Challenge(db.Model):
    __tablename__ = 'challenges'

    job_id = db.Column(db.Integer, primary_key=True, autoincrement=False, index=True, server_default='0')
    challenge = db.Column(db.String(255), server_default='')
    ssh_private_key = db.Column(db.Text, server_default='')
    ssh_public_key = db.Column(db.Text, server_default='')


class EventLogHostname(db.Model):
    __tablename__ = 'event_log_hostnames'

    event_id = db.Column(db.Integer, primary_key=True, autoincrement=False, server_default='0')
    hostname = db.Column(db.String(255), primary_key=True, index=True, server_default='')


class EventLog(db.Model):
    __tablename__ = 'event_logs'

    id = db.Column('event_id', db.Integer, primary_key=True)
    type = db.Column(db.String(50), index=True, server_default='')
    job_id = db.Column(db.Integer, index=True, server_default='0')
    date = db.Column(db.Integer, server_default='0')
    description = db.Column(db.String(255), server_default='')
    to_check = db.Column(db.String(3), index=True, server_default='YES')


class File(db.Model):
    __tablename__ = 'files'

    id = db.Column('file_id', db.Integer, primary_key=True)
    md5sum = db.Column(db.String(255), index=True, nullable=True, server_default=db.text('NULL'))
    location = db.Column(db.String(255), nullable=True, server_default=db.text('NULL'))
    method = db.Column(db.String(255), nullable=True, server_default=db.text('NULL'))
    compression = db.Column(db.String(255), nullable=True, server_default=db.text('NULL'))
    size = db.Column(db.Integer, server_default='0')


class FragJob(db.Model):
    __tablename__ = 'frag_jobs'

    job_id = db.Column('frag_id_job', db.Integer, primary_key=True, server_default='0')
    date = db.Column('frag_date', db.Integer, server_default='0')
    state = db.Column('frag_state', db.String(16), index=True, server_default="LEON")


class GanttJobsPrediction(db.Model):
    __tablename__ = 'gantt_jobs_predictions'

    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True, autoincrement=False, server_default='0')
    start_time = db.Column(db.Integer, server_default='0')


class GanttJobsPredictionsLog(db.Model):
    __tablename__ = 'gantt_jobs_predictions_log'

    sched_date = db.Column(db.Integer, primary_key=True, autoincrement=False, server_default='0')
    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True, server_default='0')
    start_time = db.Column(db.Integer, server_default='0')


class GanttJobsPredictionsVisu(db.Model):
    __tablename__ = 'gantt_jobs_predictions_visu'

    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True, autoincrement=False, server_default='0')
    start_time = db.Column(db.Integer, server_default='0')


class GanttJobsResource(db.Model):
    __tablename__ = 'gantt_jobs_resources'

    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True, autoincrement=False, server_default='0')
    resource_id = db.Column(db.Integer, primary_key=True, server_default='0')


class GanttJobsResourcesLog(db.Model):
    __tablename__ = 'gantt_jobs_resources_log'

    sched_date = db.Column(db.Integer, primary_key=True, autoincrement=False, server_default='0')
    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True, server_default='0')
    resource_id = db.Column(db.Integer, primary_key=True, server_default='0')


class GanttJobsResourcesVisu(db.Model):
    __tablename__ = 'gantt_jobs_resources_visu'

    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True, autoincrement=False, server_default='0')
    resource_id = db.Column(db.Integer, primary_key=True, server_default='0')


class JobDependencie(db.Model):
    __tablename__ = 'job_dependencies'

    job_id = db.Column(db.Integer, primary_key=True, index=True, autoincrement=False, server_default='0')
    job_id_required = db.Column(db.Integer, primary_key=True, server_default='0')
    index = db.Column("job_dependency_index", db.String(7), index=True, server_default='CURRENT')


class JobResourceDescription(db.Model):
    __tablename__ = 'job_resource_descriptions'

    group_id = db.Column('res_job_group_id', db.Integer, primary_key=True, autoincrement=False, index=True, server_default='0')
    resource_type = db.Column('res_job_resource_type', db.String(255), primary_key=True, server_default='')
    value = db.Column('res_job_value', db.Integer, server_default='0')
    order = db.Column('res_job_order', db.Integer, primary_key=True, server_default='0')
    index = db.Column('res_job_index', db.String(7), index=True, server_default='CURRENT')


class JobResourceGroup(db.Model):
    __tablename__ = 'job_resource_groups'

    id = db.Column('res_group_id', db.Integer, primary_key=True)
    moldable_id = db.Column('res_group_moldable_id', db.Integer, index=True, server_default='0')
    property = db.Column('res_group_property', db.Text, nullable=True)
    index = db.Column('res_group_index', db.String(7), index=True, server_default='CURRENT')


class JobStateLog(db.Model):
    __tablename__ = 'job_state_logs'

    id = db.Column('job_state_log_id', db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, index=True, server_default='0')
    job_state = db.Column(db.String(16), index=True, server_default='Waiting')
    date_start = db.Column(db.Integer, server_default='0')
    date_stop = db.Column(db.Integer, server_default='0')


class JobType(db.Model):
    __tablename__ = 'job_types'

    id = db.Column('job_type_id', db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, index=True, server_default='0')
    type = db.Column(db.String(255), index=True, server_default='')
    types_index = db.Column(db.String(7), index=True, server_default='CURRENT')


class Job(db.DeferredReflection, db.Model):
    __tablename__ = 'jobs'
    __extra_table_args__ = (db.Index('state_id', 'state', 'job_id'), )

    id = db.Column('job_id', db.Integer, primary_key=True)
    array_id = db.Column(db.Integer, index=True, server_default='0')
    array_index = db.Column(db.Integer, server_default='1')
    initial_request = db.Column(db.Text, nullable=True)
    name = db.Column('job_name', db.String(100), nullable=True)
    env = db.Column('job_env', db.Text, nullable=True)
    type = db.Column('job_type', db.String(11), server_default='PASSIVE')
    info_type = db.Column(db.String(255), nullable=True, server_default=db.text('NULL'))
    state = db.Column(db.String(16), index=True, server_default='Waiting')
    reservation = db.Column(db.String(10), index=True, server_default='None')
    message = db.Column(db.String(255), server_default='')
    scheduler_info = db.Column(db.String(255), server_default='')
    user = db.Column('job_user', db.String(255), server_default='')
    project = db.Column(db.String(255), server_default='')
    group = db.Column('job_group', db.String(255), server_default='')
    command = db.Column(db.Text, nullable=True)
    exit_code = db.Column(db.Integer, nullable=True)
    queue_name = db.Column(db.String(100), index=True, server_default='')
    properties = db.Column(db.Text, nullable=True)
    launching_directory = db.Column(db.Text)
    submission_time = db.Column(db.Integer, server_default='0')
    start_time = db.Column(db.Integer, server_default='0')
    stop_time = db.Column(db.Integer, server_default='0')
    file_id = db.Column(db.Integer, nullable=True)
    accounted = db.Column(db.String(3), index=True, server_default='NO')
    notify = db.Column(db.String(255), nullable=True, server_default=db.text('NULL'))
    assigned_moldable_job = db.Column(db.Integer, nullable=True, server_default='0')
    checkpoint = db.Column(db.Integer, server_default='0')
    checkpoint_signal = db.Column(db.Integer)
    stdout_file = db.Column(db.Text, nullable=True)
    stderr_file = db.Column(db.Text, nullable=True)
    resubmit_job_id = db.Column(db.Integer, server_default='0')
    suspended = db.Column(db.String(3), index=True, server_default='NO')


class MoldableJobDescription(db.Model):
    __tablename__ = 'moldable_job_descriptions'

    id = db.Column('moldable_id', db.Integer, primary_key=True)
    job_id = db.Column('moldable_job_id', db.Integer, index=True, server_default='0')
    walltime = db.Column('moldable_walltime', db.Integer, server_default='0')
    index = db.Column('moldable_index', db.String(7), index=True, server_default='CURRENT')


class Queue(db.Model):
    __tablename__ = 'queues'

    name = db.Column('queue_name', db.String(100), primary_key=True, server_default='')
    priority = db.Column(db.Integer, server_default='0')
    scheduler_policy = db.Column(db.String(100), server_default='')
    state = db.Column(db.String(9), server_default='Active')


class ResourceLog(db.Model):
    __tablename__ = 'resource_logs'

    id = db.Column('resource_log_id', db.Integer, primary_key=True)
    resource_id = db.Column(db.Integer, index=True, server_default='0')
    attribute = db.Column(db.String(255), index=True, server_default='')
    value = db.Column(db.String(255), index=True, server_default='')
    date_start = db.Column(db.Integer, index=True, server_default='0')
    date_stop = db.Column(db.Integer, index=True, server_default='0')
    finaud_decision = db.Column(db.String(3), index=True, server_default='NO')


class Resource(db.DeferredReflection, db.Model):
    __tablename__ = 'resources'

    id = db.Column('resource_id', db.Integer, primary_key=True)
    type = db.Column(db.String(100), index=True, server_default='default')
    network_address = db.Column(db.String(100), index=True, server_default='')
    state = db.Column(db.String(9), index=True, server_default='Alive')
    next_state = db.Column(db.String(9), index=True, server_default='UnChanged')
    finaud_decision = db.Column(db.String(3), server_default='NO')
    next_finaud_decision = db.Column(db.String(3), server_default='NO')
    state_num = db.Column(db.Integer, server_default='0')
    suspended_jobs = db.Column(db.String(3), index=True, server_default='NO')
    scheduler_priority = db.Column(db.Integer, server_default='0')
    cpuset = db.Column(db.String(255), server_default='0')
    besteffort = db.Column(db.String(3), server_default='YES')
    deploy = db.Column(db.String(3), server_default='NO')
    expiry_date = db.Column(db.Integer, server_default='0')
    desktop_computing = db.Column(db.String(3), server_default='NO')
    last_job_date = db.Column(db.Integer, server_default='0')
    available_upto = db.Column(db.Integer, server_default='2147483647')
    last_available_upto = db.Column(db.Integer, server_default='0')
    drain = db.Column(db.String(3), server_default='NO')


class Scheduler(db.Model):
    __tablename__ = 'scheduler'

    name = db.Column(db.String(100), primary_key=True)
    script = db.Column(db.String(100))
    description = db.Column(db.String(255))


configure_mappers()


def all_models():
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and isinstance(obj, DeclarativeMeta):
            yield name, obj


def all_tables():
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and isinstance(obj, DeclarativeMeta):
            yield name, obj.__table__
        elif isinstance(obj, Table):
            yield name, obj
