# -*- coding: utf-8 -*-
from . import db


schema = db.Table('schema',
    db.Column('version', db.String(255)),
    db.Column('name', db.String(255))
)


class Accounting(db.Model):
    __tablename__ = 'accounting'

    window_start = db.Column(db.Integer, primary_key=True)
    window_stop = db.Column(db.Integer, primary_key=True)
    accounting_user = db.Column(db.String(255), primary_key=True, index=True)
    accounting_project = db.Column(db.String(255),
                                   primary_key=True,
                                   index=True)
    queue_name = db.Column(db.String(100), primary_key=True, index=True)
    consumption_type = db.Column(db.String(5), primary_key=True, index=True)
    consumption = db.Column(db.Integer)


class AdmissionRule(db.Model):
    __tablename__ = 'admission_rules'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column(db.Integer, primary_key=True)
    priority = db.Column(db.Integer, default=0)
    enabled = db.Column(db.String(3), default="YES")
    rule = db.Column(db.Text)


class AssignedResource(db.Model):
    __tablename__ = 'assigned_resources'

    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True,
                            index=True)
    resource_id = db.Column(db.Integer, primary_key=True)
    assigned_resource_index = db.Column(db.String(7),
                                        index=True,
                                        default="CURRENT")


class Challenge(db.Model):
    __tablename__ = 'challenges'

    job_id = db.Column(db.Integer, primary_key=True, index=True)
    challenge = db.Column(db.String(255), default="")
    ssh_private_key = db.Column(db.Text, default="")
    ssh_public_key = db.Column(db.Text, default="")


class EventLogHostname(db.Model):
    __tablename__ = 'event_log_hostnames'

    event_id = db.Column(db.Integer, primary_key=True)
    hostname = db.Column(db.String(255), primary_key=True, index=True)


class EventLog(db.Model):
    __tablename__ = 'event_logs'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column('event_id', db.Integer, primary_key=True)
    type = db.Column(db.String(50), index=True, default="")
    job_id = db.Column(db.Integer, index=True, default=0)
    date = db.Column(db.Integer, default=0)
    description = db.Column(db.String(255), default="")
    to_check = db.Column(db.String(3), index=True, default="YES")


class File(db.Model):
    __tablename__ = 'files'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column('file_id', db.Integer, primary_key=True)
    md5sum = db.Column(db.String(255), index=True)
    location = db.Column(db.String(255), nullable=True)
    method = db.Column(db.String(255), nullable=True)
    compression = db.Column(db.String(255), nullable=True)
    size = db.Column(db.Integer, default=0)


class FragJob(db.Model):
    __tablename__ = 'frag_jobs'
    __table_args__ = {'sqlite_autoincrement': True}

    job_id = db.Column('frag_id_job', db.Integer, primary_key=True)
    date = db.Column('frag_date', db.Integer, default=0)
    state = db.Column('frag_state', db.String(16), index=True,default="LEON")


class GanttJobsPrediction(db.Model):
    __tablename__ = 'gantt_jobs_predictions'

    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True)
    start_time = db.Column(db.Integer, default=0)


class GanttJobsPredictionsLog(db.Model):
    __tablename__ = 'gantt_jobs_predictions_log'

    sched_date = db.Column(db.Integer, primary_key=True)
    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True)
    start_time = db.Column(db.Integer)


class GanttJobsPredictionsVisu(db.Model):
    __tablename__ = 'gantt_jobs_predictions_visu'

    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True)
    start_time = db.Column(db.Integer)


class GanttJobsResource(db.Model):
    __tablename__ = 'gantt_jobs_resources'

    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True)
    resource_id = db.Column(db.Integer, primary_key=True)


class GanttJobsResourcesLog(db.Model):
    __tablename__ = 'gantt_jobs_resources_log'

    sched_date = db.Column(db.Integer, primary_key=True)
    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True)
    resource_id = db.Column(db.Integer, primary_key=True)


class GanttJobsResourcesVisu(db.Model):
    __tablename__ = 'gantt_jobs_resources_visu'

    moldable_id = db.Column('moldable_job_id', db.Integer, primary_key=True)
    resource_id = db.Column(db.Integer, primary_key=True)


class JobDependencie(db.Model):
    __tablename__ = 'job_dependencies'

    job_id = db.Column(db.Integer, primary_key=True, index=True)
    job_id_required = db.Column(db.Integer, primary_key=True)
    index = db.Column("job_dependency_index",
                      db.String(7),
                      index=True,
                      default="CURRENT")


class JobResourceDescription(db.Model):
    __tablename__ = 'job_resource_descriptions'

    group_id = db.Column('res_job_group_id',
                         db.Integer,
                         primary_key=True,
                         index=True)
    resource_type = db.Column('res_job_resource_type',
                              db.String(255),
                              primary_key=True)
    value = db.Column('res_job_value', db.Integer, default=0)
    order = db.Column('res_job_order', db.Integer, primary_key=True, default=0)
    index = db.Column('res_job_index',
                      db.String(7),
                      index=True,
                      default="CURRENT")


class JobResourceGroup(db.Model):
    __tablename__ = 'job_resource_groups'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column('res_group_id', db.Integer, primary_key=True)
    moldable_id = db.Column('res_group_moldable_id', db.Integer, index=True)
    property = db.Column('res_group_property', db.Text)
    index = db.Column('res_group_index',
                      db.String(7),
                      index=True,
                      default="CURRENT")


class JobStateLog(db.Model):
    __tablename__ = 'job_state_logs'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column('job_state_log_id', db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, index=True, default=0)
    job_state = db.Column(db.String(16),
                          index=True,
                          default="Waiting")
    date_start = db.Column(db.Integer, default=0)
    date_stop = db.Column(db.Integer, default=0)


class JobType(db.Model):
    __tablename__ = 'job_types'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column('job_type_id', db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, index=True)
    type = db.Column(db.String(255), index=True, default="")
    types_index = db.Column(db.String(7), index=True, default="CURRENT")


class Job(db.Model):
    __tablename__ = 'jobs'
    __table_args__ = (
        db.Index('state_id', 'state', 'job_id'),
        {'sqlite_autoincrement': True}
    )

    id = db.Column('job_id', db.Integer, primary_key=True)
    array_id = db.Column(db.Integer, index=True, default=0)
    array_index = db.Column(db.Integer, default=1)
    initial_request = db.Column(db.Text, nullable=True)
    name = db.Column('job_name', db.String(100), nullable=True)
    env = db.Column('job_env', db.Text, nullable=True)
    type = db.Column('job_type',
                     db.String(11),
                     default="PASSIVE")
    info_type = db.Column(db.String(255), default="", nullable=True)
    state = db.Column(db.String(16),
                      index=True,
                      default="Waiting")
    reservation = db.Column(db.String(10),
                            index=True,
                            default="None")
    message = db.Column(db.String(255), default="")
    scheduler_info = db.Column(db.String(255), default="")
    user = db.Column('job_user', db.String(255), default="")
    project = db.Column(db.String(255), default="")
    group = db.Column('job_group', db.String(255), default="")
    command = db.Column(db.Text, nullable=True)
    exit_code = db.Column(db.Integer, nullable=True)
    queue_name = db.Column(db.String(100), index=True, default="")
    properties = db.Column(db.Text, nullable=True)
    launching_directory = db.Column(db.Text)
    submission_time = db.Column(db.Integer, default=0)
    start_time = db.Column(db.Integer, default=0)
    stop_time = db.Column(db.Integer, default=0)
    file_id = db.Column(db.Integer, nullable=True)
    accounted = db.Column(db.String(3), index=True, default="NO")
    notify = db.Column(db.String(255), default="NULL", nullable=True)
    assigned_moldable_job = db.Column(db.Integer, default=0, nullable=True)
    checkpoint = db.Column(db.Integer, default=0)
    checkpoint_signal = db.Column(db.Integer)
    stdout_file = db.Column(db.Text, nullable=True)
    stderr_file = db.Column(db.Text, nullable=True)
    resubmit_job_id = db.Column(db.Integer, default=0)
    suspended = db.Column(db.String(3), index=True, default="NO")


class MoldableJobDescription(db.Model):
    __tablename__ = 'moldable_job_descriptions'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column('moldable_id', db.Integer, primary_key=True)
    job_id = db.Column('moldable_job_id',
                       db.Integer,
                       index=True,
                       default=0)
    walltime = db.Column('moldable_walltime', db.Integer, default=0)
    index = db.Column('moldable_index',
                      db.String(7),
                      index=True,
                      default="CURRENT")


class Queue(db.Model):
    __tablename__ = 'queues'

    name = db.Column('queue_name', db.String(100), primary_key=True)
    priority = db.Column(db.Integer, default=0)
    scheduler_policy = db.Column(db.String(100), default="")
    state = db.Column(db.String(9), default="Active")


class ResourceLog(db.Model):
    __tablename__ = 'resource_logs'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column('resource_log_id', db.Integer, primary_key=True)
    resource_id = db.Column(db.Integer, index=True)
    attribute = db.Column(db.String(255), index=True, default="")
    value = db.Column(db.String(255), index=True, default="")
    date_start = db.Column(db.Integer, index=True, default=0)
    date_stop = db.Column(db.Integer, index=True, default=0)
    finaud_decision = db.Column(db.String(3), index=True, default="NO")


class Resource(db.DeferredReflection, db.Model):
    __tablename__ = 'resources'
    __table_args__ = {'sqlite_autoincrement': True}

    id = db.Column('resource_id', db.Integer, primary_key=True)
    type = db.Column(db.String(100), index=True, default="default")
    network_address = db.Column(db.String(100), index=True, default="")
    state = db.Column(db.String(9), index=True, default="Alive")
    next_state = db.Column(db.String(9), index=True, default="UnChanged")
    finaud_decision = db.Column(db.String(3), default="NO")
    next_finaud_decision = db.Column(db.String(3), default="NO")
    state_num = db.Column(db.Integer, default=0)
    suspended_jobs = db.Column(db.String(3), index=True, default="NO")
    scheduler_priority = db.Column(db.Integer, default=0)
    cpuset = db.Column(db.String(255), default=0)
    besteffort = db.Column(db.String(3), default="YES")
    deploy = db.Column(db.String(3), default="NO")
    expiry_date = db.Column(db.Integer, default=0)
    desktop_computing = db.Column(db.String(3), default="NO")
    last_job_date = db.Column(db.Integer, default=0)
    available_upto = db.Column(db.Integer, default=2147483647)
    last_available_upto = db.Column(db.Integer, default=0)
    drain = db.Column(db.String(3), default="NO")


class Scheduler(db.Model):
    __tablename__ = 'scheduler'

    name = db.Column(db.String(100), primary_key=True)
    script = db.Column(db.String(100))
    description = db.Column(db.String(255))


def all_models():
    import sys
    import inspect
    from sqlalchemy.ext.declarative.api import DeclarativeMeta

    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and isinstance(obj, DeclarativeMeta):
            yield name, obj
