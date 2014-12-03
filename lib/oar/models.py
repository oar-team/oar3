# -*- coding: utf-8 -*-
from . import db

schema = db.Table(
    db.Column('version', db.String(255)),
    db.Column('name', db.String(255))
)


assigned_resources = db.Table("assigned_resources",
    db.Column("moldable_job_id",
              db.Integer,
              db.ForeignKey("moldable_job_descriptions.moldable_id"),
              primary_key=True),
    db.Column('resource_id',
              db.Integer,
              db.ForeignKey("resources.resource_id"),
              primary_key=True),
    db.Column('assigned_resource_index', db.String(7), index=True),
)


job_dependencies = db.Table("job_dependencies",
    db.Column("job_id",
              db.Integer,
              db.ForeignKey("jobs.job_id"),
              primary_key=True),
    db.Column("job_id_required",
              db.Integer,
              db.ForeignKey("jobs.job_id"),
              primary_key=True),
    db.Column("job_dependency_index", db.String(7), index=True),
)


class Accounting(db.Model):
    __tablename__ = 'accounting'

    window_start = db.Column(db.Integer, primary_key=True)
    window_stop = db.Column(db.Integer, primary_key=True, default="0")
    accounting_user = db.Column(db.String(255), primary_key=True, index=True, default="")
    accounting_project = db.Column(db.String(255), primary_key=True, index=True, default="")
    queue_name = db.Column(db.String(100), primary_key=True, index=True, default="")
    consumption_type = db.Column(db.String(5), primary_key=True, index=True, default="ASKED")
    consumption = db.Column(db.Integer, default="0")


class AdmissionRule(db.Model):
    __tablename__ = 'admission_rules'

    id = db.Column(db.BigInteger, primary_key=True)
    priority = db.Column(db.Integer, default="0")
    enabled = db.Column(db.String(3), default="YES")
    rule = db.Column(db.Text)


class Challenge(db.Model):
    __tablename__ = 'challenges'

    id = db.Column('job_id', db.Integer, db.ForeignKey("jobs.job_id"),
                   primary_key=True, index=True)
    challenge = db.Column(db.String(255), default="")
    ssh_private_key = db.Column(db.Text, default="")
    ssh_public_key = db.Column(db.Text, default="")


class EventLogHostname(db.Model):
    __tablename__ = 'event_log_hostnames'

    event_id = db.Column(db.Integer, db.ForeignKey("event_logs.event_id"),
                         primary_key=True)
    hostname = db.Column(db.String(255), primary_key=True, index=True)
    event_log = db.relationship('EventLog', backref='hostnames')


class EventLog(db.Model):
    __tablename__ = 'event_logs'

    id = db.Column('event_id', db.BigInteger, primary_key=True)
    type = db.Column(db.String(50), index=True, default="")
    job_id = db.Column(db.Integer, db.ForeignKey("jobs.job_id"), index=True,
                       default="0")
    date = db.Column(db.Integer, default="0")
    description = db.Column(db.String(255), default="")
    to_check = db.Column(db.String(3), index=True, default="YES")


class File(db.Model):
    __tablename__ = 'files'

    id = db.Column('file_id', db.BigInteger, primary_key=True)
    md5sum = db.Column(db.String(255), index=True, default="NULL")
    location = db.Column(db.String(255), default="NULL")
    method = db.Column(db.String(255), default="NULL")
    compression = db.Column(db.String(255), default="NULL")
    size = db.Column(db.Integer, default="0")


class FragJob(db.Model):
    __tablename__ = 'frag_jobs'

    id = db.Column('frag_id_job', db.Integer,
        db.ForeignKey("jobs.job_id"),
        primary_key=True,
    )
    date = db.Column('frag_date', db.Integer, default="0")
    state = db.Column('frag_state', db.String(16), index=True, default="LEON")


class GanttJobsPrediction(db.Model):
    __tablename__ = 'gantt_jobs_predictions'

    moldable_id = db.Column('moldable_job_id', db.Integer,
        db.ForeignKey("moldable_job_descriptions.moldable_id"),
        primary_key=True,
    )
    start_time = db.Column(db.Integer, default="0")
    moldable = db.relationship('MoldableJob')


class GanttJobsPredictionsLog(db.Model):
    __tablename__ = 'gantt_jobs_predictions_log'

    sched_date = db.Column(db.Integer, primary_key=True, default="0")
    moldable_id = db.Column('moldable_job_id', db.Integer,
        db.ForeignKey("moldable_job_descriptions.moldable_id"),
        primary_key=True,
    )
    start_time = db.Column(db.Integer, default="0")
    moldable = db.relationship('MoldableJob')


class GanttJobsPredictionsVisu(db.Model):
    __tablename__ = 'gantt_jobs_predictions_visu'

    moldable_id = db.Column('moldable_job_id', db.Integer,
        db.ForeignKey("moldable_job_descriptions.moldable_id"),
        primary_key=True,
    )
    start_time = db.Column(db.Integer, default="0")
    moldable = db.relationship('MoldableJob')


class GanttJobsResource(db.Model):
    __tablename__ = 'gantt_jobs_resources'

    moldable_id = db.Column('moldable_job_id', db.Integer,
        db.ForeignKey("moldable_job_descriptions.moldable_id"),
        primary_key=True,
    )
    resource_id = db.Column(db.Integer,
        db.ForeignKey("resources.resource_id"),
        primary_key=True,
    )
    moldable = db.relationship('MoldableJob')
    resource = db.relationship('Resource')


class GanttJobsResourcesLog(db.Model):
    __tablename__ = 'gantt_jobs_resources_log'

    sched_date = db.Column(db.Integer, primary_key=True, default="0")
    moldable_id = db.Column('moldable_job_id', db.Integer,
        db.ForeignKey("moldable_job_descriptions.moldable_id"),
        primary_key=True,
    )
    resource_id = db.Column(db.Integer,
        db.ForeignKey("resources.resource_id"),
        primary_key=True,
    )
    moldable = db.relationship('MoldableJob')
    resource = db.relationship('Resource')



class GanttJobsResourcesVisu(db.Model):
    __tablename__ = 'gantt_jobs_resources_visu'

    moldable_id = db.Column('moldable_job_id', db.Integer,
        db.ForeignKey("moldable_job_descriptions.moldable_id"),
        primary_key=True,
    )
    resource_id = db.Column(db.Integer,
        db.ForeignKey("resources.resource_id"),
        primary_key=True,
    )

    moldable = db.relationship('MoldableJob')
    resource = db.relationship('Resource')


class JobStateLog(db.Model):
    __tablename__ = 'job_state_logs'

    id = db.Column('job_state_log_id', db.BigInteger, primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey("jobs.job_id"), index=True,
                       default="0")
    job_state = db.Column(db.String(16), index=True, default="Waiting")
    date_start = db.Column(db.Integer, default="0")
    date_stop = db.Column(db.Integer, default="0")


class JobType(db.Model):
    __tablename__ = 'job_types'

    id = db.Column('job_type_id', db.BigInteger, primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey("jobs.job_id"), index=True,
                       default="0")
    type = db.Column(db.String(255), index=True, default="")
    types_index = db.Column(db.String(7), index=True, default="CURRENT")


class Job(db.Model):
    __tablename__ = 'jobs'
    __table_args__ = (
        db.Index('state_id', 'state', 'job_id'),
    )

    id = db.Column('job_id', db.BigInteger, primary_key=True)
    array_id = db.Column(db.Integer, index=True, default="0")
    array_index = db.Column(db.Integer, default="1")
    initial_request = db.Column(db.Text)
    name = db.Column('job_name', db.String(100))
    env = db.Column('job_env', db.Text)
    type = db.Column('job_type', db.String(11), default="PASSIVE")
    info_type = db.Column(db.String(255), default="NULL")
    state = db.Column(db.String(16), index=True, default="Waiting")
    reservation = db.Column(db.String(10), index=True, default="None")
    message = db.Column(db.String(255), default="")
    scheduler_info = db.Column(db.String(255), default="")
    user = db.Column('job_user', db.String(255), default="")
    project = db.Column(db.String(255), default="")
    group = db.Column('job_group', db.String(255), default="")
    command = db.Column(db.Text)
    exit_code = db.Column(db.Integer)
    queue_name = db.Column(db.String(100), db.ForeignKey('queues.queue_name'),
                           index=True, default="")
    properties = db.Column(db.Text)
    launching_directory = db.Column(db.Text)
    submission_time = db.Column(db.Integer, default="0")
    start_time = db.Column(db.Integer, default="0")
    stop_time = db.Column(db.Integer, default="0")
    file_id = db.Column(db.Integer, db.ForeignKey("files.file_id"))
    accounted = db.Column(db.String(3), index=True, default="NO")
    notify = db.Column(db.String(255), default="NULL")
    assigned_moldable_job = db.Column(db.Integer, default="0")
    checkpoint = db.Column(db.Integer, default="0")
    checkpoint_signal = db.Column(db.Integer)
    stdout_file = db.Column(db.Text)
    stderr_file = db.Column(db.Text)
    resubmit_job_id = db.Column(db.Integer, default="0")
    suspended = db.Column(db.String(3), index=True, default="NO")

    ## relations
    queue = db.relationship('Queue', backref='jobs')
    file = db.relationship('File', backref='jobs')

    depends_on_jobs = db.relationship("Job",
        secondary=job_dependencies,
        primaryjoin=id==job_dependencies.c.job_id,
        secondaryjoin=id==job_dependencies.c.job_id_required,
        backref="needed_by_jobs",
    )
    frag = db.relationship('FragJob', backref="job", uselist=False)
    chalenge = db.relationship('Challenge', backref='job', uselist=False)
    state_logs = db.relationship('JobStateLog', backref='job')
    types = db.relationship('JobType', backref='job')
    event_logs = db.relationship('EventLog', backref='job')
    ## relations
    moldables = db.relationship('MoldableJob', backref='job')


class MoldableJob(db.Model):
    __tablename__ = 'moldable_job_descriptions'

    id = db.Column('moldable_id', db.BigInteger, primary_key=True)
    job_id = db.Column('moldable_job_id', db.Integer,
                       db.ForeignKey("jobs.job_id"), index=True, default="0")
    walltime = db.Column('moldable_walltime', db.Integer, default="0")
    index = db.Column('moldable_index', db.String(7), index=True,
                      default="CURRENT")
    groups = db.relationship('JobResourceGroup', backref='moldable')


class JobResourceGroup(db.Model):
    __tablename__ = 'job_resource_groups'

    id = db.Column('res_group_id', db.BigInteger, primary_key=True)
    moldable_id = db.Column('res_group_moldable_id', db.Integer,
        db.ForeignKey("moldable_job_descriptions.moldable_id"),
        index=True,
    )
    property = db.Column('res_group_property', db.Text)
    index = db.Column('res_group_index', db.String(7), index=True)
    ## relations
    descriptions = db.relationship('JobResourceDescription', backref='group')


class JobResourceDescription(db.Model):
    __tablename__ = 'job_resource_descriptions'

    resource_type = db.Column('res_job_resource_type',
        db.String(255),
        primary_key=True,
    )
    group_id = db.Column('res_job_group_id',
        db.Integer,
        db.ForeignKey("job_resource_groups.res_group_id"),
        primary_key=True,
        index=True,
    )
    value = db.Column('res_job_value', db.Integer, default="0")
    order = db.Column('res_job_order', db.Integer, primary_key=True, default="0")
    index = db.Column('res_job_index', db.String(7), index=True, default="CURRENT")


class Queue(db.Model):
    __tablename__ = 'queues'

    name = db.Column('queue_name', db.String(100), primary_key=True, default="")
    priority = db.Column(db.Integer, default="0")
    scheduler_policy = db.Column(db.String(100), default="")
    state = db.Column(db.String(9), default="Active")


class ResourceLog(db.Model):
    __tablename__ = 'resource_logs'

    id = db.Column('resource_log_id', db.BigInteger, primary_key=True)
    resource_id = db.Column(db.Integer,
        db.ForeignKey("resources.resource_id"),
        index=True,
    )
    attribute = db.Column(db.String(255), index=True, default="")
    value = db.Column(db.String(255), index=True, default="")
    date_start = db.Column(db.Integer, index=True, default="0")
    date_stop = db.Column(db.Integer, index=True, default="0")
    finaud_decision = db.Column(db.String(3), index=True, default="NO")


class Resource(db.DeferredReflection, db.Model):
    __tablename__ = 'resources'

    id = db.Column('resource_id', db.BigInteger, primary_key=True)
    type = db.Column(db.String(100), index=True, default="default")
    network_address = db.Column(db.String(100), index=True, default="")
    state = db.Column(db.String(9), index=True, default="Alive")
    next_state = db.Column(db.String(9), index=True, default="UnChanged")
    finaud_decision = db.Column(db.String(3), default="NO")
    next_finaud_decision = db.Column(db.String(3), default="NO")
    state_num = db.Column(db.Integer, default=0)
    suspended_jobs = db.Column(db.String(3), index=True, default="NO")
    scheduler_priority = db.Column(db.Integer, default=0)
    cpuset = db.Column(db.String(255), default="0")
    besteffort = db.Column(db.String(3), default="YES")
    deploy = db.Column(db.String(3), default="NO")
    expiry_date = db.Column(db.Integer, default=0)
    desktop_computing = db.Column(db.String(3), default="NO")
    last_job_date = db.Column(db.Integer, default=0)
    available_upto = db.Column(db.Integer, default=2147483647)
    last_available_upto = db.Column(db.Integer, default=0)
    drain = db.Column(db.String(3), default="NO")

    ## relations
    logs = db.relationship('ResourceLog', backref='resource')
    assigned_to_moldable = db.relationship("MoldableJob",
        secondary=assigned_resources,
        backref="assigned_ressources",
    )

class Scheduler(db.Model):
    __tablename__ = 'scheduler'

    name = db.Column(db.String(100), primary_key=True)
    script = db.Column(db.String(100))
    description = db.Column(db.String(255))
