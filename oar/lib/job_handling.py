# coding: utf-8
""" Functions to handle jobs"""
import copy
import os
import random
import re

from procset import ProcSet
from sqlalchemy import distinct, func, text
from sqlalchemy.orm import aliased
from sqlalchemy.orm.session import make_transient
from sqlalchemy.sql import case
from sqlalchemy.sql.expression import select

import oar.lib.tools as tools
from oar.kao.helpers import extract_find_assign_args
from oar.lib import (
    AssignedResource,
    Challenge,
    FragJob,
    GanttJobsPrediction,
    GanttJobsResource,
    Job,
    JobDependencie,
    JobResourceDescription,
    JobResourceGroup,
    JobStateLog,
    JobType,
    MoldableJobDescription,
    Resource,
    WalltimeChange,
    config,
    db,
    get_logger,
)
from oar.lib.event import add_new_event, add_new_event_with_host, is_an_event_exists
from oar.lib.psycopg2 import pg_bulk_insert
from oar.lib.resource_handling import (
    get_current_resources_with_suspended_job,
    update_current_scheduler_priority,
)
from oar.lib.tools import (
    TimeoutExpired,
    format_ssh_pub_key,
    get_private_ssh_key_file_name,
    limited_dict2hash_perl,
)

# from oar.lib.utils import render_query

logger = get_logger("oar.lib.job_handling")


""" Use

    j1 = Job(1,"Waiting", 0, 0, "yop", "", "",{}, [], 0,
                 [
                     (1, 60,
                      [  ( [("node", 2)], [(1,32)] )  ]
                  )
                 ]
        )

    Attributes:

    mld_res_rqts: Resources requets by moldable instance
                  [                  # first moldable instance
                     (1, 60,         # moldable id, walltime
                      # list of requests composed of
                      [  ( [("node", 2), ("resource_id", 4)], [(1,32)] ) ]
                  )                  # list of hierarchy request and filtered
                 ]                   # resources (Properties)

"""

NO_PLACEHOLDER = 0
PLACEHOLDER = 1
ALLOW = 2


class JobPseudo(object):
    """Define a simple job class without database counter part"""

    types = {}
    deps = []
    key_cache = {}
    ts = False
    ph = 0
    assign = False
    assign_args = ()
    assign_kwargs = {}
    find = False
    find_args = ()
    find_kwargs = {}
    queue_name = "default"
    user = ""
    project = ""
    no_quotas = False

    def __init__(self, **kwargs):
        self.mld_res_rqts = []
        for key, value in kwargs.items():
            setattr(self, key, value)

    def simple_req(self, resources_req, walltime, resources_constraint):
        """Allow declaration of simple resources request.
        Examples:
        - j.simple_req(('node', 2), 60, [(1, 32)])
        - j.simple_req([('node', 2), ('core', 4)], 600, [(1, 64)])
        """
        if type(resources_req) == tuple:
            res_req = [resources_req]
        else:
            res_req = resources_req
        self.mld_res_rqts = [(1, walltime, [(res_req, ProcSet(*resources_constraint))])]


def get_waiting_jobs(queues, reservation="None"):
    # TODO fairsharing_nb_job_limit
    waiting_jobs = {}
    waiting_jids = []
    nb_waiting_jobs = 0

    query = db.query(Job).filter(Job.state == "Waiting")
    if isinstance(queues, str):
        query = query.filter(Job.queue_name == queues)
    else:
        query = query.filter(Job.queue_name.in_(tuple(queues)))

    query = query.filter(Job.reservation == reservation).order_by(Job.id)

    for j in query.all():
        jid = int(j.id)
        waiting_jobs[jid] = j
        waiting_jids.append(jid)
        nb_waiting_jobs += 1

    return (waiting_jobs, waiting_jids, nb_waiting_jobs)


def get_jobs_types(jids, jobs):
    import oar.kao.custom_scheduling

    jobs_types = {}
    for j_type in db.query(JobType).filter(JobType.job_id.in_(tuple(jids))):
        jid = j_type.job_id
        job = jobs[jid]
        t_v = j_type.type.split("=")
        t = t_v[0]
        if t == "timesharing":
            job.ts = True
            job.ts_user, job.ts_name = t_v[1].split(",")
        elif t == "placeholder":
            job.ph = PLACEHOLDER
            job.ph_name = t_v[1]
        elif t == "allow":
            job.ph = ALLOW
            job.ph_name = t_v[1]
        elif t == "assign":
            job.assign = True
            raw_args = "=".join(t_v[1:])
            funcname, job.assign_args, job.assign_kwargs = extract_find_assign_args(
                raw_args
            )
            job.assign_func = getattr(oar.kao.custom_scheduling, "assign_%s" % funcname)
        elif t == "find":
            job.find = True
            raw_args = "=".join(t_v[1:])
            funcname, job.find_args, job.find_kwargs = extract_find_assign_args(
                raw_args
            )
            job.find_func = getattr(oar.kao.custom_scheduling, "find_%s" % funcname)
        elif t == "no_quotas":
            job.no_quotas = True
        else:
            if len(t_v) == 2:
                v = t_v[1]
            else:
                v = ""
            if jid not in jobs_types:
                jobs_types[jid] = dict()

            (jobs_types[jid])[t] = v

    for job in jobs.values():
        if job.id in jobs_types:
            job.types = jobs_types[job.id]
        else:
            job.types = {}


def set_jobs_cache_keys(jobs):
    """
    Set keys for job use by slot_set cache to speed up the search of suitable
    slots.

    Jobs with timesharing, placeholder or dependencies requirements are not
    suitable for this cache feature. Jobs in container might leverage of cache
    because container is link to a particular slot_set.

    For jobs with dependencies, they do not update the cache entries.

    """
    for job_id, job in jobs.items():
        if (not job.ts) and (job.ph == NO_PLACEHOLDER):
            for res_rqt in job.mld_res_rqts:
                (moldable_id, walltime, hy_res_rqts) = res_rqt
                job.key_cache[int(moldable_id)] = str(walltime) + str(hy_res_rqts)


def get_data_jobs(jobs, jids, resource_set, job_security_time, besteffort_duration=0):
    """
    oarsub -q test \
        -l "nodes=1+{network_address='node3'}/nodes=1/resource_id=1" \
        sleep

    job_id: 12 [(16L,
                 7200,
                 [
                     (
                         [(u'network_address', 1)],
                         [(0, 7)]
                     ),
                     (
                         [(u'network_address', 1), (u'resource_id', 1)],
                         [(4, 7)]
                     )
                 ])]

    """

    result = (
        db.query(
            Job.id,
            Job.properties,
            MoldableJobDescription.id,
            MoldableJobDescription.walltime,
            JobResourceGroup.id,
            JobResourceGroup.property,
            JobResourceDescription.group_id,
            JobResourceDescription.resource_type,
            JobResourceDescription.value,
        )
        .filter(MoldableJobDescription.index == "CURRENT")
        .filter(JobResourceGroup.index == "CURRENT")
        .filter(JobResourceDescription.index == "CURRENT")
        .filter(Job.id.in_(tuple(jids)))
        .filter(Job.id == MoldableJobDescription.job_id)
        .filter(JobResourceGroup.moldable_id == MoldableJobDescription.id)
        .filter(JobResourceDescription.group_id == JobResourceGroup.id)
        .order_by(
            MoldableJobDescription.id, JobResourceGroup.id, JobResourceDescription.order
        )
        .all()
    )
    #            .join(MoldableJobDescription)\
    #            .join(JobResourceGroup)\
    #            .join(JobResourceDescription)\

    cache_constraints = {}

    first_job = True
    prev_j_id = 0
    prev_mld_id = 0
    prev_jrg_id = 0
    prev_res_jrg_id = 0
    mld_res_rqts = []
    jrg = []
    jr_descriptions = []
    res_constraints = []
    prev_mld_id_walltime = 0

    job = None  # Global

    for x in result:
        # remove res_order
        (
            j_id,
            j_properties,
            moldable_id,
            mld_id_walltime,
            jrg_id,
            jrg_grp_property,
            res_jrg_id,
            res_type,
            res_value,
        ) = x
        #
        # new job
        #
        if j_id != prev_j_id:
            if first_job:
                first_job = False
            else:
                jrg.append((jr_descriptions, res_constraints))
                mld_res_rqts.append((prev_mld_id, prev_mld_id_walltime, jrg))
                job.mld_res_rqts = mld_res_rqts
                mld_res_rqts = []
                jrg = []
                jr_descriptions = []
                job.key_cache = {}
                job.deps = []
                job.ts = False
                job.ph = NO_PLACEHOLDER
                job.assign = False
                job.find = False
                job.no_quotas = False

            prev_mld_id = moldable_id
            prev_j_id = j_id
            job = jobs[j_id]
            if besteffort_duration:
                prev_mld_id_walltime = besteffort_duration
            else:
                prev_mld_id_walltime = mld_id_walltime + job_security_time

        else:
            #
            # new moldable_id
            #

            if moldable_id != prev_mld_id:
                if jrg != []:
                    mld_res_rqts.append((prev_mld_id, prev_mld_id_walltime, jrg))

                prev_mld_id = moldable_id
                jrg = []
                jr_descriptions = []
                if besteffort_duration:
                    prev_mld_id_walltime = besteffort_duration
                else:
                    prev_mld_id_walltime = mld_id_walltime + job_security_time

        #
        # new set job descriptions
        #
        if res_jrg_id != prev_res_jrg_id:
            prev_res_jrg_id = res_jrg_id
            jr_descriptions = [(res_type, res_value)]

            #
            # determine resource constraints
            #
            if j_properties == "" and (
                jrg_grp_property == "" or jrg_grp_property == "type = 'default'"
            ):
                res_constraints = copy.copy(resource_set.default_itvs)
            else:
                and_sql = ""
                if j_properties and jrg_grp_property:
                    and_sql = " AND "
                if j_properties is None:
                    j_properties = ""
                if jrg_grp_property is None:
                    jrg_grp_property = ""

                sql_constraints = j_properties + and_sql + jrg_grp_property
                if sql_constraints in cache_constraints:
                    res_constraints = cache_constraints[sql_constraints]
                else:
                    request_constraints = (
                        db.query(Resource.id).filter(text(sql_constraints)).all()
                    )
                    roids = [
                        resource_set.rid_i2o[int(y[0])] for y in request_constraints
                    ]
                    res_constraints = ProcSet(*roids)
                    cache_constraints[sql_constraints] = res_constraints
        else:
            # add next res_type , res_value
            jr_descriptions.append((res_type, res_value))

        #
        # new job resources groupe_id
        #
        if jrg_id != prev_jrg_id:
            prev_jrg_id = jrg_id
            if jr_descriptions != []:
                jrg.append((jr_descriptions, res_constraints))

    # complete the last job
    mld_res_rqts.append((prev_mld_id, prev_mld_id_walltime, jrg))

    job.mld_res_rqts = mld_res_rqts
    job.key_cache = {}
    job.deps = []
    job.ts = False
    job.ph = NO_PLACEHOLDER
    job.assign = False
    job.find = False
    job.no_quotas = False

    get_jobs_types(jids, jobs)
    get_current_jobs_dependencies(jobs)
    set_jobs_cache_keys(jobs)


def get_job_suspended_sum_duration(jid, now):

    suspended_duration = 0
    for j_state_log in (
        db.query(JobStateLog)
        .filter(JobStateLog.job_id == jid)
        .filter(
            (JobStateLog.job_state == "Suspended")
            | (JobStateLog.job_state == "Resuming")
        )
    ):

        date_stop = j_state_log.date_stop
        date_start = j_state_log.date_start

        if date_stop == 0:
            res_time = now - date_start
        else:
            res_time = date_stop - date_start

        if res_time > 0:
            suspended_duration += res_time

    return suspended_duration


# TODO available_suspended_res_itvs, now
def extract_scheduled_jobs(result, resource_set, job_security_time, now):

    jids = []
    jobs_lst = []
    jobs = {}
    prev_jid = 0
    roids = []
    rid2jid = {}

    job = None  # global job

    # (job, a, b, c) = req[0]
    if result:
        for x in result:
            j, moldable_id, start_time, walltime, r_id = x
            if j.id != prev_jid:
                if prev_jid != 0:
                    job.res_set = ProcSet(*roids)
                    jobs_lst.append(job)
                    jids.append(job.id)
                    jobs[job.id] = job
                    roids = []

                prev_jid = j.id
                job = j
                job.start_time = start_time
                job.walltime = walltime + job_security_time
                job.moldable_id = moldable_id
                job.ts = False
                job.ph = NO_PLACEHOLDER
                job.assign = False
                job.find = False
                job.no_quotas = False
                if job.suspended == "YES":
                    job.walltime += get_job_suspended_sum_duration(job.id, now)

            roid = resource_set.rid_i2o[r_id]
            roids.append(roid)
            rid2jid[roid] = j.id

        job.res_set = ProcSet(*roids)
        if job.state == "Suspended":
            job.res_set = job.res_set - resource_set.suspendable_roid_itvs

        jobs_lst.append(job)
        jids.append(job.id)
        jobs[job.id] = job
        get_jobs_types(jids, jobs)

    return (jobs, jobs_lst, jids, rid2jid)


# TODO available_suspended_res_itvs, now
def get_scheduled_jobs(resource_set, job_security_time, now):
    result = (
        db.query(
            Job,
            GanttJobsPrediction.moldable_id,
            GanttJobsPrediction.start_time,
            MoldableJobDescription.walltime,
            GanttJobsResource.resource_id,
        )
        .filter(MoldableJobDescription.index == "CURRENT")
        .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)
        .filter(Job.id == MoldableJobDescription.job_id)
        .order_by(Job.start_time, Job.id)
        .all()
    )

    jobs, jobs_lst, jids, rid2jid = extract_scheduled_jobs(
        result, resource_set, job_security_time, now
    )

    return jobs_lst


def get_after_sched_no_AR_jobs(queue_name, resource_set, job_security_time, now):
    """Get waiting jobs which are not AR and after scheduler round"""
    result = (
        db.query(
            Job,
            GanttJobsPrediction.moldable_id,
            GanttJobsPrediction.start_time,
            MoldableJobDescription.walltime,
            GanttJobsResource.resource_id,
        )
        .filter(MoldableJobDescription.index == "CURRENT")
        .filter(Job.queue_name == queue_name)
        .filter(Job.state == "Waiting")
        .filter(Job.reservation == "None")
        .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)
        .filter(Job.id == MoldableJobDescription.job_id)
        .order_by(Job.start_time, Job.id)
        .all()
    )

    _, jobs_lst, _, _ = extract_scheduled_jobs(
        result, resource_set, job_security_time, now
    )

    return jobs_lst


def get_waiting_scheduled_AR_jobs(queue_name, resource_set, job_security_time, now):
    result = (
        db.query(
            Job,
            GanttJobsPrediction.moldable_id,
            GanttJobsPrediction.start_time,
            MoldableJobDescription.walltime,
            GanttJobsResource.resource_id,
        )
        .filter(MoldableJobDescription.index == "CURRENT")
        .filter(Job.queue_name == queue_name)
        .filter(Job.reservation == "Scheduled")
        .filter(Job.state == "Waiting")
        .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)
        .filter(Job.id == MoldableJobDescription.job_id)
        .order_by(Job.start_time, Job.id)
        .all()
    )

    _, jobs_lst, _, _ = extract_scheduled_jobs(
        result, resource_set, job_security_time, now
    )

    return jobs_lst


# TODO MOVE TO GANTT_HANDLING ???
def get_gantt_jobs_to_launch(
    resource_set, job_security_time, now, kill_duration_before_reservation=0
):

    # get unlaunchable jobs
    # NOT USED launcher will manage these cases ??? (MUST BE CONFIRMED)
    #
    # result = db.query(distinct(Job.id))\
    #           .filter(GanttJobsPrediction.start_time <= now)\
    #           .filter(Job.state == "Waiting")\
    #           .filter(Job.id == MoldableJobDescription.job_id)\
    #           .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
    #           .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)\
    # AND (resources.state IN (\'Dead\',\'Suspected\',\'Absent\')
    #                   OR resources.next_state IN (\'Dead\',\'Suspected\',\'Absent\'))
    #
    #           .all()
    date = now + kill_duration_before_reservation

    result = (
        db.query(
            Job,
            GanttJobsPrediction.moldable_id,
            GanttJobsPrediction.start_time,
            MoldableJobDescription.walltime,
            GanttJobsResource.resource_id,
        )
        .filter(GanttJobsPrediction.start_time <= date)
        .filter(Job.state == "Waiting")
        .filter(Job.id == MoldableJobDescription.job_id)
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)
        .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)
        .filter(Resource.id == GanttJobsResource.resource_id)
        .filter(Resource.state == "Alive")
        .all()
    )

    jobs, jobs_lst, _, rid2jid = extract_scheduled_jobs(
        result, resource_set, job_security_time, now
    )

    return (jobs, jobs_lst, rid2jid)


def job_message(job, nb_resources=None):
    """
    Gather information about a job, and return it as a string.
    Part of this information is set during the scheduling phase, and gathered in this function as side effects (walltime and res_set).

    As computing the number of resources for a job can be costly, it can be done out of this function and passed as a parameter,
    otherwise it is computed from job.res_set.

    The karma is a job value present when the fairsharing is enabled (JOB_PRIORITY=FAIRSHARE).
    """
    message_list = []
    if nb_resources:
        message_list.append("R={}".format(nb_resources))
    elif hasattr(job, "res_set"):
        message_list.append("R={}".format(len(job.res_set)))

    if hasattr(job, "walltime"):
        message_list.append("W={}".format(job.walltime))

    if job.type == "PASSIVE":
        message_list.append("J=P")
    else:
        message_list.append("J=I")

    message_list.append("Q={}".format(job.queue_name))

    logger.info("save assignements")

    message = ",".join(message_list)
    if hasattr(job, "karma"):
        message += " " + "(Karma={})".format(job.karma)

    return message


def save_assigns(jobs, resource_set):
    # http://docs.sqlalchemy.org/en/rel_0_9/core/dml.html#sqlalchemy.sql.expression.Insert.values
    if len(jobs) > 0:
        logger.debug("nb job to save: " + str(len(jobs)))
        mld_id_start_time_s = []
        mld_id_rid_s = []
        message_updates = {}

        for j in jobs.values() if isinstance(jobs, dict) else jobs:
            if j.start_time > -1:
                logger.debug("job_id to save: " + str(j.id))
                mld_id_start_time_s.append(
                    {"moldable_job_id": j.moldable_id, "start_time": j.start_time}
                )
                riods = list(j.res_set)
                mld_id_rid_s.extend(
                    [
                        {
                            "moldable_job_id": j.moldable_id,
                            "resource_id": resource_set.rid_o2i[rid],
                        }
                        for rid in riods
                    ]
                )
                msg = job_message(j, nb_resources=len(riods))
                message_updates[j.id] = msg

        if message_updates:
            logger.info("save job messages")
            db.session.query(Job).filter(Job.id.in_(message_updates)).update(
                {
                    Job.message: case(
                        message_updates,
                        value=Job.id,
                    )
                },
                synchronize_session=False,
            )

        logger.info("save assignements")
        db.session.execute(GanttJobsPrediction.__table__.insert(), mld_id_start_time_s)
        db.session.execute(GanttJobsResource.__table__.insert(), mld_id_rid_s)
        db.commit()


def save_assigns_bulk(jobs, resource_set):

    if len(jobs) > 0:
        logger.debug("nb job to save: " + str(len(jobs)))
        mld_id_start_time_s = []
        mld_id_rid_s = []
        for j in jobs.values():
            if j.start_time > -1:
                logger.debug("job_id  to save: " + str(j.id))
                mld_id_start_time_s.append((j.moldable_id, j.start_time))
                riods = list(j.res_set)
                mld_id_rid_s.extend(
                    [(j.moldable_id, resource_set.rid_o2i[rid]) for rid in riods]
                )

        logger.info("save assignements")

        with db.engine.connect() as to_conn:
            cursor = to_conn.connection.cursor()
            pg_bulk_insert(
                cursor,
                db["gantt_jobs_predictions"],
                mld_id_start_time_s,
                ("moldable_job_id", "start_time"),
                binary=True,
            )
            pg_bulk_insert(
                cursor,
                db["queues"],
                mld_id_rid_s,
                ("moldable_job_id", "resource_id"),
                binary=True,
            )


def get_current_jobs_dependencies(jobs):
    # retrieve jobs dependencies *)
    # return an hashtable, key = job_id, value = list of required jobs *)

    req = (
        db.query(JobDependencie, Job.state, Job.exit_code)
        .filter(JobDependencie.index == "CURRENT")
        .filter(Job.id == JobDependencie.job_id_required)
        .all()
    )

    for x in req:
        j_dep, state, exit_code = x
        if j_dep.job_id not in jobs:
            # This fact have no particular impact
            logger.warning(
                " during get dependencies for current job %s is not in waiting state"
                % str(j_dep.job_id)
            )
        else:
            jobs[j_dep.job_id].deps.append((j_dep.job_id_required, state, exit_code))


def get_current_not_waiting_jobs():
    jobs = db.query(Job).filter(Job.state != "Waiting").all()
    jobs_by_state = {}
    for job in jobs:
        if job.state not in jobs_by_state:
            jobs_by_state[job.state] = []
        jobs_by_state[job.state].append(job)
    return jobs_by_state


def set_job_start_time_assigned_moldable_id(jid, start_time, moldable_id):
    # db.query(Job).update({Job.start_time:
    # start_time,Job.assigned_moldable_job: moldable_id}).filter(Job.id ==
    # jid)
    db.query(Job).filter(Job.id == jid).update(
        {Job.start_time: start_time, Job.assigned_moldable_job: moldable_id},
        synchronize_session=False,
    )
    db.commit()


def set_jobs_start_time(tuple_jids, start_time):

    db.query(Job).filter(Job.id.in_(tuple_jids)).update(
        {Job.start_time: start_time}, synchronize_session=False
    )
    db.commit()


# NO USED
def add_resource_jobs_pairs(tuple_mld_ids):  # pragma: no cover
    resources_mld_ids = (
        db.query(GanttJobsResource)
        .filter(GanttJobsResource.job_id.in_(tuple_mld_ids))
        .all()
    )

    assigned_resources = [
        {
            "moldable_job_id": res_mld_id.moldable_id,
            "resource_id": res_mld_id.resource_id,
        }
        for res_mld_id in resources_mld_ids
    ]

    db.session.execute(AssignedResource.__table__.insert(), assigned_resources)
    db.commit()


def add_resource_job_pairs(moldable_id):
    resources_mld_ids = (
        db.query(GanttJobsResource)
        .filter(GanttJobsResource.moldable_id == moldable_id)
        .all()
    )

    assigned_resources = [
        {
            "moldable_job_id": res_mld_id.moldable_id,
            "resource_id": res_mld_id.resource_id,
        }
        for res_mld_id in resources_mld_ids
    ]

    db.session.execute(AssignedResource.__table__.insert(), assigned_resources)
    db.commit()


# TODO MOVE TO gantt_handling
def get_gantt_waiting_interactive_prediction_date():
    req = (
        db.query(Job.id, Job.info_type, GanttJobsPrediction.start_time, Job.message)
        .filter(Job.state == "Waiting")
        .filter(Job.type == "INTERACTIVE")
        .filter(Job.reservation == "None")
        .filter(MoldableJobDescription.job_id == Job.id)
        .filter(GanttJobsPrediction.moldable_id == MoldableJobDescription.id)
        .all()
    )
    return req


def insert_job(**kwargs):
    """Insert job in database

    #   "{ sql1 }/prop1=1/prop2=3+{sql2}/prop3=2/prop4=1/prop5=1+...,walltime=60"
    #
    #   res = "/switch=2/nodes=10+{lic_type = 'mathlab'}/licence=20" types="besteffort, container"
    #
    insert_job(
    res = [
        ( 60, [("switch=2/nodes=20", ""), ("licence=20", "lic_type = 'mathlab'")] ) ],
    types = ["besteffort", "container"],
    user= "")


    """

    default_values = {
        "launching_directory": "",
        "checkpoint_signal": 0,
        "properties": "",
    }

    for k, v in default_values.items():
        if k not in kwargs:
            kwargs[k] = v

    if "res" in kwargs:
        res = kwargs.pop("res")
    else:
        res = [(60, [("resource_id=1", "")])]

    if "types" in kwargs:
        types = kwargs.pop("types")
    else:
        types = []

    if "queue_name" not in kwargs:
        kwargs["queue_name"] = "default"

    if "user" in kwargs:
        kwargs["job_user"] = kwargs.pop("user")

    ins = Job.__table__.insert().values(**kwargs)
    result = db.session.execute(ins)
    job_id = result.inserted_primary_key[0]

    mld_jid_walltimes = []
    res_grps = []

    for res_mld in res:
        w, res_grp = res_mld
        mld_jid_walltimes.append({"moldable_job_id": job_id, "moldable_walltime": w})
        res_grps.append(res_grp)

    result = db.session.execute(
        MoldableJobDescription.__table__.insert(), mld_jid_walltimes
    )

    if len(mld_jid_walltimes) == 1:
        mld_ids = [result.inserted_primary_key[0]]
    else:
        r = (
            db.query(MoldableJobDescription.id)
            .filter(MoldableJobDescription.job_id == job_id)
            .all()
        )
        mld_ids = [x for e in r for x in e]

    for mld_idx, res_grp in enumerate(res_grps):
        # job_resource_groups
        mld_id_property = []
        res_hys = []

        moldable_id = mld_ids[mld_idx]

        for r_hy_prop in res_grp:
            (res_hy, properties) = r_hy_prop
            mld_id_property.append(
                {"res_group_moldable_id": moldable_id, "res_group_property": properties}
            )
            res_hys.append(res_hy)

        result = db.session.execute(
            JobResourceGroup.__table__.insert(), mld_id_property
        )

        if len(mld_id_property) == 1:
            grp_ids = [result.inserted_primary_key[0]]
        else:
            r = (
                db.query(JobResourceGroup.id)
                .filter(JobResourceGroup.moldable_id == moldable_id)
                .all()
            )
            grp_ids = [x for e in r for x in e]

        # job_resource_descriptions
        for grp_idx, res_hy in enumerate(res_hys):
            res_description = []
            for idx, val in enumerate(res_hy.split("/")):
                tv = val.split("=")
                res_description.append(
                    {
                        "res_job_group_id": grp_ids[grp_idx],
                        "res_job_resource_type": tv[0],
                        "res_job_value": tv[1],
                        "res_job_order": idx,
                    }
                )

            db.session.execute(
                JobResourceDescription.__table__.insert(), res_description
            )

    if types:
        for typ in types:
            ins = [{"job_id": job_id, "type": typ} for typ in types]
        db.session.execute(JobType.__table__.insert(), ins)

    return job_id


def resubmit_job(job_id):
    """Resubmit a job and give the new job_id"""

    if "OARDO_USER" in os.environ:
        user = os.environ["OARDO_USER"]
    else:
        user = "oar"

    job = get_job(job_id)

    if job is None:
        return ((-5, "Unable to retrieve initial job:" + str(job_id)), 0)
    if job.type != "PASSIVE":
        return (
            (-1, "Interactive jobs and advance reservations cannot be resubmitted."),
            0,
        )
    if (
        (job.state != "Error")
        and (job.state != "Terminated")
        and (job.state != "Finishing")
    ):
        return (
            (-2, "Only jobs in the Error or Terminated state can be resubmitted."),
            0,
        )
    if (user != job.user) and (user != "oar") and (user != "root"):
        return ((-3, "Resubmitted job user mismatch."), 0)

    # Verify the content of the ssh keys
    job_challenge, ssh_private_key, ssh_public_key = get_job_challenge(job_id)
    if (ssh_public_key != "") or (ssh_private_key != ""):
        # Check if the keys are used by other jobs
        if (
            get_count_same_ssh_keys_current_jobs(user, ssh_private_key, ssh_public_key)
            > 0
        ):
            return ((-4, "Another active job is using the same job key."), 0)

    date = tools.get_date()
    # Detach and prepare old job to be reinserted
    db.session.expunge(job)
    make_transient(job)
    job.id = None
    job.state = "Hold"
    job.date = date
    if job.reservation is None:
        job.start_time = 0
    job.message = ""
    job.scheduler_info = ""
    job.exit_code = None
    job.assigned_moldable_job = 0

    db.session.add(job)
    db.session.flush()

    new_job_id = job.id

    # Insert challenge and ssh_keys
    random_number = random.randint(1, 1000000000000)
    ins = Challenge.__table__.insert().values(
        {
            "job_id": new_job_id,
            "challenge": random_number,
            "ssh_private_key": ssh_private_key,
            "ssh_public_key": ssh_public_key,
        }
    )
    db.session.execute(ins)

    # Duplicate job resource description requirements
    # Retrieve modable_job_description
    modable_job_descriptions = (
        db.query(MoldableJobDescription)
        .filter(MoldableJobDescription.job_id == job_id)
        .all()
    )

    for mdl_job_descr in modable_job_descriptions:
        res = db.session.execute(
            MoldableJobDescription.__table__.insert(),
            {
                "moldable_job_id": new_job_id,
                "moldable_walltime": mdl_job_descr.walltime,
            },
        )
        moldable_id = res.inserted_primary_key[0]

        job_resource_groups = (
            db.query(JobResourceGroup)
            .filter(JobResourceGroup.moldable_id == mdl_job_descr.id)
            .all()
        )

        for job_res_grp in job_resource_groups:
            res = db.session.execute(
                JobResourceGroup.__table__.insert(),
                {
                    "res_group_moldable_id": moldable_id,
                    "res_group_property": job_res_grp.property,
                },
            )
            res_group_id = res.inserted_primary_key[0]

            job_resource_descriptions = (
                db.query(JobResourceDescription)
                .filter(JobResourceDescription.group_id == job_res_grp.id)
                .all()
            )

            for job_res_descr in job_resource_descriptions:
                db.session.execute(
                    JobResourceDescription.__table__.insert(),
                    {
                        "res_job_group_id": res_group_id,
                        "res_job_resource_type": job_res_descr.resource_type,
                        "res_job_value": job_res_descr.value,
                        "res_job_order": job_res_descr.order,
                    },
                )

    # Duplicate job types
    job_types = db.query(JobType).filter(JobType.job_id == job_id).all()
    new_job_types = [{"job_id": new_job_id, "type": jt.type} for jt in job_types]

    db.session.execute(JobType.__table__.insert(), new_job_types)

    # Update job dependencies
    db.query(JobDependencie).filter(JobDependencie.job_id_required == job_id).update(
        {JobDependencie.job_id_required: new_job_id}, synchronize_session=False
    )

    # Update job state to waintg
    db.query(Job).filter(Job.id == new_job_id).update(
        {"state": "Waiting"}, synchronize_session=False
    )

    # Emit job state log
    db.session.execute(
        JobStateLog.__table__.insert(),
        {"job_id": new_job_id, "job_state": "Waiting", "date_start": date},
    )
    db.commit()

    return ((0, ""), new_job_id)


def is_job_already_resubmitted(job_id):
    """Check if the job was already resubmitted
    args : db ref, job id"""

    count_query = (
        select([func.count()]).select_from(Job).where(Job.resubmit_job_id == job_id)
    )
    return db.session.execute(count_query).scalar()


def set_job_resa_state(job_id, state):
    """sets the reservation field of the job of id passed in parameter
    parameters : base, jobid, state
    return value : None
    side effects : changes the field state of the job in the table Jobs
    """
    db.query(Job).filter(Job.id == job_id).update(
        {Job.reservation: state}, synchronize_session=False
    )
    db.commit()


def set_job_message(job_id, message):
    db.query(Job).filter(Job.id == job_id).update(
        {Job.message: message}, synchronize_session=False
    )
    db.commit()


def get_waiting_reservation_jobs_specific_queue(queue_name):
    """Get all waiting reservation jobs in the specified queue
    parameter : database ref, queuename
    return an array of job informations
    """
    waiting_scheduled_ar_jobs = (
        db.query(Job)
        .filter((Job.state == "Waiting") | (Job.state == "toAckReservation"))
        .filter(Job.reservation == "Scheduled")
        .filter(Job.queue_name == queue_name)
        .order_by(Job.id)
        .all()
    )
    return waiting_scheduled_ar_jobs


def update_scheduler_last_job_date(date, moldable_id):
    """used to allow search_idle_nodes to operate for dynamic node management feature (Hulot)"""

    if db.dialect == "sqlite":
        subquery = (
            db.query(AssignedResource.resource_id)
            .filter_by(moldable_id=moldable_id)
            .subquery()
        )
        db.query(Resource).filter(Resource.id.in_(subquery)).update(
            {Resource.last_job_date: date}, synchronize_session=False
        )

    else:
        db.query(Resource).filter(AssignedResource.moldable_id == moldable_id).filter(
            Resource.id == AssignedResource.resource_id
        ).update({Resource.last_job_date: date}, synchronize_session=False)
    db.commit()


# Get all waiting reservation jobs
# parameter : database ref
# return an array of moldable job informations
def get_waiting_moldable_of_reservations_already_scheduled():
    """
    return the moldable jobs assigned to already scheduled reservations.
    """
    result = (
        db.query(
            MoldableJobDescription.id,
        )
        .filter((Job.state == "Waiting") | (Job.state == "toAckReservation"))
        .filter(Job.reservation == "Scheduled")
        .filter(Job.id == MoldableJobDescription.job_id)
        .filter(GanttJobsPrediction.moldable_id == MoldableJobDescription.id)
        .filter(GanttJobsResource.moldable_id == MoldableJobDescription.id)
        .order_by(Job.id)
        .distinct()
        .all()
    )

    return [x[0] for x in result]


# TODO MOVE TO GANTT_HANDLING
def gantt_flush_tables(reservations_to_keep_mld_ids=[]):
    """Flush gantt tables but keep accepted advance reservations"""

    if reservations_to_keep_mld_ids != []:
        logger.debug(
            "reservations_to_keep_mld_ids[0]: " + str(reservations_to_keep_mld_ids[0])
        )
        db.query(GanttJobsPrediction).filter(
            ~GanttJobsPrediction.moldable_id.in_(tuple(reservations_to_keep_mld_ids))
        ).delete(synchronize_session=False)
        db.query(GanttJobsResource).filter(
            ~GanttJobsResource.moldable_id.in_(tuple(reservations_to_keep_mld_ids))
        ).delete(synchronize_session=False)
    else:
        db.query(GanttJobsPrediction).delete(synchronize_session=False)
        db.query(GanttJobsResource).delete(synchronize_session=False)

    db.commit()


def get_jobs_in_multiple_states(states, resource_set):

    result = (
        db.query(Job, AssignedResource.moldable_id, AssignedResource.resource_id)
        .filter(Job.state.in_(tuple(states)))
        .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)
        .order_by(Job.id)
        .all()
    )

    first_job = True
    jobs = {}

    prev_jid = 0
    roids = []

    job = None  # global job

    if result:
        for x in result:
            j, moldable_id, resource_id = x

            if j.id != prev_jid:
                if first_job:
                    first_job = False
                else:
                    job.res_set = ProcSet(*roids)
                    jobs[job.id] = job
                    roids = []

                prev_jid = j.id
                job = j
                job.moldable_id = moldable_id

            roids.append(resource_set.rid_i2o[resource_id])

        job.res_set = ProcSet(*roids)
        jobs[job.id] = job

    return jobs


def get_jobs_ids_in_multiple_states(states):

    result = (
        db.query(Job.id, Job.state)
        .filter(Job.state.in_(tuple(states)))
        .order_by(Job.id)
        .all()
    )

    jids_states = {}

    if result:
        for x in result:
            jid, state = x
            jids_states[jid] = state
    return jids_states


def set_moldable_job_max_time(moldable_id, walltime):
    """Set walltime for a moldable job"""
    db.query(MoldableJobDescription).filter(
        MoldableJobDescription.id == moldable_id
    ).update({MoldableJobDescription.walltime: walltime}, synchronize_session=False)

    db.commit()


# TODO MOVE TO GANTT_HANDLING
def set_gantt_job_start_time(moldable_id, current_time_sec):
    """Update start_time in gantt for a specified job"""
    db.query(GanttJobsPrediction).filter(
        GanttJobsPrediction.moldable_id == moldable_id
    ).update(
        {GanttJobsPrediction.start_time: current_time_sec}, synchronize_session=False
    )

    db.commit()


# TODO MOVE TO GANTT_HANDLING
def remove_gantt_resource_job(moldable_id, job_res_set, resource_set):
    if len(job_res_set) != 0:
        resource_ids = [resource_set.rid_o2i[rid] for rid in job_res_set]

        db.query(GanttJobsResource).filter(
            GanttJobsResource.moldable_id == moldable_id
        ).filter(~GanttJobsResource.resource_id.in_(tuple(resource_ids))).delete(
            synchronize_session=False
        )

        db.commit()


def is_timesharing_for_two_jobs(j1, j2):
    if ("timesharing" in j1.types) and ("timesharing" in j2.types):
        t1 = j1.types["timesharing"]
        t2 = j2.types["timesharing"]

        if (t1 == "*,*") and (t2 == "*,*"):
            return True

        if (j1.user == j2.user) and (j1.name == j2.name):
            return True

        if (j1.user == j2.user) and (t1 == "user,*") and (t2 == "user,*"):
            return True

        if (j1.name == j2.name) and (t1 == "*,name") and (t1 == "*,name"):
            return True

    return False


# TODO MOVE TO resource_handling
def get_jobs_on_resuming_job_resources(job_id):
    """Return the list of jobs running on resources allocated to another given job"""
    j1 = aliased(Job)
    j2 = aliased(Job)
    a1 = aliased(AssignedResource)
    a2 = aliased(AssignedResource)

    states = (
        "toLaunch",
        "toError",
        "toAckReservation",
        "Launching",
        "Running ",
        "Finishing",
    )

    result = (
        db.query(distinct(j2.id))
        .filter(a1.index == "CURRENT")
        .filter(a2.index == "CURRENT")
        .filter(j1.id == job_id)
        .filter(j1.id != j2.id)
        .filter(a1.moldable_id == j1.assigned_moldable_job)
        .filter(a2.resource_id == a1.resource_id)
        .filter(j2.state.in_(states))
        .all()
    )

    return result


def resume_job_action(job_id):
    """resume_job_action performs all action when a job is suspended"""

    set_job_state(job_id, "Running")

    resources = get_current_resources_with_suspended_job()
    if resources != ():
        db.query(Resource).filter(~Resource.id.in_(resources)).update(
            {Resource.suspended_jobs: "NO"}, synchronize_session=False
        )

    else:
        db.query(Resource).update(
            {Resource.suspended_jobs: "NO"}, synchronize_session=False
        )

    db.commit()


def get_cpuset_values(cpuset_field, moldable_id):
    """get cpuset values for each nodes of a moldable_id
    Note: this function is called get_cpuset_values_for_a_moldable_job in OAR2.x
    """
    sql_where_string = "resources.type = 'default'"

    if "SCHEDULER_RESOURCES_ALWAYS_ASSIGNED_TYPE" in config:
        resources_to_always_add_type = config[
            "SCHEDULER_RESOURCES_ALWAYS_ASSIGNED_TYPE"
        ]
        sql_where_string = (
            "("
            + sql_where_string
            + "OR resources.type = '"
            + resources_to_always_add_type
            + "')"
        )

    results = (
        db.query(Resource.network_address, getattr(Resource, cpuset_field))
        .filter(AssignedResource.moldable_id == moldable_id)
        .filter(AssignedResource.resource_id == Resource.id)
        .filter(Resource.network_address != "")
        .filter(text(sql_where_string))
        .group_by(Resource.network_address, getattr(Resource, cpuset_field))
        .all()
    )

    # hostnames_cpuset_values: {hostname: [array with the content of the database cpuset field]}
    hostnames_cpuset_fields = {}
    for hostname_cpuset_field in results:
        hostname, cpuset_field = hostname_cpuset_field
        if hostname not in hostnames_cpuset_fields:
            hostnames_cpuset_fields[hostname] = [cpuset_field]
        else:
            hostnames_cpuset_fields[hostname].append(cpuset_field)

    return hostnames_cpuset_fields


def get_array_job_ids(array_id):
    """Get all the job_ids of a given array of job identified by its id"""
    results = db.query(Job.id).filter(Job.array_id == array_id).order_by(Job.id).all()
    job_ids = [r[0] for r in results]
    return job_ids


def get_job_ids_with_given_properties(sql_property):
    """Returns the job_ids with specified properties parameters : base, where SQL constraints."""
    results = db.query(Job.id).filter(text(sql_property)).order_by(Job.id).all()
    job_ids = [r[0] for r in results]
    return job_ids


def get_job(job_id):
    try:
        job = db.query(Job).filter(Job.id == job_id).one()
    except Exception as e:
        logger.warning("get_job(" + str(job_id) + ") raises exception: " + str(e))
        return None
    else:
        return job


def get_running_job(job_id):

    res = (
        db.query(
            Job.start_time, MoldableJobDescription.walltime.label("moldable_walltime")
        )
        .filter(Job.id == job_id)
        .filter(Job.state == "Running")
        .filter(Job.assigned_moldable_job == MoldableJobDescription.id)
        .one()
    )  # TODO verify tuple usage (Job.start_time, MoldableJobDescription.walltime)
    return res


def get_current_moldable_job(moldable_id):
    """Return the moldable job of id passed in"""
    res = (
        db.query(MoldableJobDescription)
        .filter(MoldableJobDescription.index == "CURRENT")
        .filter(MoldableJobDescription.id == moldable_id)
        .one()
    )
    return res


def frag_job(job_id, user=None):
    """Set the flag 'ToFrag' of a job to 'Yes' which will threshold job deletion"""
    if not user:
        if "OARDO_USER" in os.environ:
            user = os.environ["OARDO_USER"]
        else:
            user = os.environ["USER"]

    job = get_job(job_id)

    if not job:
        return -3

    if (user == job.user) or (user == "oar") or (user == "root"):
        res = db.query(FragJob).filter(FragJob.job_id == job_id).all()

        if len(res) == 0:

            date = tools.get_date()
            frajob = FragJob(job_id=job_id, date=date)
            db.add(frajob)
            db.commit()
            add_new_event(
                "FRAG_JOB_REQUEST",
                job_id,
                "User %s requested to frag the job %s" % (user, str(job_id)),
            )
            return 0
        else:
            # Job already killed
            return -2
    else:
        return -1


def ask_checkpoint_signal_job(job_id, signal=None, user=None):
    """Verify if the user is able to checkpoint the job
    returns : 0 if all is good, 1 if the user cannot do this,
    2 if the job is not running,
    3 if the job is Interactive"""

    if not user:
        if "OARDO_USER" in os.environ:
            user = os.environ["OARDO_USER"]
        else:
            user = os.environ["USER"]

    job = get_job(job_id)

    error_msg = "Cannot checkpoint "
    if signal:
        error_msg = "Cannot signal "
    error_msg += "{} ; ".format(job_id)

    if job and (job.type == "INTERACTIVE"):
        return (3, error_msg + "The job is Interactive.")

    if job and ((user == job.user) or (user == "oar") or (user == "root")):
        if job.state == "Running":
            if signal:
                add_new_event(
                    "CHECKPOINT",
                    job_id,
                    "User {} requested a checkpoint on the job {}".format(user, job_id),
                )
            else:
                add_new_event(
                    "SIGNAL_{}".format(signal),
                    job_id,
                    "User {} requested the signal {} on the job {}".format(
                        user, signal, job_id
                    ),
                )
            return (0, None)
        else:
            return (2, error_msg + "This job is not running.")
    else:
        return (1, error_msg + "You are not the right user.")


def get_job_current_hostnames(job_id):
    """Returns the list of hosts associated to the job passed in parameter"""

    results = (
        db.query(distinct(Resource.network_address))
        .filter(AssignedResource.index == "CURRENT")
        .filter(MoldableJobDescription.index == "CURRENT")
        .filter(AssignedResource.resource_id == Resource.id)
        .filter(MoldableJobDescription.id == AssignedResource.moldable_id)
        .filter(MoldableJobDescription.job_id == job_id)
        .filter(Resource.network_address != "")
        .filter(Resource.type == "default")
        .order_by(Resource.network_address)
        .all()
    )

    return [r[0] for r in results]


def get_job_types(job_id):
    """Returns a hash table with all types for the given job ID."""

    results = db.query(JobType.type).filter(JobType.job_id == job_id).all()
    res = {}
    for t in results:
        typ = t[0]
        match = re.match(r"^\s*(token)\s*\:\s*(\w+)\s*=\s*(\d+)\s*$", typ)
        if match:
            res[match.group(1)] = {match.group(2): match.group(3)}
        else:
            match = re.match(r"^\s*(\w+)\s*=\s*(.+)$", typ)
            if match:
                res[match.group(1)] = match.group(2)
            else:
                res[typ] = True
    return res


def add_current_job_types(job_id, j_type):
    req = db.insert(JobType).values({"job_id": job_id, "type": j_type})
    db.session.execute(req)


def remove_current_job_types(job_id, j_type):
    db.query(JobType).filter(JobType.job_id == job_id).filter(
        JobType.type == j_type
    ).filter(JobType.types_index == "CURRENT").delete(synchronize_session=False)
    db.commit()


def log_job(job):  # pragma: no cover
    """sets the index fields to LOG on several tables
    this will speed up future queries
    """
    if db.dialect == "sqlite":
        return
    db.query(MoldableJobDescription).filter(
        MoldableJobDescription.index == "CURRENT"
    ).filter(MoldableJobDescription.job_id == job.id).update(
        {MoldableJobDescription.index: "LOG"}, synchronize_session=False
    )

    db.query(JobResourceDescription).filter(
        MoldableJobDescription.job_id == job.id
    ).filter(JobResourceGroup.moldable_id == MoldableJobDescription.id).filter(
        JobResourceDescription.group_id == JobResourceGroup.id
    ).update(
        {JobResourceDescription.index: "LOG"}, synchronize_session=False
    )

    db.query(JobResourceGroup).filter(JobResourceGroup.index == "CURRENT").filter(
        MoldableJobDescription.index == "LOG"
    ).filter(MoldableJobDescription.job_id == job.id).filter(
        JobResourceGroup.moldable_id == MoldableJobDescription.id
    ).update(
        {JobResourceGroup.index: "LOG"}, synchronize_session=False
    )

    db.query(JobType).filter(JobType.types_index == "CURRENT").filter(
        JobType.job_id == job.id
    ).update({JobType.types_index: "LOG"}, synchronize_session=False)

    db.query(JobDependencie).filter(JobDependencie.index == "CURRENT").filter(
        JobDependencie.job_id == job.id
    ).update({JobDependencie.index: "LOG"}, synchronize_session=False)

    if job.assigned_moldable_job != 0:
        db.query(AssignedResource).filter(AssignedResource.index == "CURRENT").filter(
            AssignedResource.moldable_id == int(job.assigned_moldable_job)
        ).update({AssignedResource.index: "LOG"}, synchronize_session=False)
    db.commit()


def set_job_state(jid, state):

    result = (
        db.query(Job)
        .filter(Job.id == jid)
        .filter(Job.state != "Error")
        .filter(Job.state != "Terminated")
        .filter(Job.state != state)
        .update({Job.state: state}, synchronize_session=False)
    )
    db.commit()

    if result == 1:  # OK for sqlite
        logger.debug(
            "Job state updated, job_id: " + str(jid) + ", wanted state: " + state
        )

        date = tools.get_date()

        # TODO: optimize job log
        db.query(JobStateLog).filter(JobStateLog.date_stop == 0).filter(
            JobStateLog.job_id == jid
        ).update({JobStateLog.date_stop: date}, synchronize_session=False)
        db.commit()
        req = db.insert(JobStateLog).values(
            {"job_id": jid, "job_state": state, "date_start": date}
        )
        db.session.execute(req)

        if (
            state == "Terminated"
            or state == "Error"
            or state == "toLaunch"
            or state == "Running"
            or state == "Suspended"
            or state == "Resuming"
        ):
            job = db.query(Job).filter(Job.id == jid).one()
            if state == "Suspend":
                tools.notify_user(job, "SUSPENDED", "Job is suspended.")
            elif state == "Resuming":
                tools.notify_user(job, "RESUMING", "Job is resuming.")
            elif state == "Running":
                tools.notify_user(job, "RUNNING", "Job is running.")
            elif state == "toLaunch":
                update_current_scheduler_priority(job, "+2", "START")
            else:  # job is "Terminated" or ($state eq "Error")
                if job.stop_time < job.start_time:
                    db.query(Job).filter(Job.id == jid).update(
                        {Job.stop_time: job.start_time}, synchronize_session=False
                    )
                    db.commit()

                if job.assigned_moldable_job != "0":
                    # Update last_job_date field for resources used
                    update_scheduler_last_job_date(date, int(job.assigned_moldable_job))

                if state == "Terminated":
                    tools.notify_user(job, "END", "Job stopped normally.")
                else:
                    # Verify if the job was suspended and if the resource
                    # property suspended is updated
                    if job.suspended == "YES":
                        r = get_current_resources_with_suspended_job()

                        if r != ():
                            db.query(Resource).filter(~Resource.id.in_(r)).update(
                                {Resource.suspended_jobs: "NO"},
                                synchronize_session=False,
                            )
                        else:
                            db.query(Resource).update(
                                {Resource.suspended_jobs: "NO"},
                                synchronize_session=False,
                            )
                        db.commit()

                    tools.notify_user(
                        job, "ERROR", "Job stopped abnormally or an OAR error occured."
                    )

                update_current_scheduler_priority(job, "-2", "STOP")

                # Here we must not be asynchronously with the scheduler
                log_job(job)
                # $dbh is valid so these 2 variables must be defined
                completed = tools.notify_almighty("ChState")
                if not completed:
                    logger.warning(
                        "Not able to notify almighty to launch the job "
                        + str(job.id)
                        + " (socket error)"
                    )

    else:
        logger.warning(
            "Job is already termindated or in error or wanted state, job_id: "
            + str(jid)
            + ", wanted state: "
            + state
        )


def get_job_duration_in_state(jid, state):
    """Get the amount of time in the defined state for a job"""
    date = tools.get_date()
    result = (
        db.query(JobStateLog.date_start, JobStateLog.date_stop)
        .filter(JobStateLog.job_id == jid)
        .filter(JobStateLog.job_state == state)
        .all()
    )
    duration = 0
    for dates in result:
        date_start, date_stop = dates
        t_end = date if date_stop == 0 else date_stop
        duration = duration + (t_end - date_start)
    return duration


def hold_job(job_id, running, user=None):
    """sets the state field of a job to 'Hold'
    equivalent to set_job_state(base,jobid,"Hold") except for permissions on user
    parameters : jobid, running, user
    return value : 0 on success, -1 on error (if the user calling this method
    is not the user running the job)
    side effects : changes the field state of the job to 'Hold' in the table Jobs.
    """

    if not user:
        if "OARDO_USER" in os.environ:
            user = os.environ["OARDO_USER"]
        else:
            user = os.environ["USER"]

    job = get_job(job_id)

    user_allowed_hold_resume = False
    if (
        "USERS_ALLOWED_HOLD_RESUME" in config
        and config["USERS_ALLOWED_HOLD_RESUME"] == "yes"
    ):
        user_allowed_hold_resume = True

    event_type = "HOLD_WAITING_JOB"
    if running:
        event_type = "HOLD_RUNNING_JOB"

    if job:
        if (
            running
            and (not user_allowed_hold_resume)
            and (user != "oar")
            and (user != "root")
        ):
            return -4
        elif (user == job.user) or (user == "oar") or (user == "root"):
            if ((job.state == "Waiting") or (job.state == "Resuming")) or (
                running
                and (
                    job.state == "toLaunch"
                    or job.state == "Launching"
                    or job.state == "Running"
                )
            ):
                add_new_event(
                    event_type,
                    job_id,
                    "User {} launched oarhold on the job {}".format(user, job_id),
                )
                return 0
            else:
                return -3
        else:
            return -2
    else:
        return -1

    return 0


def resume_job(job_id, user=None):
    """Returns the state of the job from 'Hold' to 'Waiting'
    equivalent to set_job_state(base,jobid,"Waiting") except for permissions on
    user and the fact the job must already be in 'Hold' state
    parameters : jobid
    return value : 0 on success, -1 on error (if the user calling this method
    is not the user running the job)
    side effects : changes the field state of the job to 'Waiting' in the table
    Jobs
    """
    if not user:
        if "OARDO_USER" in os.environ:
            user = os.environ["OARDO_USER"]
        else:
            user = os.environ["USER"]

    job = get_job(job_id)

    user_allowed_hold_resume = False
    if (
        "USERS_ALLOWED_HOLD_RESUME" in config
        and config["USERS_ALLOWED_HOLD_RESUME"] == "yes"
    ):
        user_allowed_hold_resume = True

    if job:
        if (
            (job.state == "Suspended")
            and (not user_allowed_hold_resume)
            and (user != "oar")
            and (user != "root")
        ):
            return -4
        elif (user == job.user) or (user == "oar") or (user == "root"):
            if (job.state == "Hold") or (job.state == "Suspended"):
                add_new_event(
                    "RESUME_JOB",
                    job_id,
                    "User {} launched oarresume on the job {}".format(user, job_id),
                )
                return 0
            return -3
        return -2
    else:
        return -1


def get_job_challenge(job_id):
    """gets the challenge string of a OAR Job
    parameters : base, jobid
    return value : challenge, ssh_private_key, ssh_public_key"""
    res = db.query(Challenge).filter(Challenge.job_id == job_id).one()
    return (res.challenge, res.ssh_private_key, res.ssh_public_key)


def get_count_same_ssh_keys_current_jobs(user, ssh_private_key, ssh_public_key):
    """return the number of current jobs with the same ssh keys"""
    count_query = (
        select([func.count(Challenge.job_id)])
        .select_from(Challenge)
        .where(Challenge.job_id == Job.id)
        .where(
            Job.state.in_(
                (
                    "Waiting",
                    "Hold",
                    "toLaunch",
                    "toError",
                    "toAckReservation",
                    "Launching",
                    "Running",
                    "Suspended",
                    "Resuming",
                )
            )
        )
        .where(Challenge.ssh_private_key == ssh_private_key)
        .where(Challenge.ssh_public_key == ssh_public_key)
        .where(Job.user != user)
        .where(Challenge.ssh_private_key != "")
    )
    return db.session.execute(count_query).scalar()


def get_jobs_in_state(state):
    """Return the jobs in the specified state"""
    return db.query(Job).filter(Job.state == state).all()


def get_job_host_log(moldable_id):
    """Returns the list of hosts associated to the moldable job passed in parameter
    parameters : base, moldable_id
    return value : list of distinct hostnames"""

    res = (
        db.query(distinct(Resource.network_address))
        .filter(AssignedResource.moldable_id == moldable_id)
        .filter(Resource.id == AssignedResource.resource_id)
        .filter(Resource.network_address != "")
        .filter(Resource.type == "default")
        .all()
    )
    return [h[0] for h in res]


def suspend_job_action(job_id, moldable_id):
    """perform all action when a job is suspended"""
    set_job_state(job_id, "Suspended")
    db.query(Job).filter(Job.id == job_id).update(
        {"suspended": "YES"}, synchronize_session=False
    )
    resource_ids = get_current_resources_with_suspended_job()

    db.query(Resource).filter(Resource.id.in_(resource_ids)).update(
        {"suspended_jobs": "YES"}, synchronize_session=False
    )
    db.commit()


def get_job_cpuset_name(job_id, job=None):
    """Get the cpuset name for the given job"""
    user = None
    if job is None:
        user_tuple = db.query(Job.user).filter(Job.id == job_id).one()
        user = user_tuple[0]
    else:
        user = job.user

    return user + "_" + str(job_id)


def job_fragged(job_id):
    """Set the flag 'ToFrag' of a job to 'No'"""
    db.query(FragJob).filter(FragJob.job_id == job_id).update(
        {FragJob.state: "FRAGGED"}, synchronize_session=False
    )
    db.commit()


def job_arm_leon_timer(job_id):
    """Set the state to TIMER_ARMED of job"""
    db.query(FragJob).filter(FragJob.job_id == job_id).update(
        {FragJob.state: "TIMER_ARMED"}, synchronize_session=False
    )
    db.commit()


def job_refrag(job_id):
    """Set the state to LEON of job"""
    db.query(FragJob).filter(FragJob.job_id == job_id).update(
        {FragJob.state: "LEON"}, synchronize_session=False
    )
    db.commit()


def job_leon_exterminate(job_id):
    """Set the state LEON_EXTERMINATE of job"""
    db.query(FragJob).filter(FragJob.job_id == job_id).update(
        {FragJob.state: "LEON_EXTERMINATE"}, synchronize_session=False
    )
    db.commit()


def get_frag_date(job_id):
    """Get the date of the frag of a job"""
    res = db.query(FragJob.date).filter(FragJob.job_id == job_id).one()
    return res[0]


def set_job_exit_code(job_id, exit_code):
    """Set exit code to just finished job"""
    db.query(Job).filter(Job.id == job_id).update(
        {Job.exit_code: exit_code}, synchronize_session=False
    )
    db.commit()


def check_end_of_job(
    job_id, exit_script_value, error, hosts, user, launchingDirectory, epilogue_script
):
    """check end of job"""
    log_jid = "[" + str(job_id) + "] "
    # TODO: do we really need to get refresh job data by reget it ? (see bipbip usage)
    job = get_job(job_id)

    do_finishing_sequence = True
    notify_almighty_term = False
    events = []
    if job.state in ["Running", "Launching", "Suspended", "Resuming"]:
        logger.debug(log_jid + "Job is ended")
        set_finish_date(job)
        set_job_state(job_id, "Finishing")

        if exit_script_value:
            try:
                set_job_exit_code(job_id, int(exit_script_value))
            except ValueError:
                # TODO log a warning
                # exit_script_value is not an int ( equal to 'N'),
                # nothing to do.
                pass

        if error == 0:
            logger.debug(log_jid + "User Launch completed OK")
            events.append(
                ("SWITCH_INTO_TERMINATE_STATE", log_jid + "Ask to change the job state")
            )
            notify_almighty_term = True
        elif error == 1:
            # Prologue error
            events.append(("PROLOGUE_ERROR", log_jid + "error of oarexec prologue"))
        elif error == 2:
            # Epilogue error
            events.append(("EPILOGUE_ERROR", log_jid + "error of oarexec epilogue"))
        elif error == 3:
            # Oarexec is killed by Leon normaly
            events.append(
                ("SWITCH_INTO_ERROR_STATE", log_jid + "Ask to change the job state")
            )
            logger.debug(log_jid + "The job was killed by Leon.")
            job_types = get_job_types(job_id)
            if ("besteffort" in job_types.keys()) and (
                "idempotent" in job_types.keys()
            ):
                if is_an_event_exists(job_id, "BESTEFFORT_KILL"):
                    new_job_id = resubmit_job(job_id)
                    logger.warning(
                        "We resubmit the job "
                        + str(job_id)
                        + " (new id = "
                        + str(new_job_id)
                        + ") because it is a besteffort and idempotent job."
                    )
                    events.append(
                        (
                            "RESUBMIT_JOB_AUTOMATICALLY",
                            log_jid
                            + "The job "
                            + str(job_id)
                            + " is a besteffort and idempotent job so we resubmit it (new id = "
                            + str(new_job_id),
                        )
                    )
        elif error == 5:
            # Oarexec is not able to write in the node
            events.append(
                (
                    "CANNOT_WRITE_NODE_FILE",
                    log_jid + 'oarexec cannot create the node file"',
                )
            )
        elif error == 6:
            # Oarexec can not write its pid file
            events.append(
                (
                    "CANNOT_WRITE_PID_FILE",
                    log_jid + "oarexec cannot create its pid file",
                )
            )
        elif error == 7:
            # Can t get shell of user
            events.append(
                (
                    "USER_SHELL",
                    log_jid
                    + "Cannot get shell of user "
                    + str(user)
                    + ", so I suspect node "
                    + hosts[0],
                )
            )
        elif error == 8:
            # Oarexec can not create tmp directory
            events.append(
                (
                    "CANNOT_CREATE_TMP_DIRECTORY",
                    log_jid
                    + "oarexec cannot create tmp directory on "
                    + hosts[0]
                    + ": "
                    + config["OAREXEC_DIRECTORY"],
                )
            )
        elif error == 10:
            # Oarexecuser.sh can not go into working directory
            events.append(
                ("SWITCH_INTO_ERROR_STATE", log_jid + "Ask to change the job state")
            )
            events.append(
                (
                    "WORKING_DIRECTOR",
                    log_jid
                    + "Cannot go into the working directory "
                    + launchingDirectory
                    + " of the job on node "
                    + hosts[0],
                )
            )
        elif error == 20:
            # Oarexecuser.sh can not write stdout and stderr files
            events.append(
                ("SWITCH_INTO_ERROR_STATE", log_jid + "Ask to change the job state")
            )
            events.append(
                (
                    "OUTPUT_FILES",
                    log_jid
                    + "Cannot create .stdout and .stderr files in "
                    + launchingDirectory
                    + " on the node "
                    + hosts[0],
                )
            )
        elif error == 12:
            # oarexecuser.sh can not go into working directory and epilogue is in error
            events.append(
                ("SWITCH_INTO_ERROR_STATE", log_jid + "Ask to change the job state")
            )
            warning = (
                log_jid
                + "Cannot go into the working directory "
                + launchingDirectory
                + " of the job on node "
                + hosts[0]
                + " AND epilogue is in error"
            )
            events.append(("WORKING_DIRECTORY", warning))
            events.append(("EPILOGUE_ERROR", warning))
        elif error == 22:
            # oarexecuser.sh can not create STDOUT and STDERR files and epilogue is in error
            events.append(
                ("SWITCH_INTO_ERROR_STATE", log_jid + "Ask to change the job state")
            )
            warning = (
                log_jid
                + "Cannot create STDOUT and STDERR files AND epilogue is in error"
            )
            events.append(("OUTPUT_FILES", warning))
            events.append(("EPILOGUE_ERROR", warning))
        elif error == 30:
            # oarexec timeout on bipbip hashtable transfer via SSH
            events.append(
                (
                    "SSH_TRANSFER_TIMEOUT",
                    log_jid + "Timeout SSH hashtable transfer on " + hosts[0],
                )
            )
        elif error == 31:
            # oarexec got a bad hashtable dump from bipbip
            events.append(
                ("BAD_HASHTABLE_DUMP", log_jid + "Bad hashtable dump on " + hosts[0])
            )
        elif error == 33:
            # oarexec received a SIGUSR1 signal and there was an epilogue error
            events.append(
                ("SWITCH_INTO_TERMINATE_STATE", log_jid + "Ask to change the job state")
            )
            events.append(
                (
                    "EPILOGUE_ERROR",
                    log_jid
                    + "oarexec received a SIGUSR1 signal and there was an epilogue error",
                )
            )
        elif error == 34:
            # Oarexec received a SIGUSR1 signal
            events.append(
                ("SWITCH_INTO_TERMINATE_STATE", log_jid + "Ask to change the job state")
            )
            logger.debug(
                log_jid
                + "oarexec received a SIGUSR1 signal; so INTERACTIVE job is ended"
            )
            notify_almighty_term = True
        elif error == 50:
            # launching oarexec timeout
            events.append(
                (
                    "LAUNCHING_OAREXEC_TIMEOUT",
                    log_jid
                    + "launching oarexec timeout, exit value = "
                    + str(error)
                    + "; the job $job_id is in Error and the node "
                    + hosts[0]
                    + "is Suspected",
                )
            )
        elif error == 40:
            # oarexec received a SIGUSR2 signal
            events.append(
                ("SWITCH_INTO_TERMINATE_STATE", log_jid + "Ask to change the job state")
            )
            logger.debug(
                log_jid
                + "oarexec received a SIGUSR2 signal; so user process has received a checkpoint signal"
            )
            notify_almighty_term = True
        elif error == 42:
            # oarexec received a user signal
            events.append(
                ("SWITCH_INTO_TERMINATE_STATE", log_jid + "Ask to change the job state")
            )
            logger.debug(
                log_jid
                + "oarexec received a SIGURG signal; so user process has received the user defined signal"
            )
            notify_almighty_term = True
        elif error == 41:
            # oarexec received a SIGUSR2 signal
            events.append(
                ("SWITCH_INTO_TERMINATE_STATE", log_jid + "Ask to change the job state")
            )
            warning = (
                log_jid
                + "oarexec received a SIGUSR2 signal and there was an epilogue error; so user process has received a checkpoint signal"
            )
            logger.debug(warning)
            events.append(("EPILOGUE_ERROR", warning))
            notify_almighty_term = True
        else:
            warning = (
                log_jid
                + "Error of oarexec, exit value = "
                + str(error)
                + "; the job is in Error and the node "
                + hosts[0]
                + " is Suspected; If this job is of type cosystem or deploy, check if the oar server is able to connect to the corresponding nodes, oar-node started"
            )
            events.append(("EXIT_VALUE_OAREXEC", warning))
    else:
        do_finishing_sequence = False
        logger.debug(
            log_jid + "I was previously killed or Terminated but I did not know that!!"
        )

    if do_finishing_sequence:
        job_finishing_sequence(epilogue_script, job_id, events)
    if notify_almighty_term:
        tools.notify_almighty("Term")

    tools.notify_almighty("BipBip")


def job_finishing_sequence(epilogue_script, job_id, events):
    if epilogue_script:
        # launch server epilogue
        cmd = [epilogue_script, str(job_id)]
        logger.debug("[JOB FINISHING SEQUENCE] Launching command : " + str(cmd))
        timeout = config["SERVER_PROLOGUE_EPILOGUE_TIMEOUT"]

        try:
            child = tools.Popen(cmd)
            return_code = child.wait(timeout)

            if return_code:
                msg = (
                    "[JOB FINISHING SEQUENCE] Server epilogue exit code: "
                    + str(return_code)
                    + " (!=0) (cmd: "
                    + str(cmd)
                    + ")"
                )
                logger.error(msg)
                events.append(("SERVER_EPILOGUE_EXIT_CODE_ERROR", msg, None))

        except OSError:
            logger.error("Cannot run: " + str(cmd))
        except TimeoutExpired:
            tools.kill_child_processes(child.pid)
            msg = (
                "[JOB FINISHING SEQUENCE] Server epilogue timeouted (cmd: "
                + str(cmd)
                + ")"
            )
            logger.error(msg)
            events.append(("SERVER_EPILOGUE_TIMEOUT", msg, None))

    job_types = get_job_types(job_id)
    if (
        ("deploy" not in job_types.keys())
        and ("cosystem" not in job_types.keys())
        and ("noop" not in job_types.keys())
    ):
        ###############
        # CPUSET PART #
        ###############
        # Clean all CPUSETs if needed
        if "JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD" in config:
            cpuset_field = config["JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD"]
            cpuset_name = get_job_cpuset_name(job_id)
            openssh_cmd = config["OPENSSH_CMD"]
            # TODO
            # if 'OAR_SSH_CONNECTION_TIMEOUT':
            #    tools.set_ssh_timeout(config['OAR_SSH_CONNECTION_TIMEOUT'])

            cpuset_file = ""
            if "JOB_RESOURCE_MANAGER_FILE" in config:
                cpuset_file = config["JOB_RESOURCE_MANAGER_FILE"]

            if not re.match(r"^\/", cpuset_file):
                if "OARDIR" not in os.environ:
                    msg = "$OARDIR variable envionment must be defined"
                    logger.error(msg)
                    raise Exception(msg)
                cpuset_file = os.environ["OARDIR"] + "/" + cpuset_file

            cpuset_full_path = ""
            cpuset_path = config["CPUSET_PATH"]
            if cpuset_path and cpuset_name:
                cpuset_full_path = cpuset_path + "/" + cpuset_name

            job = get_job(job_id)
            nodes_cpuset_fields = get_cpuset_values(
                cpuset_field, job.assigned_moldable_job
            )
            if nodes_cpuset_fields and len(nodes_cpuset_fields) > 0:
                logger.debug(
                    "[JOB FINISHING SEQUENCE] [CPUSET] ["
                    + str(job_id)
                    + "] Clean cpuset on each nodes"
                )
                taktuk_cmd = config["TAKTUK_CMD"]
                job_challenge, ssh_private_key, ssh_public_key = get_job_challenge(
                    job_id
                )
                ssh_public_key = format_ssh_pub_key(
                    ssh_public_key, cpuset_full_path, job.user, job.user
                )

                cpuset_data_hash = {
                    "job_id": job.id,
                    "name": cpuset_name,
                    "nodes": nodes_cpuset_fields,
                    "cpuset_path": cpuset_path,
                    "ssh_keys": {
                        "public": {
                            "file_name": config["OAR_SSH_AUTHORIZED_KEYS_FILE"],
                            "key": ssh_public_key,
                        },
                        "private": {
                            "file_name": get_private_ssh_key_file_name(cpuset_name),
                            "key": ssh_private_key,
                        },
                    },
                    "oar_tmp_directory": config["OAREXEC_DIRECTORY"],
                    "user": job.user,
                    "job_user": job.user,
                    "types": job_types,
                    "resources": "undef",
                    "node_file_db_fields": "undef",
                    "node_file_db_fields_distinct_values": "undef",
                    "array_id": job.array_id,
                    "array_index": job.array_index,
                    "stdout_file": job.stdout_file.replace("%jobid%", str(job.id)),
                    "stderr_file": job.stderr_file.replace("%jobid%", str(job.id)),
                    "launching_directory": job.launching_directory,
                    "job_name": job.name,
                    "walltime_seconds": "undef",
                    "walltime": "undef",
                    "project": job.project,
                    "log_level": config["LOG_LEVEL"],
                }
                # dict2hash_w_undef
                cpuset_data_str = limited_dict2hash_perl(cpuset_data_hash)
                tag, bad = tools.manage_remote_commands(
                    nodes_cpuset_fields.keys(),
                    cpuset_data_str,
                    cpuset_file,
                    "clean",
                    openssh_cmd,
                    taktuk_cmd,
                )
                if tag == 0:
                    msg = (
                        "[JOB FINISHING SEQUENCE] [CPUSET] ["
                        + str(job.id)
                        + "] Bad cpuset file: "
                        + cpuset_file
                    )
                    logger.error(msg)
                    events.append(("CPUSET_MANAGER_FILE", msg))
                elif len(bad) > 0:
                    logger.error(
                        "[job_finishing_sequence] ["
                        + str(job.id)
                        + " Cpuset error and register event CPUSET_CLEAN_ERROR on nodes : "
                        + str(bad)
                    )
                    events.append(
                        (
                            "CPUSET_CLEAN_ERROR",
                            "[job_finishing_sequence] OAR suspects nodes for the job "
                            + str(job.id)
                            + " : "
                            + str(bad),
                            str(bad),
                        )
                    )
        ####################
        # CPUSET PART, END #
        ####################

    # Execute PING_CHECKER if asked
    if (
        ("ACTIVATE_PINGCHECKER_AT_JOB_END" in config)
        and (config["ACTIVATE_PINGCHECKER_AT_JOB_END"] == "yes")
        and ("deploy" not in job_types.keys())
        and ("noop" not in job_types.keys())
    ):
        hosts = get_job_current_hostnames(job_id)
        logger.debug(
            "[job_finishing_sequence] ["
            + str(job_id)
            + "Run pingchecker to test nodes at the end of the job on nodes: "
            + str(hosts)
        )

        (pingcheck, bad_hosts) = tools.pingchecker(hosts)

        if (pingcheck and len(bad_hosts) > 0) or not pingcheck:
            if pingcheck:
                reason = "OAR suspects nodes"
                suspected_hosts = bad_hosts
            else:
                reason = "timeout triggered"
                suspected_hosts = hosts

            msg = "{}, job {}, nodes: {}".format(reason, job_id, suspected_hosts)
            logger.error(
                "[job_finishing_sequence] PING_CHECKER_NODE_SUSPECTED_END_JOB: " + msg
            )
            events.append(
                (
                    "PING_CHECKER_NODE_SUSPECTED_END_JOB",
                    "[job_finishing_sequence] " + msg,
                    suspected_hosts,
                )
            )

    for event in events:
        if len(event) == 2:
            ev_type, msg = event
            add_new_event(ev_type, job_id, msg)
        else:
            ev_type, msg, hosts = event
            add_new_event_with_host(ev_type, job_id, msg, hosts)

    # Just to force commit (from OAR2, useful for OAR3 ?)
    db.commit()

    if len(events) > 0:
        tools.notify_almighty("ChState")


def get_job_frag_state(job_id):
    """Get the frag_state value for a specific job"""
    res = db.query(FragJob.state).filter(FragJob.job_id == job_id).one_or_none()
    if res:
        return res[0]
    else:
        return None


def get_jobs_to_kill():
    """Return the list of jobs that have their frag state to LEON"""
    res = (
        db.query(Job)
        .filter(FragJob.state == "LEON")
        .filter(Job.id == FragJob.job_id)
        .filter(~Job.state.in_(("Error", "Terminated", "Finishing")))
        .all()
    )
    return res


def set_finish_date(job):
    """Set the stop time of the job passed in parameter to the current time
    (will be greater or equal to start time)"""
    date = tools.get_date()
    if date < job.start_time:
        date = job.start_time
    db.query(Job).filter(Job.id == job.id).update(
        {Job.stop_time: date}, synchronize_session=False
    )
    db.commit()


def set_running_date(job_id):
    """Set the starting time of the job passed in parameter to the current time"""
    date = tools.get_date()
    # In OAR2 gantt  moldable_id=0 is used to indicate time gantt orign, not in OAR3
    # gantt_date = get_gantt_date()
    # if gantt_date < date:
    #     date = gantt_date
    db.query(Job).filter(Job.id == job_id).update(
        {Job.start_time: date}, synchronize_session=False
    )
    db.commit()


def get_to_exterminate_jobs():
    """ "Return the list of jobs that have their frag state to LEON_EXTERMINATE"""
    res = (
        db.query(Job)
        .filter(FragJob.state == "LEON_EXTERMINATE")
        .filter(Job.id == FragJob.job_id)
        .all()
    )
    return res


def get_timer_armed_job():
    """Return the list of jobs that have their frag state to TIMER_ARMED"""
    res = (
        db.query(Job)
        .filter(FragJob.state == "TIMER_ARMED")
        .filter(Job.id == FragJob.job_id)
        .all()
    )
    return res


def archive_some_moldable_job_nodes(moldable_id, hosts):
    """Sets the index fields to LOG in the table assigned_resources"""
    # import pdb; pdb.set_trace()
    if config["DB_TYPE"] == "Pg":
        db.query(AssignedResource).filter(
            AssignedResource.moldable_id == moldable_id
        ).filter(Resource.id == AssignedResource.resource_id).filter(
            Resource.network_address.in_(tuple(hosts))
        ).update(
            {AssignedResource.index: "LOG"}, synchronize_session=False
        )
        db.commit()


def get_job_resources_properties(job_id):
    """Returns the list of resources properties associated to the job passed in parameter"""
    results = (
        db.query(Resource)
        .filter(Job.id == job_id)
        .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)
        .filter(AssignedResource.resource_id == Resource.id)
        .order_by(Resource.id)
        .all()
    )
    return results


def get_jobs_state(job_ids):
    """Returns state for each given jobs designated by their id"""
    results = (
        db.query(Job.id, Job.state)
        .filter(Job.id.in_(tuple(job_ids)))
        .order_by(Job.id)
        .all()
    )
    return results


def get_job_state(job_id):
    """Returns state for each given job designated by its id"""
    return db.query(Job.state).filter(Job.id == job_id).one()[0]


# WALLTIME CHANGE interaction between related Job tables and WalltimeChange one,
# see walltime.py for WalltimeChange only
class JobWalltimeChange(object):
    def __init__(self, **kwargs):
        self.mld_res_rqts = []
        for key, value in kwargs.items():
            setattr(self, key, value)


def get_jobs_with_walltime_change():
    """Get all jobs with extra time requests to process"""
    results = (
        db.query(
            Job.id,
            Job.queue_name,
            Job.start_time,
            Job.user,
            Job.name,
            MoldableJobDescription.walltime,
            WalltimeChange.pending,
            WalltimeChange.force,
            WalltimeChange.delay_next_jobs,
            WalltimeChange.granted,
            WalltimeChange.granted_with_force,
            WalltimeChange.granted_with_delay_next_jobs,
            AssignedResource.resource_id,
        )
        .filter(Job.state == "Running")
        .filter(Job.id == WalltimeChange.job_id)
        .filter(Job.assigned_moldable_job == MoldableJobDescription.id)
        .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)
        .filter(WalltimeChange.pending != 0)
        .all()
    )

    jobs_wtc = {}
    for x in results:
        (
            jid,
            queue,
            start_time,
            user,
            name,
            walltime,
            pending,
            force,
            delay_next_jobs,
            granted,
            granted_with_force,
            granted_with_delay_next_jobs,
            rid,
        ) = x

        if jid not in jobs_wtc:
            j = JobWalltimeChange(
                queue=queue,
                start_time=start_time,
                user=user,
                name=name,
                walltime=walltime,
                pending=pending,
                force=force,
                delay_next_jobs=delay_next_jobs,
                granted=granted,
                granted_with_force=granted_with_force,
                granted_with_delay_next_jobs=granted_with_delay_next_jobs,
                rids=[rid],
            )
            jobs_wtc[jid] = j
        else:
            j_wtc = jobs_wtc[jid]
            j_wtc.rids.append(rid)
    return jobs_wtc


def get_possible_job_end_time_in_interval(
    from_,
    to,
    resources,
    scheduler_job_security_time,
    delay_next_jobs,
    job_types,
    job_user,
    job_name,
):
    """Compute the possible end time for a job in an interval of the gantt of the predicted jobs"""
    first = to
    to += scheduler_job_security_time
    only_adv_reservations = ""
    if delay_next_jobs == "YES":
        only_adv_reservations = "j.reservation != 'None' AND"
    resources_str = ",".join([str(i) for i in resources])

    # NB: we do not remove jobs from the same user, because other jobs can be behind and this may change
    # the scheduling for other users. The user can always delete his job if needed for extratime.
    exclude = ""
    if "timesharing" in job_types:
        if (job_types["timesharing"] == "user,*") or (
            job_types["timesharing"] == "*,user"
        ):
            exclude += "((t.type = 'timesharing=user,*' OR t.type = 'timesharing=*,user') and j.job_user = {}) OR ".format(
                job_user
            )
        elif (
            job_types["timesharing"] == "name,*" or job_types["timesharing"] == "*,name"
        ):
            exclude += "((t.type = 'timesharing=*,name' OR t.type = 'timesharing=name,*') and j.job_name = {}) OR ".format(
                job_name
            )
        elif (
            job_types["timesharing"] == "name,user"
            or job_types["timesharing"] == "user,name"
        ):
            exclude += "((t.type = 'timesharing=user,name' OR t.type = 'timesharing=name,user') and j.job_name = '$job_name' AND j.job_user = '{}') OR ".format(
                job_user
            )
        elif job_types["timesharing"] == "*,*":
            exclude += "t.type = 'timesharing=*,*' OR"

    if "allowed" in job_types:
        exclude = "t.type = 'placeholder={}' OR ".format(job_types["allowed"])

    req = """
SELECT
  DISTINCT gp.start_time
FROM
  jobs j, moldable_job_descriptions m, gantt_jobs_predictions gp, gantt_jobs_resources gr
WHERE
  j.job_id = m.moldable_job_id AND
  {}
  gp.moldable_job_id = m.moldable_id AND
  gp.start_time > {} AND
  gp.start_time <= {} AND
  gr.moldable_job_id = gp.moldable_job_id AND
  NOT EXISTS (
    SELECT
      t.job_id
    FROM
      job_types t
    WHERE
      t.job_id = j.job_id AND (
      {}
      t.type = 'besteffort' )
  ) AND
  gr.resource_id IN ( {} )
    """.format(
        only_adv_reservations, from_, to, exclude, resources_str
    )
    raw_start_times = db.engine.execute(text(req))

    for start_time in raw_start_times.fetchall():
        if (not first) or (first > (start_time[0] - scheduler_job_security_time)):
            first = start_time[0] - scheduler_job_security_time - 1

    return first


def change_walltime(job_id, new_walltime, message):
    """Change the walltime of a job and add an event"""
    db.query(MoldableJobDescription).filter(
        MoldableJobDescription.job_id == job_id
    ).update({MoldableJobDescription.walltime: new_walltime}, synchronize_session=False)
    db.commit()
    add_new_event("WALLTIME", job_id, message, to_check="NO")
    db.commit()
