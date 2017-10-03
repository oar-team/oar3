# coding: utf-8
""" Functions to handle jobs"""
import os
import re
import random

from sqlalchemy import (func, text, distinct)
from sqlalchemy.orm import aliased
from sqlalchemy.orm.session import make_transient

from sqlalchemy.sql.expression import select

from procset import ProcSet

from oar.lib import (db, Job, MoldableJobDescription, JobResourceDescription,
                     JobResourceGroup, Resource, GanttJobsPrediction,
                     JobDependencie, GanttJobsResource, JobType,
                     JobStateLog, AssignedResource, FragJob,
                     get_logger, config, Challenge)

from oar.lib.resource_handling import get_current_resources_with_suspended_job

from oar.lib.psycopg2 import pg_bulk_insert
from oar.lib.event import add_new_event

from oar.kao.tools import update_current_scheduler_priority
import oar.lib.tools as tools

from oar.kao.helpers import extract_find_assign_args

logger = get_logger('oar.lib.job_handling')


''' Use

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

'''

NO_PLACEHOLDER = 0
PLACEHOLDER = 1
ALLOW = 2


class JobPseudo(object):
    ''' Define a simple job class without database counter part
    '''
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
    queue_name = 'default'
    user = ''
    project = ''

    def __init__(self, **kwargs):
        self.mld_res_rqts = []
        for key, value in kwargs.items():
            setattr(self, key, value)

    def simple_req(self, resources_req, walltime, resources_constraint):
        ''' Allow declaration of simple resources request.
        Examples:
        - j.simple_req(('node', 2), 60, [(1, 32)])
        - j.simple_req([('node', 2), ('core', 4)], 600, [(1, 64)])
        '''
        if type(resources_req) == tuple:
            res_req = [resources_req]
        else:
            res_req = resources_req
        self.mld_res_rqts = [(1, walltime, [(res_req, ProcSet(*resources_constraint))])]


def get_waiting_jobs(queue, reservation='None'):
    # TODO  fairsharing_nb_job_limit
    waiting_jobs = {}
    waiting_jids = []
    nb_waiting_jobs = 0

    for j in db.query(Job).filter(Job.state == "Waiting")\
                          .filter(Job.queue_name == queue)\
                          .filter(Job.reservation == reservation)\
                          .order_by(Job.id).all():
        jid = int(j.id)
        waiting_jobs[jid] = j
        waiting_jids.append(jid)
        nb_waiting_jobs += 1

    return (waiting_jobs, waiting_jids, nb_waiting_jobs)


def get_jobs_types(jids, jobs):
    import oar.kao.advanced_scheduling
    jobs_types = {}
    for j_type in db.query(JobType).filter(JobType.job_id.in_(tuple(jids))):
        jid = j_type.job_id
        job = jobs[jid]
        t_v = j_type.type.split("=")
        t = t_v[0]
        if t == "timesharing":
            job.ts = True
            job.ts_user, job.ts_name = t_v[1].split(',')
        elif t == "placeholder":
            job.ph = PLACEHOLDER
            job.ph_name = t_v[1]
        elif t == "allow":
            job.ph = ALLOW
            job.ph_name = t_v[1]
        elif t == "assign":
            job.assign = True
            raw_args = '='.join(t_v[1:])
            funcname, job.assign_args, job.assign_kwargs = extract_find_assign_args(raw_args)
            job.assign_func = getattr(oar.kao.advanced_scheduling, 'assign_%s' % funcname)
        elif t == "find":
            job.find = True
            raw_args = '='.join(t_v[1:])
            funcname, job.find_args, job.find_kwargs = extract_find_assign_args(raw_args)
            job.find_func = getattr(oar.kao.advanced_scheduling, 'find_%s' % funcname)
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


def get_data_jobs(jobs, jids, resource_set, job_security_time,
                  besteffort_duration=0):
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

    result = db.query(Job.id,
                      Job.properties,
                      MoldableJobDescription.id,
                      MoldableJobDescription.walltime,
                      JobResourceGroup.id,
                      JobResourceGroup.property,
                      JobResourceDescription.group_id,
                      JobResourceDescription.resource_type,
                      JobResourceDescription.value)\
        .filter(MoldableJobDescription.index == 'CURRENT')\
        .filter(JobResourceGroup.index == 'CURRENT')\
        .filter(JobResourceDescription.index == 'CURRENT')\
        .filter(Job.id.in_(tuple(jids)))\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .filter(JobResourceGroup.moldable_id == MoldableJobDescription.id)\
        .filter(JobResourceDescription.group_id == JobResourceGroup.id)\
        .order_by(MoldableJobDescription.id,
                  JobResourceGroup.id,
                  JobResourceDescription.order)\
        .all()
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

    job = None # Global

    for x in result:
        # remove res_order
        (j_id, j_properties,
         moldable_id,
         mld_id_walltime,
         jrg_id,
         jrg_grp_property,
         res_jrg_id,
         res_type,
         res_value) = x
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
                    jrg.append((jr_descriptions, res_constraints))
                    mld_res_rqts.append(
                        (prev_mld_id, prev_mld_id_walltime, jrg))

                prev_mld_id = moldable_id
                jrg = []
                jr_descriptions = []
                if besteffort_duration:
                    prev_mld_id_walltime = besteffort_duration
                else:
                    prev_mld_id_walltime = mld_id_walltime + job_security_time
        #
        # new job resources groupe_id
        #
        if jrg_id != prev_jrg_id:
            prev_jrg_id = jrg_id
            if jr_descriptions != []:
                jrg.append((jr_descriptions, res_constraints))
                jr_descriptions = []

        #
        # new set job descriptions
        #
        if res_jrg_id != prev_res_jrg_id:
            prev_res_jrg_id = res_jrg_id
            jr_descriptions = [(res_type, res_value)]

            #
            # determine resource constraints
            #
            if (j_properties == "" and (jrg_grp_property == "" or jrg_grp_property == "type = 'default'")):
                res_constraints = ProcSet(*resource_set.roid_itvs)
            else:
                and_sql = ''
                if j_properties and jrg_grp_property:
                    and_sql = ' AND '
                if j_properties is None:
                    j_properties = ''
                if jrg_grp_property is None:
                    jrg_grp_property  = ''

                sql_constraints = j_properties + and_sql + jrg_grp_property
                if sql_constraints in cache_constraints:
                    res_constraints = cache_constraints[sql_constraints]
                else:
                    request_constraints = db.query(
                        Resource.id).filter(text(sql_constraints)).all()
                    roids = [resource_set.rid_i2o[int(y[0])]
                             for y in request_constraints]
                    res_constraints = ProcSet(*roids)
                    cache_constraints[sql_constraints] = res_constraints
        else:
            # add next res_type , res_value
            jr_descriptions.append((res_type, res_value))

    # complete the last job
    jrg.append((jr_descriptions, res_constraints))
    mld_res_rqts.append((prev_mld_id, prev_mld_id_walltime, jrg))

    job.mld_res_rqts = mld_res_rqts
    job.key_cache = {}
    job.deps = []
    job.ts = False
    job.ph = NO_PLACEHOLDER
    job.assign = False
    job.find = False

    get_jobs_types(jids, jobs)
    get_current_jobs_dependencies(jobs)
    set_jobs_cache_keys(jobs)


def get_job_suspended_sum_duration(jid, now):

    suspended_duration = 0
    for j_state_log in db.query(JobStateLog).filter(JobStateLog.job_id == jid)\
                                            .filter((JobStateLog.job_state == 'Suspended') | (JobStateLog.job_state == 'Resuming')):

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

    job = None # global job

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
                if job.suspended == 'YES':
                    job.walltime += get_job_suspended_sum_duration(job.id, now)

            roid = resource_set.rid_i2o[r_id]
            roids.append(roid)
            rid2jid[roid] = j.id

        job.res_set = ProcSet(*roids)
        if job.state == 'Suspended':
            job.res_set = job.res_set - resource_set.suspendable_roid_itvs

        jobs_lst.append(job)
        jids.append(job.id)
        jobs[job.id] = job
        get_jobs_types(jids, jobs)

    return (jobs, jobs_lst, jids, rid2jid)


# TODO available_suspended_res_itvs, now
def get_scheduled_jobs(resource_set, job_security_time, now):
    result = db.query(Job,
                      GanttJobsPrediction.moldable_id,
                      GanttJobsPrediction.start_time,
                      MoldableJobDescription.walltime,
                      GanttJobsResource.resource_id)\
        .filter(MoldableJobDescription.index == 'CURRENT')\
        .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)\
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .order_by(Job.start_time, Job.id)\
        .all()

    jobs, jobs_lst, jids, rid2jid = extract_scheduled_jobs(result, resource_set,
                                                           job_security_time, now)

    return jobs_lst


def get_after_sched_no_AR_jobs(queue_name, resource_set, job_security_time, now):
    """ Get waiting jobs which are not AR and after scheduler round
    """
    result = db.query(Job,
                      GanttJobsPrediction.moldable_id,
                      GanttJobsPrediction.start_time,
                      MoldableJobDescription.walltime,
                      GanttJobsResource.resource_id)\
        .filter(MoldableJobDescription.index == 'CURRENT')\
        .filter(Job.queue_name == queue_name)\
        .filter(Job.state == 'Waiting')\
        .filter(Job.reservation == 'None')\
        .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)\
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .order_by(Job.start_time, Job.id)\
        .all()

    _, jobs_lst, _, _ = extract_scheduled_jobs(result, resource_set,
                                               job_security_time, now)

    return jobs_lst


def get_waiting_scheduled_AR_jobs(queue_name, resource_set, job_security_time, now):
    result = db.query(Job,
                      GanttJobsPrediction.moldable_id,
                      GanttJobsPrediction.start_time,
                      MoldableJobDescription.walltime,
                      GanttJobsResource.resource_id)\
        .filter(MoldableJobDescription.index == 'CURRENT')\
        .filter(Job.queue_name == queue_name)\
        .filter(Job.reservation == 'Scheduled')\
        .filter(Job.state == 'Waiting')\
        .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)\
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .order_by(Job.start_time, Job.id)\
        .all()

    _, jobs_lst, _, _ = extract_scheduled_jobs(result, resource_set,
                                               job_security_time, now)

    return jobs_lst

# TODO MOVE TO GANTT_HANDLING ???
def get_gantt_jobs_to_launch(resource_set, job_security_time, now):

    # get unlaunchable jobs
    # NOT USED launcher will manage these cases ??? (MUST BE CONFIRMED)
    #
    #result = db.query(distinct(Job.id))\
    #           .filter(GanttJobsPrediction.start_time <= now)\
    #           .filter(Job.state == "Waiting")\
    #           .filter(Job.id == MoldableJobDescription.job_id)\
    #           .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
    #           .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)\
    # AND (resources.state IN (\'Dead\',\'Suspected\',\'Absent\')
    #                   OR resources.next_state IN (\'Dead\',\'Suspected\',\'Absent\'))
    #
    #           .all()

    result = db.query(Job,
                      GanttJobsPrediction.moldable_id,
                      GanttJobsPrediction.start_time,
                      MoldableJobDescription.walltime,
                      GanttJobsResource.resource_id)\
        .filter(GanttJobsPrediction.start_time <= now)\
        .filter(Job.state == "Waiting")\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
        .filter(GanttJobsResource.moldable_id
                == GanttJobsPrediction.moldable_id)\
        .filter(Resource.id == GanttJobsResource.resource_id)\
        .filter(Resource.state == 'Alive')\
        .all()

    jobs, jobs_lst, _, rid2jid = extract_scheduled_jobs(result, resource_set,
                                                        job_security_time, now)

    return (jobs, jobs_lst, rid2jid)


def save_assigns(jobs, resource_set):
    # http://docs.sqlalchemy.org/en/rel_0_9/core/dml.html#sqlalchemy.sql.expression.Insert.values
    if len(jobs) > 0:
        logger.debug("nb job to save: " + str(len(jobs)))
        mld_id_start_time_s = []
        mld_id_rid_s = []
        for j in jobs.values() if isinstance(jobs, dict) else jobs:
            if j.start_time > -1:
                logger.debug("job_id to save: " + str(j.id))
                mld_id_start_time_s.append(
                    {'moldable_job_id': j.moldable_id, 'start_time': j.start_time})
                riods = list(j.res_set)
                mld_id_rid_s.extend(
                    [{'moldable_job_id': j.moldable_id,
                      'resource_id': resource_set.rid_o2i[rid]} for rid in riods])

        logger.info("save assignements")

        db.session.execute(
            GanttJobsPrediction.__table__.insert(), mld_id_start_time_s)
        db.session.execute(GanttJobsResource.__table__.insert(), mld_id_rid_s)
        db.commit()

    # INSERT INTO  gantt_jobs_predictions  (moldable_job_id,start_time) VALUES
    # INSERT INTO  gantt_jobs_resources (moldable_job_id,resource_id) VALUES


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
                    [(j.moldable_id, resource_set.rid_o2i[rid]) for rid in riods])

        logger.info("save assignements")

        with db.engine.connect() as to_conn:
            cursor = to_conn.connection.cursor()
            pg_bulk_insert(cursor, db['gantt_jobs_predictions'], mld_id_start_time_s,
                           ('moldable_job_id', 'start_time'), binary=True)
            pg_bulk_insert(cursor, db['queues'], mld_id_rid_s,
                           ('moldable_job_id', 'resource_id'), binary=True)
    
            
def get_current_jobs_dependencies(jobs):
    # retrieve jobs dependencies *)
    # return an hashtable, key = job_id, value = list of required jobs *)

    req = db.query(JobDependencie, Job.state, Job.exit_code)\
            .filter(JobDependencie.index == "CURRENT")\
            .filter(Job.id == JobDependencie.job_id_required)\
            .all()

    for x in req:
        j_dep, state, exit_code = x
        if j_dep.job_id not in jobs:
            # This fact have no particular impact
            logger.warning(" during get dependencies for current job %s is not in waiting state" % str(j_dep.job_id))
        else:
            jobs[j_dep.job_id].deps.append(
                (j_dep.job_id_required, state, exit_code))


def get_current_not_waiting_jobs():
    jobs = db.query(Job).filter(Job.state != 'Waiting').all()
    jobs_by_state = {}
    for job in jobs:
        if job.state not in jobs_by_state:
            jobs_by_state[job.state] = []
        jobs_by_state[job.state].append(job)
    return (jobs_by_state)


def set_job_start_time_assigned_moldable_id(jid, start_time, moldable_id):
    # db.query(Job).update({Job.start_time:
    # start_time,Job.assigned_moldable_job: moldable_id}).filter(Job.id ==
    # jid)
    db.query(Job).filter(Job.id == jid).update(
        {Job.start_time: start_time, Job.assigned_moldable_job: moldable_id})
    db.commit()


def set_jobs_start_time(tuple_jids, start_time):

    db.query(Job).filter(Job.id.in_(tuple_jids)).update(
        {Job.start_time: start_time}, synchronize_session=False)
    db.commit()


# NO USED
def add_resource_jobs_pairs(tuple_mld_ids):  # pragma: no cover
    resources_mld_ids = db.query(GanttJobsResource)\
                          .filter(GanttJobsResource.job_id.in_(tuple_mld_ids))\
                          .all()

    assigned_resources = [{'moldable_job_id': res_mld_id.moldable_id,
                           'resource_id': res_mld_id.resource_id} for res_mld_id in resources_mld_ids]

    db.session.execute(AssignedResource.__table__.insert(), assigned_resources)
    db.commit()

def add_resource_job_pairs(moldable_id):
    resources_mld_ids = db.query(GanttJobsResource)\
                          .filter(GanttJobsResource.moldable_id == moldable_id)\
                          .all()

    assigned_resources = [{'moldable_job_id': res_mld_id.moldable_id,
                           'resource_id': res_mld_id.resource_id} for res_mld_id in resources_mld_ids]

    db.session.execute(AssignedResource.__table__.insert(), assigned_resources)
    db.commit()

# TODO MOVE TO gantt_handling
def get_gantt_waiting_interactive_prediction_date():
    req = db.query(Job.id,
                   Job.info_type,
                   GanttJobsPrediction.start_time,
                   Job.message)\
        .filter(Job.state == 'Waiting')\
        .filter(Job.type == 'INTERACTIVE')\
        .filter(Job.reservation == 'None')\
        .filter(MoldableJobDescription.job_id == Job.id)\
        .filter(GanttJobsPrediction.moldable_id == MoldableJobDescription.id)\
        .all()
    return req


def insert_job(**kwargs):
    """ Insert job in database

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

    default_values = {'launching_directory': "", 'checkpoint_signal': 0, 'properties': ""}

    for k, v in default_values.items():
        if k not in kwargs:
            kwargs[k] = v

    if 'res' in kwargs:
        res = kwargs.pop('res')
    else:
        res = [(60, [('resource_id=1', "")])]

    if 'types' in kwargs:
        types = kwargs.pop('types')
    else:
        types = []

    if 'queue_name' not in kwargs:
        kwargs['queue_name'] = 'default'

    if 'user' in kwargs:
        kwargs['job_user'] = kwargs.pop('user')

    ins = Job.__table__.insert().values(**kwargs)
    result = db.session.execute(ins)
    job_id = result.inserted_primary_key[0]

    mld_jid_walltimes = []
    res_grps = []

    for res_mld in res:
        w, res_grp = res_mld
        mld_jid_walltimes.append(
            {'moldable_job_id': job_id, 'moldable_walltime': w})
        res_grps.append(res_grp)

    result = db.session.execute(MoldableJobDescription.__table__.insert(),
                                mld_jid_walltimes)

    if len(mld_jid_walltimes) == 1:
        mld_ids = [result.inserted_primary_key[0]]
    else:
        r = db.query(MoldableJobDescription.id)\
              .filter(MoldableJobDescription.job_id == job_id).all()
        mld_ids = [x for e in r for x in e]

    for mld_idx, res_grp in enumerate(res_grps):
        # job_resource_groups
        mld_id_property = []
        res_hys = []

        moldable_id = mld_ids[mld_idx]

        for r_hy_prop in res_grp:
            (res_hy, properties) = r_hy_prop
            mld_id_property.append({'res_group_moldable_id': moldable_id,
                                    'res_group_property': properties})
            res_hys.append(res_hy)

        result = db.session.execute(JobResourceGroup.__table__.insert(),
                                    mld_id_property)

        if len(mld_id_property) == 1:
            grp_ids = [result.inserted_primary_key[0]]
        else:
            r = db.query(JobResourceGroup.id)\
                  .filter(JobResourceGroup.moldable_id == moldable_id).all()
            grp_ids = [x for e in r for x in e]

        # job_resource_descriptions
        for grp_idx, res_hy in enumerate(res_hys):
            res_description = []
            for idx, val in enumerate(res_hy.split('/')):
                tv = val.split('=')
                res_description.append({'res_job_group_id': grp_ids[grp_idx],
                                        'res_job_resource_type': tv[0],
                                        'res_job_value': tv[1],
                                        'res_job_order': idx})

            db.session.execute(JobResourceDescription.__table__.insert(),
                               res_description)

    if types:
        ins = [{'job_id': job_id, 'type': typ} for typ in types]
        db.session.execute(JobType.__table__.insert(), ins)

    return job_id

def resubmit_job(job_id):
    """Resubmit a job and give the new job_id"""

    user = os.environ['OARDO_USER']
    job = get_job(job_id)

    if job is None:
        return 0
    if job.type != 'PASSIVE':
        return -1
    if (job.state != 'Error') and (job.state != 'Terminated') and (job.state != 'Finishing'):
        return -2
    if (user != job.user) and (user != 'oar') and (user != 'root'):
        return -3
    
    # Verify the content of the ssh keys
    job_challenge, ssh_private_key, ssh_public_key = get_job_challenge(job_id)
    if (ssh_public_key != '') or (ssh_private_key != ''):
        # Check if the keys are used by other jobs
        if get_count_same_ssh_keys_current_jobs(user, ssh_private_key, ssh_public_key) > 0:
            return -4

    date = tools.get_date()
    # Detach and prepare old job to be reinserted
    db.session.expunge(job)
    make_transient(job)
    job.id = None
    job.state = 'Hold'
    job.date = date
    if job.reservation is None:
        job.start_time = 0
    job.message = ''
    job.scheduler_info = ''
    job.exit_code = None
    job.assigned_moldable_job = 0
    
    db.session.add(job)
    db.session.flush()

    new_job_id = job.id

    # Insert challenge and ssh_keys
    random_number = random.randint(1, 1000000000000)
    ins = Challenge.__table__.insert().values(
        {'job_id': new_job_id, 'challenge': random_number,
         'ssh_private_key': ssh_private_key, 'ssh_public_key': ssh_public_key})
    db.session.execute(ins)

    # Duplicate job resource description requirements
    # Retrieve modable_job_description
    modable_job_descriptions = db.query(MoldableJobDescription)\
                                .filter(MoldableJobDescription.job_id == jobd_id).all()

    for mdl_job_descr in modable_job_descriptions:
         res = db.session.execute(MoldableJobDescription.__table__.insert(),
                                  {'moldable_job_id': new_job_id,
                                   'moldable_walltime': mdl_job_descr.walltime})
         moldable_id = res.inserted_primary_key[0]

         job_resource_groups = db.query(JobResourceGroup)\
                                 .filter(JobResourceGroup.moldable_id == mdl_job_descr.id).all()
         
         for job_res_grp in job_resource_groups:
             res = db.session.execute(JobResourceGroup.__table__.insert(),
                                      {'res_group_moldable_id': moldable_id,
                                       'res_group_property':  job_res_grp.property})
             res_group_id = res.inserted_primary_key[0]

             job_resource_descriptions = db.query(JobResourceDescription)\
                                           .filter(JobResourceDescription.group_id == job_res_grp.id).all()

             for job_res_descr in job_resource_descriptions:
                 db.session.execute(JobResourceGroup.__table__.insert(),
                                    {'res_job_group_id': res_group_id,
                                     'res_job_resource_type': job_res_descr.resource_type,
                                     'res_job_value': job_res_descr.value,
                                     'res_job_order': job_res_descr.order})
                 
    # Duplicate job types
    job_types = db.query(JobType).filter(JobType.job_id == job_id).all()
    new_job_types = [{'job_id': new_job_id, 'type': jt.type} for jt in job_typess]

    db.session.execute(JobType.__table__.insert(), new_job_types)

    # Update job dependencies
    db.query(JobDependencie).filter(JobDependencie.job_id_required  == job_id)\
                            .update({'job_id_required ': new_job_id})
    
    # Update job state to waintg
    db.query(Job).filter(Job.id == new_job_id).update({'state': 'Waiting'})

    # Emit job state log
    db.session.execute(JobStateLog.__table__.insert(),
                       {'job_id': new_job_id, 'job_state': 'Waiting', 'date_start': date})       
         
    db.commit()                                                
        
    return new_job_id
    

def is_job_already_resubmitted(job_id):
    '''Check if the job was already resubmitted
    args : db ref, job id'''

    count_query = select([func.count()]).select_from(Job).where(Job.resubmit_job_id == job_id)
    return db.session.execute(count_query).scalar()


def set_job_resa_state(job_id, state):
    ''' sets the reservation field of the job of id passed in parameter
    parameters : base, jobid, state
    return value : None
    side effects : changes the field state of the job in the table Jobs
    '''
    db.query(Job).filter(Job.id == job_id).update({Job.reservation: state})
    db.commit()


def set_job_message(job_id, message):
    db.query(Job).filter(Job.id == job_id).update({Job.message: message})
    db.commit()


def get_waiting_reservation_jobs_specific_queue(queue_name):
    '''Get all waiting reservation jobs in the specified queue
    parameter : database ref, queuename
    return an array of job informations
    '''
    waiting_scheduled_ar_jobs = db.query(Job)\
                                  .filter((Job.state == 'Waiting')
                                          |
                                          (Job.state == 'toAckReservation'))\
                                  .filter(Job.reservation == 'Scheduled')\
                                  .filter(Job.queue_name == queue_name)\
                                  .order_by(Job.id).all()
    return waiting_scheduled_ar_jobs


def update_scheduler_last_job_date(date, moldable_id):
    ''' used to allow search_idle_nodes to operate for dynamic node management feature (Hulot)
    '''

    if db.dialect == "sqlite":
        subquery = db.query(AssignedResource.resource_id).filter_by(moldable_id=moldable_id)\
                     .subquery()
        db.query(Resource).filter(Resource.id.in_(subquery))\
                          .update({Resource.last_job_date: date}, synchronize_session=False)

    else:
        db.query(Resource).filter(AssignedResource.moldable_id == moldable_id)\
                          .filter(Resource.id == AssignedResource.resource_id)\
                          .update({Resource.last_job_date: date}, synchronize_session=False)
    db.commit()


# Get all waiting reservation jobs
# parameter : database ref
# return an array of moldable job informations
def get_waiting_reservations_already_scheduled(resource_set, job_security_time):

    result = db.query(Job,
                      GanttJobsPrediction.start_time, GanttJobsResource.resource_id,
                      MoldableJobDescription.walltime, MoldableJobDescription.id)\
        .filter((Job.state == 'Waiting') | (Job.state == 'toAckReservation'))\
        .filter(Job.reservation == 'Scheduled')\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .filter(GanttJobsPrediction.moldable_id == MoldableJobDescription.id)\
        .filter(GanttJobsResource.moldable_id == MoldableJobDescription.id)\
        .order_by(Job.id).all()

    first_job = True
    jobs = {}
    jids = []

    prev_jid = 0
    roids = []

    job = None # global job

    if result:
        for x in result:
            j, start_time, resource_id, walltime, moldable_id = x

            if j.id != prev_jid:
                if first_job:
                    first_job = False
                else:
                    job.res_set = ProcSet(*roids)
                    jids.append(job.id)
                    jobs[job.id] = job
                    roids = []

                prev_jid = j.id
                job = j
                job.start_time = start_time
                job.walltime = walltime + job_security_time

            roids.append(resource_set.rid_i2o[resource_id])

        job.res_set = ProcSet(*roids)

        jids.append(job.id)
        jobs[job.id] = job

        get_jobs_types(jids, jobs)

    return (jids, jobs)

# TODO MOVE TO GANTT_HANDLING
def gantt_flush_tables(reservations_to_keep_mld_ids):
    '''Flush gantt tables but keep accepted advance reservations'''

    if reservations_to_keep_mld_ids != []:
        logger.debug("reservations_to_keep_mld_ids[0]: " + str(reservations_to_keep_mld_ids[0]))
        db.query(GanttJobsPrediction)\
          .filter(~GanttJobsPrediction.moldable_id.in_(tuple(reservations_to_keep_mld_ids)))\
          .delete(synchronize_session=False)
        db.query(GanttJobsResource)\
          .filter(~GanttJobsResource.moldable_id.in_(tuple(reservations_to_keep_mld_ids)))\
          .delete(synchronize_session=False)
    else:
        db.query(GanttJobsPrediction).delete()
        db.query(GanttJobsResource).delete()

    db.commit()


def get_jobs_in_multiple_states(states, resource_set):

    result = db.query(Job, AssignedResource.moldable_id, AssignedResource.resource_id)\
               .filter(Job.state.in_(tuple(states)))\
               .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)\
               .order_by(Job.id).all()

    first_job = True
    jobs = {}

    prev_jid = 0
    roids = []

    job = None # global job

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

    result = db.query(Job.id, Job.state)\
               .filter(Job.state.in_(tuple(states)))\
               .order_by(Job.id).all()

    jids_states = {}

    if result:
        for x in result:
            jid, state = x
            jids_states[jid] = state
    return jids_states

def set_moldable_job_max_time(moldable_id, walltime):
    """Set walltime for a moldable job"""
    db.query(MoldableJobDescription)\
      .filter(MoldableJobDescription.id == moldable_id)\
      .update({MoldableJobDescription.walltime: walltime})

    db.commit()

# TODO MOVE TO GANTT_HANDLING
def set_gantt_job_start_time(moldable_id, current_time_sec):
    """Update start_time in gantt for a specified job"""
    db.query(GanttJobsPrediction)\
      .filter(GanttJobsPrediction.moldable_id == moldable_id)\
      .update({GanttJobsPrediction.start_time: current_time_sec})

    db.commit()

# TODO MOVE TO GANTT_HANDLING
def remove_gantt_resource_job(moldable_id, job_res_set, resource_set):
    if len(job_res_set) != 0:
        resource_ids = [resource_set.rid_o2i[rid] for rid in job_res_set]

        db.query(GanttJobsResource)\
          .filter(GanttJobsResource.moldable_id == moldable_id)\
          .filter(~GanttJobsResource.resource_id.in_(tuple(resource_ids)))\
          .delete(synchronize_session=False)

        db.commit()


def is_timesharing_for_two_jobs(j1, j2):
    if ('timesharing' in j1.types) and ('timesharing' in j2.types):
        t1 = j1.types['timesharing']
        t2 = j2.types['timesharing']

        if (t1 == '*,*') and (t2 == '*,*'):
            return True

        if (j1.user == j2.user) and (j1.name == j2.name):
            return True

        if (j1.user == j2.user) and (t1 == 'user,*') and (t2 == 'user,*'):
            return True

        if (j1.name == j2.name) and (t1 == '*,name') and (t1 == '*,name'):
            return True

    return False

#TODO MOVE TO resource_handling
def get_jobs_on_resuming_job_resources(job_id):
    '''Return the list of jobs running on resources allocated to another given job'''
    j1 = aliased(Job)
    j2 = aliased(Job)
    a1 = aliased(AssignedResource)
    a2 = aliased(AssignedResource)

    states = ('toLaunch', 'toError', 'toAckReservation', 'Launching', 'Running ', 'Finishing')

    result = db.query(distinct(j2.id))\
               .filter(a1.index == 'CURRENT')\
               .filter(a2.index == 'CURRENT')\
               .filter(j1.id == job_id)\
               .filter(j1.id != j2.id)\
               .filter(a1.moldable_id == j1.assigned_moldable_job)\
               .filter(a2.resource_id == a1.resource_id)\
               .filter(j2.state.in_(states))\
               .all()

    return result


def resume_job_action(job_id):
    '''resume_job_action performs all action when a job is suspended'''

    set_job_state(job_id, 'Running')

    resources = get_current_resources_with_suspended_job()
    if resources != ():
        db.query(Resource)\
          .filter(~Resource.id.in_(resources))\
          .update({Resource.suspended_jobs: 'NO'}, synchronize_session=False)

    else:
        db.query(Resource)\
          .update({Resource.suspended_jobs: 'NO'}, synchronize_session=False)

    db.commit()


# get_cpuset_values_for_a_moldable_job
# get cpuset values for each nodes of a MJob
def get_cpuset_values(cpuset_field, moldable_id):
    # TODO TOFINISH
    raise  NotImplementedError('get_cpuset_values is NOT ENTIRELY IMPLEMENTED')
    logger.warning("get_cpuset_values is NOT ENTIRELY IMPLEMENTED")
    sql_where_string = "\'0\'"
    if "SCHEDULER_RESOURCES_ALWAYS_ASSIGNED_TYPE" in config:
        resources_to_always_add_type = config["SCHEDULER_RESOURCES_ALWAYS_ASSIGNED_TYPE"]
    else:
        resources_to_always_add_type = ""

    # TODO
    if resources_to_always_add_type != "":
        sql_where_string = "resources.type = \'$resources_to_always_add_type\'"

    # TODO TOFINISH  
    results = db.query(Resource)\
                .filter(AssignedResource.moldable_id == moldable_id)\
                .filter(AssignedResource.resource_id == Resource.id)


    # my $sth = $dbh->prepare("   SELECT resources.network_address, resources.$cpuset_field
    #                            FROM resources, assigned_resources
    #                            WHERE
    #                                assigned_resources.moldable_job_id = $moldable_job_id AND
    #                                assigned_resources.resource_id = resources.resource_id AND
    #                                resources.network_address != \'\' AND
    #                                (resources.type = \'default\' OR
    #                                 $sql_where_string)
    #                            GROUP BY resources.network_address, resources.$cpuset_field
    #                        ");

    return results

def get_array_job_ids(array_id):
    """ Get all the job_ids of a given array of job identified by its id"""
    results = db.query(Job.id).filter(Job.array_id == array_id)\
                .order_by(Job.id).all()
    job_ids = [r[0] for r in results]
    return job_ids

def get_job_ids_with_given_properties(sql_property):
    """Returns the job_ids with specified properties parameters : base, where SQL constraints."""
    results = db.query(Job.id).filter(text(sql_property))\
                .order_by(Job.id).all()
    return results

def get_job(job_id):
    try:
        job = db.query(Job).filter(Job.id == job_id).one()
    except Exception as e:
        logger.warning("get_job(" + str(job_id) + ") raises execption: " + str(e))
        return None
    else:
        return job


def frag_job(job_id, user=None):
    """Sets the flag 'ToFrag' of a job to 'Yes' which will threshold job deletion"""
    if not user: 
        if 'OARDO_USER' in os.environ:
            user = os.environ['OARDO_USER']
        else:
            user = os.environ['USER']

    job = get_job(job_id)

    if not job:
        return -3

    if (user == job.user) or (user == 'oar') or (user == 'root'):
        res = db.query(FragJob).filter(FragJob.job_id == job_id).all()

        if len(res) == 0:

            date = tools.get_date()
            frajob = FragJob(job_id=job_id, date=date)
            db.add(frajob)
            db.commit()
            add_new_event("FRAG_JOB_REQUEST",
                          job_id, "User %s requested to frag the job %s"
                          % (user, str(job_id)))
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
        if 'OARDO_USER' in os.environ:
            user = os.environ['OARDO_USER']
        else:
            user = os.environ['USER']

    job = get_job(job_id)

    error_msg = 'Cannot checkpoint '
    if signal:
        error_msg = 'Cannot signal '
    error_msg += '{} ; '.format(job_id) 
    

    if job and (job.type == 'INTERACTIVE'):
        return (3, error_msg + 'The job is Interactive.')

    if job and ((user == job.user) or (user == 'oar') or (user == 'root')):
        if job.state == 'Running':
            if signal:
                add_new_event('CHECKPOINT', job_id,
                              'User {} requested a checkpoint on the job {}'.format(user, job_id))
            else:
                add_new_event('SIGNAL_{}'.format(signal), job_id,
                              "User {} requested the signal {} on the job {}"
                              .format(user, signal, job_id))
            return (0, None)
        else:
            return (2, error_msg + 'This job is not running.')
    else:
        return (1, error_msg + 'You are not the right user.')

def get_job_current_hostnames(job_id):
    """Returns the list of hosts associated to the job passed in parameter"""

    results = db.query(distinct(Resource.network_address))\
                .filter(AssignedResource.index == 'CURRENT')\
                .filter(MoldableJobDescription.index == 'CURRENT')\
                .filter(AssignedResource.resource_id == Resource.id)\
                .filter(MoldableJobDescription.id == AssignedResource.moldable_id)\
                .filter(MoldableJobDescription.job_id == job_id)\
                .filter(Resource.network_address != '')\
                .filter(Resource.type == 'default')\
                .order_by(Resource.network_address).all()

    return results



def get_job_types(job_id):
    """Returns a hash table with all types for the given job ID."""

    results = db.query(JobType.type).filter(JobType.id == job_id).all()

    res = {}
    for t in results:
        match = re.match(r'^\s*(token)\s*\:\s*(\w+)\s*=\s*(\d+)\s*$', t)
        if match:
            res[match.group(1)] = {match.group(2): match.group(3)}
        else:
            match = re.match(r'^\s*(\w+)\s*=\s*(.+)$', t)
            if match:
                res[match.group(1)] = match.group(2)
            else:
                res[t] = True
    return res

# log_job
# sets the index fields to LOG on several tables
# this will speed up future queries
# parameters : base, jobid
# return value : /


def log_job(job):  # pragma: no cover
    if db.dialect == "sqlite":
        return
    db.query(MoldableJobDescription)\
      .filter(MoldableJobDescription.index == 'CURRENT')\
      .filter(MoldableJobDescription.job_id == job.id)\
      .update({MoldableJobDescription.index: 'LOG'}, synchronize_session=False)

    db.query(JobResourceDescription)\
      .filter(MoldableJobDescription.job_id == job.id)\
      .filter(JobResourceGroup.moldable_id == MoldableJobDescription.id)\
      .filter(JobResourceDescription.group_id == JobResourceGroup.id) \
      .update({JobResourceDescription.index: 'LOG'}, synchronize_session=False)

    db.query(JobResourceGroup)\
      .filter(JobResourceGroup.index == 'CURRENT')\
      .filter(MoldableJobDescription.index == 'LOG')\
      .filter(MoldableJobDescription.job_id == job.id)\
      .filter(JobResourceGroup.moldable_id == MoldableJobDescription.id)\
      .update({JobResourceGroup.index: 'LOG'}, synchronize_session=False)

    db.query(JobType)\
      .filter(JobType.types_index == 'CURRENT')\
      .filter(JobType.job_id == job.id)\
      .update({JobType.types_index: 'LOG'}, synchronize_session=False)

    db.query(JobDependencie)\
      .filter(JobDependencie.index == 'CURRENT')\
      .filter(JobDependencie.job_id == job.id)\
      .update({JobDependencie.index: 'LOG'}, synchronize_session=False)

    if job.assigned_moldable_job != "0":
        db.query(AssignedResource)\
          .filter(AssignedResource.index == 'CURRENT')\
          .filter(AssignedResource.moldable_id == int(job.assigned_moldable_job))\
          .update({AssignedResource.index: 'LOG'},
                  synchronize_session=False)
    db.commit()

def set_job_state(jid, state):

    # TODO
    # TODO Later: notify_user
    # TODO Later: update_current_scheduler_priority

    result = db.query(Job).filter(Job.id == jid)\
                          .filter(Job.state != 'Error')\
                          .filter(Job.state != 'Terminated')\
                          .filter(Job.state != state)\
                          .update({Job.state: state})
    db.commit()

    if result == 1:  # OK for sqlite
        logger.debug(
            "Job state updated, job_id: " + str(jid) + ", wanted state: " + state)

        date = tools.get_date()

        # TODO: optimize job log
        db.query(JobStateLog).filter(JobStateLog.date_stop == 0)\
                             .filter(JobStateLog.job_id == jid)\
                             .update({JobStateLog.date_stop: date})
        db.commit()
        req = db.insert(JobStateLog).values(
            {'job_id': jid, 'job_state': state, 'date_start': date})
        db.session.execute(req)

        if state == "Terminated" or state == "Error" or state == "toLaunch" or \
           state == "Running" or state == "Suspended" or state == "Resuming":
            job = db.query(Job).filter(Job.id == jid).one()
            if state == "Suspend":
                tools.notify_user(job, "SUSPENDED", "Job is suspended.")
            elif state == "Resuming":                tools.notify_user(job, "RESUMING", "Job is resuming.")
            elif state == "Running":
                tools.notify_user(job, "RUNNING", "Job is running.")
            elif state == "toLaunch":
                update_current_scheduler_priority(job, "+2", "START")
            else:  # job is "Terminated" or ($state eq "Error")
                if job.stop_time < job.start_time:
                    db.query(Job).filter(Job.id == jid)\
                                 .update({Job.stop_time: job.start_time})
                    db.commit()

                if job.assigned_moldable_job != "0":
                    # Update last_job_date field for resources used
                    update_scheduler_last_job_date(
                        date, int(job.assigned_moldable_job))

                if state == "Terminated":
                    tools.notify_user(job, "END", "Job stopped normally.")
                else:
                    # Verify if the job was suspended and if the resource
                    # property suspended is updated
                    if job.suspended == "YES":
                        r = get_current_resources_with_suspended_job()

                        if r != ():
                            db.query(Resource).filter(~Resource.id.in_(r))\
                                              .update({Resource.suspended_jobs: 'NO'})

                        else:
                            db.query(Resource).update(
                                {Resource.suspended_jobs: 'NO'})
                        db.commit()

                    tools.notify_user(
                        job, "ERROR", "Job stopped abnormally or an OAR error occured.")
                #import pdb; pdb.set_trace()
                update_current_scheduler_priority(job, "-2", "STOP")

                # Here we must not be asynchronously with the scheduler
                log_job(job)
                # $dbh is valid so these 2 variables must be defined
                nb_sent = tools.notify_almighty("ChState")
                if nb_sent == 0:
                    logger.warning("Not able to notify almighty to launch the job " +
                                   str(job.id) + " (socket error)")

    else:
        logger.warning("Job is already termindated or in error or wanted state, job_id: " +
                       str(jid) + ", wanted state: " + state)

def hold_job(job_id, running, user=None):
    """sets the state field of a job to 'Hold'
    equivalent to set_job_state(base,jobid,"Hold") except for permissions on user
    parameters : jobid, running, user
    return value : 0 on success, -1 on error (if the user calling this method
    is not the user running the job)
    side effects : changes the field state of the job to 'Hold' in the table Jobs.
    """

    if not user:
        if 'OARDO_USER' in os.environ:
            user = os.environ['OARDO_USER']
        else:
            user = os.environ['USER']

    job = get_job(job_id)

    user_allowed_hold_resume = False
    if 'USERS_ALLOWED_HOLD_RESUME' in config and  config['USERS_ALLOWED_HOLD_RESUME'] == 'yes':
        user_allowed_hold_resume = True

    event_type = 'HOLD_WAITING_JOB'
    if running:
        event_type = 'HOLD_RUNNING_JOB'

    if job:
        if running and (not user_allowed_hold_resume) and (user != 'oar') and (user != 'root'):
            return -4
        elif (user == job.user) or (user == 'oar') or (user == 'root'):
            if ((job.state == 'Waiting') or (job.state == 'Resuming')) or\
               (running and (job.state == 'toLaunch' or job.state == 'Launching' or job.state == 'Running')):
                add_new_event(event_type, job_id,
                              'User {} launched oarhold on the job {}'.format(user, job_id))
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
        if 'OARDO_USER' in os.environ:
            user = os.environ['OARDO_USER']
        else:
            user = os.environ['USER']

    job = get_job(job_id)
    
    user_allowed_hold_resume = False
    if 'USERS_ALLOWED_HOLD_RESUME' in config and  config['USERS_ALLOWED_HOLD_RESUME'] == 'yes':
        user_allowed_hold_resume = True

    if job:
        if (job.state == 'Suspended') and (not user_allowed_hold_resume) and (user != 'oar') and (user != 'root'):
            return -4
        elif (user == job.user) or (user == 'oar') or (user == 'root'):
            if (job.state == 'Hold') or (job.state == 'Suspended'):
                add_new_event('RESUME_JOB', job_id,
                              'User {} launched oarresume on the job {}'.format(user, job_id))
                return 0
            return -3
        return -2
    else:
        return -1

def get_job_challenge(job_id):
    """gets the challenge string of a OAR Job
    parameters : base, jobid
    return value : challenge, ssh_private_key, ssh_public_key"""
    res =  db.query(Challenge).filter(Challenge.job_id == job_id).one()
    return (res.challenge, res.ssh_private_key, res.ssh_public_key)

def get_count_same_ssh_keys_current_jobs(user, ssh_private_key, ssh_public_key):
    """return the number of current jobs with the same ssh keys"""
    count_query = select([func.count(Challenge.job_id)]).select_from(Challenge, Job)\
                                                         .where(Challenge.job_id == Job.job_id)\
                                                         .where(Job.state.in_(('Waiting', 'Hold',
                                                                               'toLaunch','toError',
                                                                               'toAckReservation',
                                                                               'Launching','Running',
                                                                               'Suspended','Resuming')))\
                                                         .where(Challenge.ssh_private_key == ssh_private_key)\
                                                         .where(Challenge.ssh_public_key == ssh_public_key)\
                                                         .where(Job.user != user)\
                                                         .where(Challenge.ssh_private_key != '')
                                                                
    return db.session.execute(count_query).scalar()


def get_job_host_log(moldable_id):
    """Returns the list of hosts associated to the moldable job passed in parameter
    parameters : base, moldable_id
    return value : list of distinct hostnames"""

    results = db.query(distinct(Resource.network_address))\
                .filter(AssignedResource.moldable_id == moldable_id)\
                .filter(Resource.id == AssignedResource.resource_id)\
                .filter(Resource.network_address != '')\
                .filter(Resource.type == 'default').all()
    return results

def suspend_job_action(job_id, moldable_id):
    """perform all action when a job is suspended"""
    set_job_state(job_id, 'Suspended')
    db.query(Job).filter(Job.id == job_id).update({'suspend': 'YES'})
    
    resource_ids = get_current_resources_with_suspended_job()
    
    db.query(Resource).filter(Resource.id.in_(tuple(resource_ids))).update({'suspend_jobs': 'YES'})
    db.commit()

def get_job_cpuset_name(job_id, job=None):
    """Get the cpuset name for the given job"""
    user = None
    if job is None:
        user = db.query(Job.user).filter(Job.id == job_id).one()
    else:
        user = job.user

    return user + '_' + str(job_id)

def get_cpuset_values_for_a_moldable_job():
    raise NotImplementedError('TODO get_cpuset_values_for_a_moldable_job')

def job_arm_leon_timer(job_id):
    """Set the state to TIMER_ARMED of job"""
    db.query(FragJob).filter(FragJob.job_id == job_id).update({FragJob.state: 'TIMER_ARMED'})
    db.commit()

def job_finishing_sequence(epilogue_script, job_id, events):
    
    raise NotImplementedError('TODO: job_finishing_sequence')

def get_job_frag_state(job_id):
    """Get the frag_state value for a specific job"""
    return db.query(FragJob.state).filter(FragJob.job_id == job_id).one()
    
def get_jobs_to_kill():
    """Return the list of jobs that have their frag state to LEON"""
    res = db.query(Job).filter(FragJob.state == 'LEON')\
                       .filter(Job.id == FragJob.job_id)\
                       .filter(~Job.state.in_(('Error', 'Terminated', 'Finishing'))).all()
    return res

def set_finish_date(job):
    """Set the stop time of the job passed in parameter to the current time
    (will be greater or equal to start time)"""
    date = tools.get_date()
    if date < job.start_time:
        date = job.start_time
    db.query(Job).filter(Job.id == job.id).update({Job.stop_time: date})
    db.commit()

def set_running_date(job_id):
    """Set the starting time of the job passed in parameter to the current time"""
    date = tools.get_date()
    #In OAR2 gantt  moldable_id=0 is used to indicate time gantt orign, not in OAR3
    # gantt_date = get_gantt_date()
    # if gantt_date < date:
    #     date = gantt_date
    db.query(Job).filter(Job.id == job_id).update({Job.start_time: date})
    db.commit()

    
def get_to_exterminate_jobs():
    """"Return the list of jobs that have their frag state to LEON_EXTERMINATE"""
    res = db.query(Job).filter(FragJob.state == 'LEON_EXTERMINATE')\
                       .filter(Job.id == FragJob.job_id).all()
    return res







