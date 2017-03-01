#!/usr/bin/env python
# coding: utf-8
# BatAar: BatSim Adaptor for OAR
from __future__ import unicode_literals, print_function

import pdb
import sys
import re
import time
import math
import click

if sys.version_info[0] == 2:
    from sets import Set

from oar.lib import (db, config, get_logger, Job, Resource, Queue)
from oar.lib.compat import iteritems

from oar.kao.job import (insert_job, set_job_state)

from oar.kao.simsim import ResourceSetSimu, JobSimu
from oar.lib.interval import itvs2batsim_str0 # TOREMOVE ?
from oar.kao.kamelot import schedule_cycle
from oar.kao.platform import Platform

from oar.kao.batsim import  BatsimScheduler, Batsim

import oar.kao.advanced_scheduling

from oar.kao.meta_sched import meta_schedule
import oar.lib.tools

plt = None
orig_func = {}

BATSIM_DEFAULT_CONFIG = {
    'DB_BASE_FILE': ':memory:',
    'DB_TYPE': 'sqlite',
    'LOG_CATEGORIES': 'all',
    'LOG_FILE': '',
    'LOG_FORMAT': '[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s',
    'LOG_LEVEL': 3,
    'HIERARCHY_LABELS': 'resource_id,network_address',
    'SCHEDULER_RESOURCE_ORDER': 'resource_id ASC',
    'SCHEDULER_JOB_SECURITY_TIME': '60',
    'SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE': 'default',
    'FAIRSHARING_ENABLED': 'no',
    'SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER': '3',
    'QUOTAS': 'no',
    'SCHEDULER_RESOURCE_ORDER': 'resource_id ASC',
    'SCHEDULER_TIMEOUT': 30,
    'SERVER_HOSTNAME': 'server',
    'SERVER_PORT': 6666,
    'ENERGY_SAVING_INTERNAL': 'no',
    'SQLALCHEMY_CONVERT_UNICODE': True,
    'SQLALCHEMY_ECHO': False,
    'SQLALCHEMY_MAX_OVERFLOW': None,
    'SQLALCHEMY_POOL_RECYCLE': None,
    'SQLALCHEMY_POOL_SIZE': None,
    'SQLALCHEMY_POOL_TIMEOUT': None,
    'TAKTUK_CMD': '/usr/bin/taktuk -t 30 -s',
}

# config.clear()
# config.update(BATSIM_DEFAULT_CONFIG)

config['LOG_FILE'] = '/tmp/batsim.log'
logger = get_logger("oar.batsim")

def monkeypatch_oar_lib_tools():
    global orig_func

    orig_func['init_judas_notify_user'] = oar.lib.tools.init_judas_notify_user
    orig_func['create_almighty_socket'] = oar.lib.tools.create_almighty_socket
    orig_func['notify_almighty'] = oar.lib.tools.notify_almighty
    orig_func['notify_tcp_socket'] = oar.lib.tools.notify_tcp_socket
    orig_func['notify_user'] = oar.lib.tools.notify_user

    oar.lib.tools.init_judas_notify_user = lambda: None
    oar.lib.tools.create_almighty_socket = lambda: None
    oar.lib.tools.notify_almighty = lambda x: len(x)
    oar.lib.tools.notify_tcp_socket = lambda addr, port, msg: len(msg)
    oar.lib.tools.notify_user = lambda job, state, msg: len(state + msg)


def restore_oar_lib_tools():
    oar.lib.tools.init_judas_notify_user = orig_func['init_judas_notify_user']
    oar.lib.tools.create_almighty_socket = orig_func['create_almighty_socket']
    oar.lib.tools.notify_almighty = orig_func['notify_almighty']
    oar.lib.tools.notify_tcp_socket = orig_func['notify_tcp_socket']
    oar.lib.tools.notify_user = orig_func['notify_user']


def db_initialization(nb_res, node_size=None):

    print("Set default queue")
    db.add(Queue(name='default', priority=3, scheduler_policy='kamelot', state='Active'))

    print("add resources")
    # add some resources
    for i in range(nb_res):
        db.add(Resource(network_address="localhost"))
    db.commit()

class BatEnvTime(object):

    def __init__(self, now):
        self.now = now

class SchedPolicyParams(object):
    def __init__(self, scheduler_policy, types):
        assign = False
        assign_func = None
        find = False
        find_func = None

        add_1h = False  # control addition of one level of hierarchy in resources' request
        add_mld = False  # control addition of one modldable instance in resources' request

        sp = scheduler_policy
        if sp == 'BASIC' or sp == '0':
            print("BASIC scheduler_policy selected")
            # default

        elif sp == 'BEST_EFFORT_CONTIGUOUS' or sp == '1':
            print("BEST_EFFORT_CONTIGUOUS scheduler_policy selected")
            find = True
            find_func = getattr(oar.kao.advanced_scheduling, 'find_contiguous_1h')
            assign = True
            assign_func = getattr(oar.kao.advanced_scheduling, 'assign_one_time_find')
        elif sp == 'CONTIGUOUS' or sp == '2':
            print("CONTIGUOUS scheduler_policy selected")
            find = True
            find_func = getattr(oar.kao.advanced_scheduling, 'find_contiguous_1h')
        elif sp == 'BEST_EFFORT_LOCAL' or sp == '3':
            print("BEST_EFFORT_LOCAL scheduler_policy selected")
            add_1h = True
            add_mld = True
        elif sp == 'LOCAL' or sp == '4':
            print("LOCAL scheduler_policy selected")
            add_1h = True

        if types and types != '':
            types_array = types.split(',')
            for type_value in types_array:
                t, v = type_value.split('=')
                if t == "assign":
                    print("type assign with function: ", v)
                    assign = True
                    assign_func = getattr(oar.kao.advanced_scheduling, 'assign_' + v)
                if t == "find":
                    print("type find with function: ", v)
                    find = True
                    find_func = getattr(oar.kao.advanced_scheduling, 'find_' + v)
        self.assign = assign
        self.assign_func = assign_func
        self.find = find
        self.find_func = find_func

        self.add_1h = add_1h
        self.add_mld = add_mld


class BatSched(BatsimScheduler):
    def __init__(self, scheduler_policy, types, sched_delay, node_size,
                 database_mode='no-db', platform_model="simu"):
        self.scheduler_policy = scheduler_policy
        self.platform_model = platform_model
        self.database_mode = database_mode
        self.node_size = node_size
        self.sched_delay = sched_delay
        self.jobs = {}

        self.db_jid2s_jid = {}     #TODO verify
        self.index_simu2db_jid = 0 #TODO verify
        self.offset_simu2db_jid = 1 #TODO verify/parametrize

        self.ujid_l = [] #TODO ???

        self.env = None
        self.platform = None

        if sys.version_info[0] == 2:
            self.waiting_jids = Set()
        else:
            self.waiting_jids = set()
        self.jobs_completed = []
        self.nb_res = 0

        self.sp_params = SchedPolicyParams(scheduler_policy, types)

    def onAfterBatsimInit(self):

        self.nb_res = self.bs.nb_res
        nb_res = self.nb_res
        node_size = self.node_size
        # import pdb; pdb.set_trace()
        if self.database_mode == 'no-db':
            hy_resource_id = [[(i, i)] for i in range(1, nb_res+1)]
            hierarchy = {'resource_id': hy_resource_id}
            if node_size > 0:
                node_id = [[(node_size*i, node_size*(i+1)-1)] for i in range(int(nb_res/node_size))]
                hierarchy['node'] = node_id

            print('hierarchy: ', hierarchy)

            res_set = ResourceSetSimu(
                rid_i2o=range(nb_res+1),
                rid_o2i=range(nb_res+1),
                roid_itvs=[(1, nb_res)],
                hierarchy=hierarchy,
                available_upto={2147483600: [(1, nb_res)]}
            )
        elif self.database_mode == 'memory':
            monkeypatch_oar_lib_tools()
            db_initialization(nb_res)
            self.platform_model = 'batsim-db'
            res_set = None

        else:
            raise NotImplementedError('Database mode: ' + self.database_mode)

        #import pdb; pdb.set_trace()

        self.env = BatEnvTime(0) #???
        self.platform = Platform(self.platform_model, env=self.env, resource_set=res_set,
                                 jobs=self.jobs, db_jid2s_jid=self.db_jid2s_jid)
        #global plt
        #plt = self.platform

        self.platform.running_jids = []
        self.platform.waiting_jids = self.waiting_jids
        self.platform.completed_jids = []

        #self.jobs_res = {}
        #self.jobs_completed = []
        #self.jobs_waiting = []

        #self.sched_delay = 0.5

        # self.availableResources = SortedSet(range(self.bs.nb_res))
        # self.previousAllocations = dict()

    def generateJob(self, data_storage_job):
        j = data_storage_job
        new_ujid = len(self.ujid_l)
        self.ujid_l.append(j.id)
        jid = new_ujid
        walltime = int(math.ceil(float(j.requested_time)))
        res = j.requested_resources
        rqb = [([('resource_id', res)], [(1, self.nb_res)])]
        rqbh = [([('node', 1), ('resource_id', res)], [(1, self.nb_res)])]

        if self.sp_params.add_1h:
            if self.sp_params.add_mld:
                mld_res_rqts = [(2*jid, walltime, rqbh), (2*jid+1, walltime, rqb)]
            else:
                mld_res_rqts = [(jid, walltime, rqbh)]
        else:
            if self.sp_params.add_mld:
                mld_res_rqts = [(2*jid, walltime, rqb), (2*jid+1, walltime, rqb)]
            else:
                mld_res_rqts = [(jid, walltime, rqb)]

        job = JobSimu(id=jid,
                      ds_job=j,
                      state='Waiting',
                      queue='default',
                      start_time=0,
                      walltime=0,
                      types={},
                      res_set=[],
                      moldable_id=0,
                      mld_res_rqts=mld_res_rqts,
                      run_time=0,
                      deps=[],
                      key_cache={},
                      ts=False, ph=0,
                      assign=self.sp_params.assign, assign_func=self.sp_params.assign_func,
                      find=self.sp_params.find, find_func=self.sp_params.find_func,
                      db_jid=jid+1 # TODO First job is 0 in DB ?
        )

        if self.platform_model == 'batsim-db':
            insert_job(queue_name='default', res=[(walltime, [('resource_id=' + str(res), "")])],
                       state='Waiting', properties='', user='')
            self.db_jid2s_jid[self.index_simu2db_jid + self.offset_simu2db_jid] = jid
            self.index_simu2db_jid += 1
        return job


    def scheduleJobs(self):
        print("Scheduling Round")
        jids_to_launch = [] 
        real_time = time.time()
        if self.platform_model == 'simu':
            schedule_cycle(self.platform, self.env.now, "default")

            # retrieve jobs to launch
            for jid, job in iteritems(self.platform.assigned_jobs):
                print("job.start_time %s" % job.start_time)
                if (job.start_time == self.env.now) and (job.state == 'Waiting'):
                    self.waiting_jids.remove(jid)
                    jids_to_launch.append(jid)
                    job.state = "Running"
                    print("tolaunch: %s" % jid)
                    self.platform.running_jids.append(jid)

        else:
            print("call meta_schedule('internal')")
            #pdb.set_trace()
            meta_schedule('internal', self.platform)
            #pdb.set_trace()

            result = db.query(Job).filter(Job.state == 'toLaunch')\
                                  .order_by(Job.id).all()

            for job_db in result:
                set_job_state(job_db.id, 'Running')
                jid = self.db_jid2s_jid[job_db.id]
                self.waiting_jids.remove(jid)
                jids_to_launch.append(jid)
                self.jobs[jid].state = "Running"
                print("_tolaunch: %s" % jid)
                self.platform.running_jids.append(jid)


        print("Ids of jobs to launch: ", *jids_to_launch)
        print("Time befort scheduling round: ", self.bs._current_time, self.sched_delay)
        # update time
        real_sched_time = time.time() - real_time
        if self.sched_delay == -1:
            self.bs.consume_time(real_sched_time) #TODO
        else:
            self.bs.consume_time(self.sched_delay)

        self.env.now = self.bs._current_time

        print("Time after scheduling round: ", self.bs._current_time)
        # send to uds
        if len(jids_to_launch) > 0:
            scheduled_jobs = []
            jobs_res = {}
            for jid in jids_to_launch:
                ds_job = self.jobs[jid].ds_job
                res = itvs2batsim_str0(self.jobs[jid].res_set) 
                scheduled_jobs.append(ds_job)
                jobs_res[ds_job.id] = res

            self.bs.start_jobs(scheduled_jobs, jobs_res)

        #real_sched_time = time.time() - real_time
        #if self.sched_delay == -1:
        #    now_float += real_sched_time
        #else:
        #now_float += self.sched_delay
        #send_bat_msg(self.sock, now_float, jids_to_launch, self.jobs)


    def onJobSubmission(self, data_storage_job):
        data_storage_jobs = [data_storage_job] # TODO vectorize in batsim.py !!!
        print(data_storage_jobs)
        for ds_job in data_storage_jobs:
            job = self.generateJob(ds_job)
            self.jobs[job.id] = job
            self.waiting_jids.add(job.id)
        self.scheduleJobs()

    def onJobCompletion(self, data_storage_job):
        data_storage_jobs = [data_storage_job] # TODO vectorize in batsim.py !!!

        for job in data_storage_jobs:
            jid = self.ujid_l.index(job.id)
            self.jobs_completed.append(jid)
            if jid in self.platform.running_jids:
                self.platform.running_jids.remove(jid)
            if self.platform_model == 'batsim-db':
                set_job_state(self.jobs[jid].db_jid, 'Terminated')

        self.scheduleJobs()

class BatSched_old(object):

    def __init__(self, res_set, jobs, platform_model="simu", db_jid2s_jid={}, sched_delay=-1,
                 uds_name='/tmp/bat_socket'):

        self.platform_model = platform_model
        self.sched_delay = sched_delay
        self.db_jid2s_jid = db_jid2s_jid
        self.env = BatEnv(0)
        self.platform = Platform(
            platform_model, env=self.env, resource_set=res_set, jobs=jobs, db_jid2s_jid=db_jid2s_jid)
        global plt
        plt = self.platform
        self.jobs = jobs
        self.nb_jobs = len(jobs)
        self.sock = create_uds(uds_name)

        self.platform.running_jids = []
        if sys.version_info[0] == 2:
            self.waiting_jids = Set()
        else:
            self.waiting_jids = set()
        self.platform.waiting_jids = self.waiting_jids
        self.platform.completed_jids = []

    def sched_loop(self):
        nb_completed_jobs = 0
        while nb_completed_jobs < self.nb_jobs:

            now_float, jobs_submitted, new_jobs_completed = read_bat_msg(
                self.sock)

            # now_str = "10"
            # jobs_submitted = [1]
            # new_jobs_completed = []

            if jobs_submitted:
                for jid in jobs_submitted:
                    self.waiting_jids.add(jid)
                    if self.platform_model == 'batsim-db':
                        print('set_job_state("Waiting"):', self.jobs[jid].db_jid)
                        set_job_state(self.jobs[jid].db_jid, 'Waiting')

            nb_completed_jobs += len(new_jobs_completed)

            print("new job completed: %s" % new_jobs_completed)

            for jid in new_jobs_completed:
                jobs_completed.append(jid)
                if jid in self.platform.running_jids:
                    self.platform.running_jids.remove(jid)
                if self.platform_model == 'batsim-db':
                    set_job_state(self.jobs[jid].db_jid, 'Terminated')

            now = int(now_float)
            self.env.now = now  # TODO can be remove ???
            real_time = time.time()

            print("jobs running: %s" % self.platform.running_jids)
            print("jobs waiting: %s" % self.waiting_jids)
            print("jobs completed: %s" % jobs_completed)

            jids_to_launch = []

            if self.platform_model == 'simu':
                print("call schedule_cycle.... %s" % now)
                schedule_cycle(self.platform, now, "default")

                # retrieve jobs to launch
                jids_to_launch = []
                for jid, job in iteritems(self.platform.assigned_jobs):
                    print("job.start_time %s" % job.start_time)
                    if (job.start_time == now) and (job.state=='Waiting'):
                        self.waiting_jids.remove(jid)
                        jids_to_launch.append(jid)
                        job.state = "Running"
                        print("tolaunch: %s" % jid)
                        self.platform.running_jids.append(jid)

            else:
                print("call meta_schedule('internal')")
                meta_schedule('internal', plt)
                # Launching phase
                # Retrieve job to Launch

                result = db.query(Job).filter(Job.state == 'toLaunch')\
                                      .order_by(Job.id).all()

                for job_db in result:
                    set_job_state(job_db.id, 'Running')
                    jid = self.db_jid2s_jid[job_db.id]
                    self.waiting_jids.remove(jid)
                    jids_to_launch.append(jid)
                    self.jobs[jid].state = "Running"
                    print("_tolaunch: %s" % jid)
                    self.platform.running_jids.append(jid)

            real_sched_time = time.time() - real_time
            if self.sched_delay == -1:
                now_float += real_sched_time
            else:
                now_float += self.sched_delay
            send_bat_msg(self.sock, now_float, jids_to_launch, self.jobs)

    def run(self):
        self.sched_loop()

##############

@click.command()
@click.option('-d', '--database-mode', default='no-db',
              help='select database mode (no-db, memory, oarconf)')
@click.option('-t', '--types', default='',
              help="types added to each jobs ex 'find=contiguous_1h,assign=one_time_find'")
@click.option('-s', '--socket-endpoint', default='tcp://*:28000',
              help="name of socket to comminication with BatSim simulator")
@click.option('-n', '--node_size', default=0,
              help="size of node used for 2 levels hierarachy")
@click.option('-p', '--scheduler_policy', type=click.STRING,
              help="select a particular scheduler policy:\
              BASIC | 0 | : \
              * Equivalent to Conservative Backfilling for OAR (default)\
              BEST_EFFORT_CONTIGUOUS | 1 |\
              * Keep assignment with lower end time\
              between contiguous and default polices. Contiguous policy is considered first, walltime is \
              the same for each policy, contiguous assignment is retained if end times are equal\
              CONTIGUOUS | 2 |\
              * Allocated resources will have consecutive resouce_id\
              BEST_EFFORT_LOCAL | 3 |\
              * Keep assignment with lower end time\
              between local and default polices. Local policy is considered first, walltime is \
              the same for each policy, contiguous assignment is retained if end times are equal\
              LOCAL | 4 |\
              * Allocated resources which belongs to the same node. Node's size must be provided,\
              job's sizes are not allowed to exceed node's size.")
@click.option('-t', '--scheduler_delay', default=0.05, type=click.INT, #TODO default=-1
              help="set the delay in seconds taken by scheduler to schedule all jobs. By default \
              the actual delay of scheduler is used")
@click.option('-r', '--redis_hostname', default='localhost', type=click.STRING, help="Set redis server hostname.")
@click.option('-P', '--redis_port', default=6379, type=click.INT, help="Set redis server port.")
@click.option('-k', '--redis_key_prefix', default=None, type=click.STRING, help="Set redis key prefix.")
@click.option('-v', '--verbose', is_flag=True, help="Be more verbose.")
#@click.option('--protect', is_flag=True, help="Activate jobs' test (like overlaping) .")

def bataar(database_mode, socket_endpoint, node_size, scheduler_policy, types, scheduler_delay, redis_hostname,
           redis_port, redis_key_prefix, verbose):
    """Adaptor to Batsim Simulator."""
    #    import pdb; pdb.set_trace()
    if database_mode == 'memory':
        config.clear()
        config.update(BATSIM_DEFAULT_CONFIG)

    print("Starting simulation...")
    print("Scheduler Policy:", scheduler_policy)
    print("Scheduler delay:", scheduler_delay)

    verbose_level = 0
    if verbose:
        verbose_level = 2
    #import pdb; pdb.set_trace()
    #if database_mode == 'no-db':
    scheduler = BatSched(scheduler_policy, types, scheduler_delay, node_size, database_mode)
    batsim = Batsim(scheduler, redis_key_prefix, redis_hostname, redis_port,
                    None, socket_endpoint, verbose_level)
    batsim.start()

    #elif database_mode == 'memory':

    #   global offset_idx
    # offset_idx = 1
    #    monkeypatch_oar_lib_tools()
    #    db_initialization(nb_res)

    #
    # prepare jobs
    #
    #     db_jid2s_jid = {}
    #    print("Prepare jobs")
    #    for i, j in enumerate(json_jobs):
    #        jid = int(j["id"])
    #        walltime = int(math.ceil(float(j["walltime"])))
    #        jobs[jid] = JobSimu(id=jid,
    #                            state="Waiting",
    #                            queue="test",
    #                            start_time=0,
    #                            walltime=0,
    #                            moldable_id=0,
    #                            mld_res_rqts=[(jid, walltime,
    #                                           [([("resource_id", j["res"])],
    #                                             [(1, nb_res - 0)])])],
    #                            run_time=0,
    #                            db_jid=i + 1,
    #                            assign=False,
    #                            find=False)

    #        insert_job(res=[(walltime, [('resource_id=' + str(j["res"]), "")])],
    #                   state='Hold', properties='', user='')
    #        db_jid2s_jid[i + 1] = jid
    #
    #    db.flush()  # TO REMOVE ???
    #    # import pdb; pdb.set_trace()
    #    BatSched([], jobs, 'batsim-db', db_jid2s_jid, scheduler_delay, socket_endpoint).run()

    if __name__ != '__main__' and database_mode == 'memory':
        # If used oar.lib.tools' functions are used after we need to undo monkeypatching.
        # Main use case is suite testing evaluation
        restore_oar_lib_tools()

