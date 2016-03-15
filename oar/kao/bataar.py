#!/usr/bin/env python
# coding: utf-8
# BatAar: BatSim Adaptor for OAR
from __future__ import unicode_literals, print_function

import struct
import socket
import sys
import json
import click
import time

if sys.version_info[0] == 2:
    from sets import Set

from oar.lib import (db, config, get_logger, Job, Resource, Queue)
from oar.lib.compat import iteritems

from oar.kao.job import (insert_job, set_job_state)

from oar.kao.simsim import ResourceSetSimu, JobSimu
from oar.lib.interval import itvs2batsim_str0
from oar.kao.kamelot import schedule_cycle
from oar.kao.platform import Platform

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

jobs = {}
jobs_completed = []
jobs_waiting = []

sched_delay = 5.0

nb_completed_jobs = 0
nb_jobs = 0
nb_res = 0


def create_uds(uds_name):

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    logger.info('connecting to', uds_name)
    try:
        sock.connect(uds_name)
        logger.info('connected')
    except socket.error:
        print("socket error: ", uds_name)
        logger.error("socket error", uds_name)
        sys.exit(1)
    return sock


def read_bat_msg(connection):
    lg_str = connection.recv(4)

    if not lg_str:
        print("connection is closed by batsim core")
        exit(1)

    # print 'from client (lg_str): %r' % lg_str
    lg = struct.unpack("i", lg_str)[0]
    # print 'size msg to recv %d' % lg
    if sys.version_info[0] == 2:
        msg = connection.recv(lg)
    else:
        chunks = []
        bytes_recd = 0
        while bytes_recd < lg:
            chunk = connection.recv(lg - bytes_recd)
            if chunk == b'':
                raise RuntimeError("socket connection broken")
            chunks.append(chunk)
            bytes_recd = bytes_recd + len(chunk)
        msg = (b''.join(chunks)).decode("utf-8")

    print('from batsim : %s' % msg)
    sub_msgs = msg.split('|')
    data = sub_msgs[0].split(":")
    # version = int(data[0])
    now = float(data[1])

    jobs_submitted = []
    new_jobs_completed = []
    for i in range(1, len(sub_msgs)):
        data = sub_msgs[i].split(':')
        if data[1] == 'S':
            jobs_submitted.append(int(data[2]))
        elif data[1] == 'C':
            time = float(data[0])
            jid = int(data[2])
            jobs[jid].state = "Terminated"
            jobs[jid].run_time = time - jobs[jid].run_time
            new_jobs_completed.append(jid)
        else:
            raise Exception("Unknow submessage type" + data[1])

    return (now, jobs_submitted, new_jobs_completed)


def send_bat_msg(connection, now, jids_to_launch, jobs):
    msg = "0:" + str(now) + "|"
    if jids_to_launch:
        msg += str(now) + ":J:"
        for jid in jids_to_launch:
            msg += str(jid) + "=" + itvs2batsim_str0(jobs[jid].res_set) + ";"
        msg = msg[:-1]  # remove last semicolon

    else:  # Do nothing
        msg += str(now) + ":N"

    print(msg)
    lg = struct.pack("i", int(len(msg)))
    connection.sendall(lg)
    connection.sendall(msg.encode("utf-8"))


def load_json_workload_profile(filename):
    wkp_file = open(filename)
    wkp = json.load(wkp_file)
    return wkp["jobs"], wkp["nb_res"]


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


class BatEnv(object):

    def __init__(self, now):
        self.now = now


class BatSched(object):

    def __init__(self, res_set, jobs, mode_platform="simu", db_jid2s_jid={}, sched_delay=-1,
                 uds_name='/tmp/bat_socket'):

        self.mode_platform = mode_platform
        self.sched_delay = sched_delay
        self.db_jid2s_jid = db_jid2s_jid
        self.env = BatEnv(0)
        self.platform = Platform(
            mode_platform, env=self.env, resource_set=res_set, jobs=jobs, db_jid2s_jid=db_jid2s_jid)
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

            now_str, jobs_submitted, new_jobs_completed = read_bat_msg(
                self.sock)

            # now_str = "10"
            # jobs_submitted = [1]
            # new_jobs_completed = []

            if jobs_submitted:
                for jid in jobs_submitted:
                    self.waiting_jids.add(jid)
                    if self.mode_platform == 'batsim-db':
                        print('set_job_state("Waiting"):', self.jobs[jid].db_jid)
                        set_job_state(self.jobs[jid].db_jid, 'Waiting')

            nb_completed_jobs += len(new_jobs_completed)

            print("new job completed: %s" % new_jobs_completed)

            for jid in new_jobs_completed:
                jobs_completed.append(jid)
                if jid in self.platform.running_jids:
                    self.platform.running_jids.remove(jid)
                if self.mode_platform == 'batsim-db':
                    set_job_state(self.jobs[jid].db_jid, 'Terminated')

            now = float(now_str)
            self.env.now = now  # TODO can be remove ???
            real_time = time.time()

            print("jobs running: %s" % self.platform.running_jids)
            print("jobs waiting: %s" % self.waiting_jids)
            print("jobs completed: %s" % jobs_completed)

            jids_to_launch = []

            if self.mode_platform == 'simu':
                print("call schedule_cycle.... %s" % now)
                schedule_cycle(self.platform, now, "default")

                # retrieve jobs to launch
                jids_to_launch = []
                for jid, job in iteritems(self.platform.assigned_jobs):
                    print(">>>>>>> job.start_time %s" % job.start_time)
                    if job.start_time == now:
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
                now += real_sched_time
            else:
                now += self.sched_delay
            self.env.now = now
            send_bat_msg(self.sock, now, jids_to_launch, self.jobs)

    def run(self):
        self.sched_loop()

##############


@click.command()
@click.argument('wkp_filename', required=True)
@click.option('-d', '--database-mode', default='no-db',
              help='select database mode (no-db, memory, oarconf)')
@click.option('-t', '--types', default='',
              help="types added to each jobs ex 'find=contiguous_1h,assign=one_time_find'")
@click.option('-s', '--socket', default='/tmp/bat_socket',
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
@click.option('-t', '--scheduler_delay', default=-1, type=click.INT,
              help="set the delay in seconds taken by scheduler to schedule all jobs. By default \
              the actual delay of scheduler is used")
def bataar(wkp_filename, database_mode, socket, node_size, scheduler_policy, types, scheduler_delay):
    #    import pdb; pdb.set_trace()
    if database_mode == 'memory':
        config.clear()
        config.update(BATSIM_DEFAULT_CONFIG)

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
        pass
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

    #
    # Load workload
    #

    json_jobs, nb_res = load_json_workload_profile(wkp_filename)

    print("nb_res:", nb_res)

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

    if database_mode == 'no-db':
        #
        # generate ResourceSet
        #

        hy_resource_id = [[(i, i)] for i in range(1,nb_res+1)]
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

        #
        # prepare jobs
        #
        mld_id = 1
        print("Genererate jobs")

        for j in json_jobs:
            jid = int(j['id'])
            rqb = [([('resource_id', j['res'])], [(1, nb_res)])]
            rqbh = [([('node', 1), ('resource_id', j['res'])], [(1, nb_res)])]

            if add_1h:
                if add_mld:
                    mld_res_rqts = [(mld_id, j["walltime"], rqbh), (mld_id+1, j["walltime"], rqb)]
                    mld_id += 2
                else:
                    mld_res_rqts = [(mld_id, j["walltime"], rqbh)]
                    mld_id += 1
            else:
                if add_mld:
                    mld_res_rqts = [(mld_id, j["walltime"], rqb), (mld_id+1, j["walltime"], rqb)]
                    mld_id += 2
                else:
                    mld_res_rqts = [(mld_id, j["walltime"], rqb)]
                    mld_id += 1

            jobs[jid] = JobSimu(id=jid,
                                state="Waiting",
                                queue="test",
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
                                assign=assign, assign_func=assign_func,
                                find=find, find_func=find_func)

            # print("jobs: ", jid, " mld_res_rqts: ", mld_res_rqts)
        # import pdb; pdb.set_trace()
        BatSched(res_set, jobs, 'simu', {}, scheduler_delay, socket).run()

    elif database_mode == 'memory':

        global offset_idx
        offset_idx = 1
        monkeypatch_oar_lib_tools()
        db_initialization(nb_res)

        #
        # prepare jobs
        #
        db_jid2s_jid = {}
        print("Prepare jobs")
        for i, j in enumerate(json_jobs):
            jid = int(j["id"])
            jobs[jid] = JobSimu(id=jid,
                                state="Waiting",
                                queue="test",
                                start_time=0,
                                walltime=0,
                                moldable_id=0,
                                mld_res_rqts=[(jid, j["walltime"],
                                               [([("resource_id", j["res"])],
                                                 [(1, nb_res - 0)])])],
                                run_time=0,
                                db_jid=i + 1,
                                assign=False,
                                find=False)

            insert_job(res=[(j["walltime"], [('resource_id=' + str(j["res"]), "")])],
                       state='Hold', properties='', user='')
            db_jid2s_jid[i + 1] = jid

        db.flush()  # TO REMOVE ???
        # import pdb; pdb.set_trace()
        BatSched([], jobs, 'batsim-db', db_jid2s_jid, scheduler_delay, socket).run()

        if __name__ != '__main__':
            # If used oar.lib.tools' functions are used after we need to undo monkeypatching.
            # Main use case is suite testing evaluation
            restore_oar_lib_tools()

if __name__ == '__main__':  # pragma: no cover
    bataar()
